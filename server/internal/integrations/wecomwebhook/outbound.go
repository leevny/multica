package wecomwebhook

// outbound.go — the announce path. Subscribes to the task lifecycle
// (task:queued / task:dispatch / task:running / task:waiting_local_directory /
// task:completed) and the terminal outcomes (chat:done, task:failed,
// task:cancelled), posting one rendered message per announcement into the
// WeCom group whose webhook the session is bound to, resolving the
// destination through channel_chat_session_binding the same way the
// wecom/telegram outbounds do:
//
//	event → chat_session → binding (channel_type='wecomwebhook') → installation → key
//
// Noise control, in three layers:
//
//   - Status lines coalesce: consecutive status events for ONE task within
//     statusWindow are last-state-wins — the held line is replaced and the
//     window restarts, so a queued→dispatch→running burst produces a single
//     "「标题」开始执行" line when the window closes. Explicitly NOT
//     subscribed: task:coalesced / task:progress / task:usage / task:message.
//   - A terminal message consumes the held status line of its task as a
//     prefix, so a run that ends within the window lands as ONE message
//     ("「标题」开始执行" + the reply) instead of two.
//   - task:completed never announces after a terminal message for the same
//     task already went out: chat:done publishes BEFORE task:completed in the
//     completion flow (task.go), so the guard is what keeps every chat turn
//     at reply-only. A turn that produced no reply (no_response) sends
//     nothing and does NOT arm the guard, so the held "已完成" line still
//     flushes and the group learns the run ended.
//
// Every skip is logged at INFO with the session/task id and reason — silent
// skips cost half a day of debugging once. Announcements ignore task origin:
// a binding exists only where a user explicitly asked for group pushes, so a
// run started in the web UI still announces (unlike the reply-path outbounds,
// which gate on channel-ingested origin — here the binding IS the consent).
//
// Everything the reply paths do that a webhook cannot — streaming, progress
// bubbles, media, threading, per-user language — is absent by design; the
// group robot API has none of it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// busHandlerTimeout bounds one event's handling. The bus is synchronous — a
// handler that stalls wedges the publish call site — so this is tight, and
// the chunk+retry budget inside the sender works within it.
const busHandlerTimeout = 15 * time.Second

// DefaultStatusWindow is the coalesce window for same-task status lines:
// consecutive lifecycle events of one task within the window collapse into a
// single last-state-wins message, and a terminal event arriving inside the
// window consumes the held line as its prefix. Tunable at deployment time via
// MULTICA_WECOM_WEBHOOK_STATUS_WINDOW (a Go duration, e.g. "8s").
const DefaultStatusWindow = 5 * time.Second

// terminalGuardTTL bounds the completed-after-terminal guard. A task id never
// repeats, so the only job is keeping the map small; 15 minutes dwarfs any
// completion-to-broadcast gap.
const terminalGuardTTL = 15 * time.Minute

// statusVerbs maps the subscribed lifecycle events to the status verb of the
// one-line announcement. dispatch and running share a verb on purpose: the
// two events land back to back and last-state-wins collapses them anyway.
// zh-Hans copy, same rationale as the terminal lines below.
var statusVerbs = map[string]string{
	protocol.EventTaskQueued:                "已入队",
	protocol.EventTaskDispatch:              "开始执行",
	protocol.EventTaskRunning:               "开始执行",
	protocol.EventTaskWaitingLocalDirectory: "等待本地目录",
	protocol.EventTaskCompleted:             "已完成",
}

// Stable skip reasons for the INFO skip log. Greppable, and the reason a
// session produced no announcement is visible in one line.
const (
	skipNoChatSession        = "no_chat_session"
	skipNoBinding            = "no_binding"
	skipInstallationMissing  = "installation_missing"
	skipInstallationInactive = "installation_not_active"
	skipKeyUnavailable       = "key_unavailable"
	// Status-push (notify-system) skips. The structural default - a
	// workspace with no robot - logs at DEBUG; an installation that exists
	// but cannot deliver logs at INFO, the "why isn't my robot announcing"
	// case.
	skipNoActiveInstallation = "no_active_installation"
	skipAnnounceDisabled     = "announce_disabled"
)

// outboundQueries is the slice of generated queries the announce path needs.
type outboundQueries interface {
	GetChannelChatSessionBindingBySession(ctx context.Context, arg db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error)
	GetChannelInstallation(ctx context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error)
	GetChatSession(ctx context.Context, id pgtype.UUID) (db.ChatSession, error)
	ListChannelInstallationsByWorkspace(ctx context.Context, arg db.ListChannelInstallationsByWorkspaceParams) ([]db.ChannelInstallation, error)
}

// messageSender is the send surface the announcer needs. *Sender satisfies
// it; tests substitute a recorder.
type messageSender interface {
	SendMarkdown(ctx context.Context, key, content string) ([]string, error)
}

// destination is the resolved delivery address of one announcement: the
// installation's plaintext key and its display suffix. Held in memory only —
// never logged, never persisted.
type destination struct {
	key       string
	keySuffix string
}

// pendingStatus is one held status line awaiting either its window close or a
// consuming terminal event. gen invalidates stale timers: every replacement
// bumps it, so an AfterFunc from a superseded event finds a mismatch and
// exits.
type pendingStatus struct {
	gen      uint64
	line     string
	dests    []destination
	issueKey string
}

// IssueStatusAnnouncer is the notify-system hook the notification listeners
// call: ONE invocation per issue:updated status change, never per recipient.
// *Outbound implements it; the interface keeps cmd/server's listener wiring
// testable without the integration.
type IssueStatusAnnouncer interface {
	AnnounceIssueStatusChange(ctx context.Context, workspaceID, issueID, issueTitle, from, to string)
}

// Outbound announces task lifecycle and terminal outcomes to bound WeCom
// groups.
type Outbound struct {
	q            outboundQueries
	open         func([]byte) ([]byte, error)
	sender       messageSender
	logger       *slog.Logger
	statusWindow time.Duration

	// mu guards the coalescer state below. The bus handlers run on publish
	// call sites' goroutines; the flush timers run on AfterFunc goroutines.
	mu         sync.Mutex
	pending    map[string]*pendingStatus // task id → held status line
	terminalAt map[string]time.Time      // task id → a terminal announcement went out
}

// NewOutbound builds the subscriber. open is box.Open for the deployment's
// MULTICA_WECOM_WEBHOOK_SECRET_KEY — the same box the wiring registered the
// Factory with.
func NewOutbound(q outboundQueries, open func([]byte) ([]byte, error), sender messageSender, logger *slog.Logger) (*Outbound, error) {
	if q == nil {
		return nil, errors.New("wecomwebhook: Outbound requires queries")
	}
	if open == nil {
		return nil, errors.New("wecomwebhook: Outbound requires a decrypt function")
	}
	if sender == nil {
		return nil, errors.New("wecomwebhook: Outbound requires a sender")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Outbound{
		q:            q,
		open:         open,
		sender:       sender,
		logger:       logger,
		statusWindow: DefaultStatusWindow,
	}, nil
}

// SetStatusWindow overrides the status coalesce window. Non-positive values
// fall back to DefaultStatusWindow.
func (o *Outbound) SetStatusWindow(d time.Duration) {
	if d <= 0 {
		d = DefaultStatusWindow
	}
	o.statusWindow = d
}

// Register subscribes the task lifecycle and terminal events. Everything
// else — progress, usage, inbox, typing — is out of scope for a webhook; see
// the file comment.
func (o *Outbound) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventChatDone, o.handleEvent)
	bus.Subscribe(protocol.EventTaskFailed, o.handleEvent)
	bus.Subscribe(protocol.EventTaskCancelled, o.handleEvent)
	bus.Subscribe(protocol.EventTaskQueued, o.handleEvent)
	bus.Subscribe(protocol.EventTaskDispatch, o.handleEvent)
	bus.Subscribe(protocol.EventTaskRunning, o.handleEvent)
	bus.Subscribe(protocol.EventTaskWaitingLocalDirectory, o.handleEvent)
	bus.Subscribe(protocol.EventTaskCompleted, o.handleEvent)
}

// terminalPayload is the union of the fields the events can carry. The
// payloads arrive as either a struct (chat:done) or a map (task lifecycle
// events); re-marshalling through JSON is the one decoder that handles both
// without per-event branches.
type terminalPayload struct {
	TaskID        string `json:"task_id"`
	IssueID       string `json:"issue_id"`
	ChatSessionID string `json:"chat_session_id"`
	Content       string `json:"content"`
	MessageKind   string `json:"message_kind"`
	FailureReason string `json:"failure_reason"`
	RetryPending  bool   `json:"retry_pending"`
}

func (o *Outbound) handleEvent(e events.Event) {
	ctx, cancel := context.WithTimeout(context.Background(), busHandlerTimeout)
	defer cancel()
	p, ok := decodePayload(e)
	if !ok {
		return
	}
	var err error
	if _, isStatus := statusVerbs[e.Type]; isStatus {
		err = o.processStatusEvent(ctx, e, p)
	} else {
		err = o.processTerminalEvent(ctx, e, p)
	}
	if err != nil {
		// An announcement failure is never surfaced to a caller — the
		// publisher has moved on. Log with enough to find the session.
		o.logger.ErrorContext(ctx, "wecomwebhook: announce failed",
			"event", e.Type, "chat_session_id", e.ChatSessionID, "task_id", e.TaskID, "error", err)
	}
}

func (o *Outbound) processStatusEvent(ctx context.Context, e events.Event, p terminalPayload) error {
	verb := statusVerbs[e.Type]
	taskID := taskIDOf(e, p)
	// A terminal announcement for this task already reached (or was attempted
	// for) the group — chiefly chat:done publishing before task:completed in
	// the same completion flow. The reply IS the completion announcement.
	if o.alreadyTerminal(taskID) {
		o.logger.DebugContext(ctx, "wecomwebhook: status skipped (terminal announced)",
			"event", e.Type, "task_id", taskID)
		return nil
	}
	sessionID := sessionIDOf(e, p)
	if sessionID == "" {
		// Issue / autopilot runs have no chat session and no announce route.
		// DEBUG: busier than the binding skips and carries no session id to
		// grep, but visible all the same.
		o.logger.DebugContext(ctx, "wecomwebhook: announcement skipped",
			"reason", skipNoChatSession, "event", e.Type, "task_id", taskID)
		return nil
	}
	dests, skip, err := o.resolveDestination(ctx, sessionID)
	if err != nil {
		return err
	}
	if skip != "" {
		o.logSkip(ctx, e.Type, sessionID, taskID, skip)
		return nil
	}
	line := fmt.Sprintf("**「%s」%s**", o.sessionTitle(ctx, sessionID), verb)
	// A completed line for a linked issue participates in the issue's merge
	// window, so the notify-system status_changed for the same work item
	// cannot double-announce the completion (see holdStatus).
	linked := ""
	if e.Type == protocol.EventTaskCompleted && p.IssueID != "" {
		linked = "issue:" + p.IssueID
	}
	o.holdStatus(taskID, line, dests, linked)
	return nil
}

func (o *Outbound) processTerminalEvent(ctx context.Context, e events.Event, p terminalPayload) error {
	sessionID := sessionIDOf(e, p)
	taskID := taskIDOf(e, p)
	if sessionID == "" {
		o.logger.DebugContext(ctx, "wecomwebhook: announcement skipped",
			"reason", skipNoChatSession, "event", e.Type, "task_id", taskID)
		return nil
	}
	dests, skip, err := o.resolveDestination(ctx, sessionID)
	if err != nil {
		return err
	}
	if skip != "" {
		o.logSkip(ctx, e.Type, sessionID, taskID, skip)
		return nil
	}
	message := renderAnnouncement(e.Type, p)
	if message == "" {
		// A no_response turn or a retry-pending failure says nothing. Leave
		// any held status line alone: it either merges into a later terminal
		// event or flushes on its own window.
		return nil
	}
	// The held status line becomes the reply's prefix, so a run that ended
	// inside the window lands as one message instead of two. A line flushed
	// just before this event leaves nothing to merge — two messages, the
	// ordinary window-expiry case.
	if prefix := o.popStatus(taskID); prefix != "" {
		message = prefix + "\n" + message
	}
	if err := o.sendToAll(ctx, dests, message); err != nil {
		return err
	}
	o.markTerminal(taskID)
	o.logger.DebugContext(ctx, "wecomwebhook: announced",
		"event", e.Type, "chat_session_id", sessionID)
	return nil
}

// resolveDestination resolves session → binding → installation → key.
// A non-empty skip reason means the announcement cannot go out and the caller
// should log and move on.
func (o *Outbound) resolveDestination(ctx context.Context, sessionID string) ([]destination, string, error) {
	sessionUUID, err := parseSessionUUID(sessionID)
	if err != nil {
		return nil, skipNoBinding, nil
	}
	binding, err := o.q.GetChannelChatSessionBindingBySession(ctx, db.GetChannelChatSessionBindingBySessionParams{
		ChatSessionID: sessionUUID,
		ChannelType:   channelTypeWecomWebhook,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, skipNoBinding, nil
		}
		return nil, "", fmt.Errorf("resolve binding: %w", err)
	}
	inst, err := o.q.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          binding.InstallationID,
		ChannelType: channelTypeWecomWebhook,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, skipInstallationMissing, nil
		}
		return nil, "", fmt.Errorf("load installation: %w", err)
	}
	if inst.Status != string(InstallationActive) {
		return nil, skipInstallationInactive, nil
	}
	installation, err := installationFromRow(inst, o.open)
	if err != nil {
		return nil, "", err
	}
	if installation.Key == "" {
		return nil, skipKeyUnavailable, nil
	}
	return []destination{{key: installation.Key, keySuffix: installation.KeySuffix}}, "", nil
}

// sendToAll delivers one message to every destination. Keys fail
// independently - one group's refusal must not cost the others - and the
// first error surfaces after every destination got its chance.
func (o *Outbound) sendToAll(ctx context.Context, dests []destination, message string) error {
	var firstErr error
	for _, dest := range dests {
		if _, err := o.sender.SendMarkdown(ctx, dest.key, message); err != nil {
			wrapped := fmt.Errorf("announce to group (key suffix %s): %w", dest.keySuffix, err)
			if firstErr == nil {
				firstErr = wrapped
			}
		}
	}
	return firstErr
}

// AnnounceIssueStatusChange is the notify-system hook (the
// IssueStatusAnnouncer implementation): ONE call per issue:updated status
// change, from notifySubscribers' statusChanged branch - never per
// recipient. It pushes "「issue title」status: from -> to" to every active,
// toggle-on installation in the workspace (installation -> key, direct; no
// binding layer), through the same sender and the same merge window the run
// lifecycle announcements use, so a task:completed for the same issue and
// this status line collapse into one message (last arrival wins).
func (o *Outbound) AnnounceIssueStatusChange(ctx context.Context, workspaceID, issueID, issueTitle, from, to string) {
	wsUUID, err := parseUUID(workspaceID)
	if err != nil {
		o.logger.ErrorContext(ctx, "wecomwebhook: status announce failed",
			"reason", "bad_workspace_id", "workspace_id", workspaceID, "issue_id", issueID, "error", err)
		return
	}
	rows, err := o.q.ListChannelInstallationsByWorkspace(ctx, db.ListChannelInstallationsByWorkspaceParams{
		WorkspaceID: wsUUID,
		ChannelType: channelTypeWecomWebhook,
	})
	if err != nil {
		o.logger.ErrorContext(ctx, "wecomwebhook: status announce failed",
			"workspace_id", workspaceID, "issue_id", issueID,
			"error", fmt.Errorf("list installations: %w", err))
		return
	}
	var dests []destination
	firstSkip := ""
	for _, row := range rows {
		if row.Status != string(InstallationActive) {
			firstSkip = firstSkipOr(firstSkip, skipInstallationInactive)
			continue
		}
		inst, err := installationFromRow(row, o.open)
		if err != nil {
			o.logger.DebugContext(ctx, "wecomwebhook: status destination unusable",
				"reason", "config_unreadable", "installation_id", row.ID.String(), "error", err)
			firstSkip = firstSkipOr(firstSkip, skipKeyUnavailable)
			continue
		}
		if !inst.AnnounceStatusChanges {
			firstSkip = firstSkipOr(firstSkip, skipAnnounceDisabled)
			o.logger.DebugContext(ctx, "wecomwebhook: status destination skipped",
				"reason", skipAnnounceDisabled, "installation_id", row.ID.String(),
				"workspace_id", workspaceID, "issue_id", issueID)
			continue
		}
		if inst.Key == "" {
			firstSkip = firstSkipOr(firstSkip, skipKeyUnavailable)
			continue
		}
		dests = append(dests, destination{key: inst.Key, keySuffix: inst.KeySuffix})
	}
	if len(dests) == 0 {
		reason := firstSkip
		if reason == "" {
			reason = skipNoActiveInstallation
		}
		level := slog.LevelInfo
		if reason == skipNoActiveInstallation {
			// The structural default: no robot in this workspace. Visible,
			// but not worth an INFO line per status change forever.
			level = slog.LevelDebug
		}
		o.logger.Log(ctx, level, "wecomwebhook: announcement skipped",
			"reason", reason, "event", "issue_status_changed",
			"workspace_id", workspaceID, "issue_id", issueID)
		return
	}
	title := strings.TrimSpace(issueTitle)
	if title == "" {
		title = "Issue"
	}
	line := fmt.Sprintf("**「%s」状态：%s → %s**", title, from, to)
	o.holdStatus("issue:"+issueID, line, dests, "issue:"+issueID)
}

// firstSkipOr keeps the first (most specific) skip reason for the summary
// line when no destination was usable.
func firstSkipOr(current, candidate string) string {
	if current != "" {
		return current
	}
	return candidate
}

// sessionTitle reads the chat session's title for the status line. An empty
// or unreadable title falls back to a generic noun so the line still reads.
func (o *Outbound) sessionTitle(ctx context.Context, sessionID string) string {
	title := "对话"
	sessionUUID, err := parseSessionUUID(sessionID)
	if err != nil {
		return title
	}
	session, err := o.q.GetChatSession(ctx, sessionUUID)
	if err != nil {
		return title
	}
	if t := strings.TrimSpace(session.Title); t != "" {
		return t
	}
	return title
}

func (o *Outbound) logSkip(ctx context.Context, eventType, sessionID, taskID, reason string) {
	o.logger.InfoContext(ctx, "wecomwebhook: announcement skipped",
		"reason", reason, "event", eventType, "chat_session_id", sessionID, "task_id", taskID)
}

// holdStatus records (or replaces) a held status line and restarts its
// window. Last event wins; the replaced entry's timer self-invalidates
// through the gen bump.
//
// linkedKey merges the run-lifecycle and notify-system paths on one work
// item: a task:completed line carries its issue key, and a status_changed
// line IS its issue key. When the other side is already pending, the new
// line replaces it in place (whichever arrived later wins) instead of
// adding a second message about the same completion. Search order:
//
//   - pending[linkedKey]: a status_changed line (or a completed line that
//     already replaced one) held under the issue key;
//   - any entry whose issueKey matches: a pending task-completed line held
//     under its task key.
func (o *Outbound) holdStatus(key, line string, dests []destination, linkedKey string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.pending == nil {
		o.pending = make(map[string]*pendingStatus)
	}
	if linkedKey != "" {
		if held := o.pending[linkedKey]; held != nil {
			o.replaceHeld(linkedKey, held, line, dests)
			return
		}
		for k, held := range o.pending {
			if held.issueKey == linkedKey {
				o.replaceHeld(k, held, line, dests)
				return
			}
		}
	}
	gen := uint64(1)
	if held := o.pending[key]; held != nil {
		gen = held.gen + 1
	}
	issueKey := ""
	if strings.HasPrefix(linkedKey, "issue:") {
		issueKey = linkedKey
	}
	o.pending[key] = &pendingStatus{gen: gen, line: line, dests: dests, issueKey: issueKey}
	time.AfterFunc(o.statusWindow, func() { o.flushStatus(key, gen) })
}

// replaceHeld overwrites a pending entry with the newer line, keeping the
// entry's key, and restarts its window.
func (o *Outbound) replaceHeld(key string, held *pendingStatus, line string, dests []destination) {
	gen := held.gen + 1
	o.pending[key] = &pendingStatus{gen: gen, line: line, dests: dests, issueKey: held.issueKey}
	time.AfterFunc(o.statusWindow, func() { o.flushStatus(key, gen) })
}

// flushStatus sends the held line when its window closed without a replacing
// event or a consuming terminal message. Runs on an AfterFunc goroutine.
func (o *Outbound) flushStatus(taskID string, gen uint64) {
	o.mu.Lock()
	held := o.pending[taskID]
	if held == nil || held.gen != gen {
		// Replaced by a later event, consumed by a terminal message, or the
		// task's session turned out terminal — nothing to send.
		o.mu.Unlock()
		return
	}
	delete(o.pending, taskID)
	o.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), busHandlerTimeout)
	defer cancel()
	if err := o.sendToAll(ctx, held.dests, held.line); err != nil {
		o.logger.ErrorContext(ctx, "wecomwebhook: announce failed",
			"event", "status_window", "task_id", taskID, "error", err)
		return
	}
	o.logger.DebugContext(ctx, "wecomwebhook: announced",
		"event", "status_window", "task_id", taskID)
}

// popStatus removes and returns the task's held status line, if any.
func (o *Outbound) popStatus(taskID string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	held := o.pending[taskID]
	if held == nil {
		return ""
	}
	delete(o.pending, taskID)
	return held.line
}

// markTerminal records that a terminal announcement went out for the task so
// a later task:completed does not announce the same completion again.
func (o *Outbound) markTerminal(taskID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.terminalAt == nil {
		o.terminalAt = make(map[string]time.Time)
	}
	now := time.Now()
	o.terminalAt[taskID] = now
	// Keep the guard map small: entries older than the TTL cannot be racing
	// a completion broadcast anymore.
	for id, at := range o.terminalAt {
		if now.Sub(at) > terminalGuardTTL {
			delete(o.terminalAt, id)
		}
	}
}

func (o *Outbound) alreadyTerminal(taskID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.terminalAt[taskID].After(time.Time{})
}

// decodePayload flattens the event's payload into terminalPayload. Unknown
// shapes decode to the zero value; the caller treats empty as nothing to say.
func decodePayload(e events.Event) (terminalPayload, bool) {
	if e.Payload == nil {
		return terminalPayload{}, false
	}
	raw, err := json.Marshal(e.Payload)
	if err != nil {
		return terminalPayload{}, false
	}
	var p terminalPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return terminalPayload{}, false
	}
	return p, true
}

// sessionIDOf prefers the envelope's scope hint, falling back to the payload.
func sessionIDOf(e events.Event, p terminalPayload) string {
	if e.ChatSessionID != "" {
		return e.ChatSessionID
	}
	return p.ChatSessionID
}

// taskIDOf prefers the envelope's scope hint, falling back to the payload.
func taskIDOf(e events.Event, p terminalPayload) string {
	if e.TaskID != "" {
		return e.TaskID
	}
	return p.TaskID
}

// parseUUID is parseSessionUUID without the chat-specific wording; the
// notify-system path passes workspace and issue ids through it.
func parseUUID(id string) (pgtype.UUID, error) {
	return parseSessionUUID(id)
}

func parseSessionUUID(id string) (pgtype.UUID, error) {
	var u pgtype.UUID
	if err := u.Scan(id); err != nil {
		return pgtype.UUID{}, fmt.Errorf("wecomwebhook: parse chat_session_id %q: %w", id, err)
	}
	return u, nil
}

// renderAnnouncement builds the group-visible message for one terminal
// event, already in the wecom markdown subset. Empty means "nothing to
// announce" (an agent turn that ended with no reply, an intermediate failed
// attempt that will retry).
//
// Fixed copy is zh-Hans: WeCom deployments are China-only, the group robot
// speaks to a WeCom group, and the adapter has no per-reader language to
// resolve (unlike the reply paths, which know the asker).
func renderAnnouncement(eventType string, p terminalPayload) string {
	switch eventType {
	case protocol.EventChatDone:
		// A no_response turn carried no words; announcing "the agent finished
		// silently" tells the group nothing it can act on. The held status
		// line is left to flush on its own, so the group still sees 已完成.
		if p.MessageKind == protocol.ChatMessageKindNoResponse {
			return ""
		}
		return RenderMarkdown(p.Content)
	case protocol.EventTaskFailed:
		// An intermediate failure is a retry in progress, not an outcome.
		if p.RetryPending {
			return ""
		}
		body := `<font color="warning">**任务失败**</font>`
		if p.FailureReason != "" {
			body += "\n" + RenderMarkdown(p.FailureReason)
		}
		return body
	case protocol.EventTaskCancelled:
		return `<font color="comment">**任务已取消**</font>`
	default:
		return ""
	}
}
