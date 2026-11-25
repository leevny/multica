package wecomwebhook

// outbound.go — the announce path. Subscribes to the terminal lifecycle
// events (chat:done, task:failed, task:cancelled) and posts one rendered
// message per event into the WeCom group whose webhook the session is bound
// to, resolving the destination through channel_chat_session_binding the
// same way the wecom/telegram outbounds do:
//
//	event → chat_session → binding (channel_type='wecomwebhook') → installation → key
//
// A session with no binding is skipped silently — that is the default state
// of every session, and the binding is an explicit user action, so there is
// no "not configured" surprise to surface. Announcements ignore task origin:
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

// outboundQueries is the slice of generated queries the announce path needs.
type outboundQueries interface {
	GetChannelChatSessionBindingBySession(ctx context.Context, arg db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error)
	GetChannelInstallation(ctx context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error)
}

// messageSender is the send surface the announcer needs. *Sender satisfies
// it; tests substitute a recorder.
type messageSender interface {
	SendMarkdown(ctx context.Context, key, content string) ([]string, error)
}

// Outbound announces terminal task/chat outcomes to bound WeCom groups.
type Outbound struct {
	q      outboundQueries
	open   func([]byte) ([]byte, error)
	sender messageSender
	logger *slog.Logger
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
	return &Outbound{q: q, open: open, sender: sender, logger: logger}, nil
}

// Register subscribes the terminal lifecycle events. Everything else —
// progress, inbox, typing — is out of scope for a webhook; see the file
// comment.
func (o *Outbound) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventChatDone, o.handleEvent)
	bus.Subscribe(protocol.EventTaskFailed, o.handleEvent)
	bus.Subscribe(protocol.EventTaskCancelled, o.handleEvent)
}

// terminalPayload is the union of the fields the three events can carry.
// The payloads arrive as either a struct (chat:done) or a map
// (task:failed/task:cancelled); re-marshalling through JSON is the one
// decoder that handles both without per-event branches.
type terminalPayload struct {
	TaskID        string `json:"task_id"`
	ChatSessionID string `json:"chat_session_id"`
	Content       string `json:"content"`
	MessageKind   string `json:"message_kind"`
	FailureReason string `json:"failure_reason"`
	RetryPending  bool   `json:"retry_pending"`
}

func (o *Outbound) handleEvent(e events.Event) {
	ctx, cancel := context.WithTimeout(context.Background(), busHandlerTimeout)
	defer cancel()
	if err := o.processEvent(ctx, e); err != nil {
		// An announcement failure is never surfaced to a caller — the
		// publisher has moved on. Log with enough to find the session.
		o.logger.ErrorContext(ctx, "wecomwebhook: announce failed",
			"event", e.Type, "chat_session_id", e.ChatSessionID, "task_id", e.TaskID, "error", err)
	}
}

func (o *Outbound) processEvent(ctx context.Context, e events.Event) error {
	p, ok := decodePayload(e)
	if !ok {
		return nil
	}
	sessionID := e.ChatSessionID
	if sessionID == "" {
		sessionID = p.ChatSessionID
	}
	// Issue / autopilot runs have no chat session and nothing to announce.
	if sessionID == "" {
		return nil
	}
	sessionUUID, err := parseSessionUUID(sessionID)
	if err != nil {
		return nil
	}
	binding, err := o.q.GetChannelChatSessionBindingBySession(ctx, db.GetChannelChatSessionBindingBySessionParams{
		ChatSessionID: sessionUUID,
		ChannelType:   channelTypeWecomWebhook,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Not bound — the ordinary case, silently skipped.
			return nil
		}
		return fmt.Errorf("resolve binding: %w", err)
	}
	inst, err := o.q.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          binding.InstallationID,
		ChannelType: channelTypeWecomWebhook,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load installation: %w", err)
	}
	if inst.Status != string(InstallationActive) {
		// Revoked (or otherwise not live): the group no longer receives
		// pushes. Silent, same policy as the wecom outbound.
		return nil
	}
	installation, err := installationFromRow(inst, o.open)
	if err != nil {
		return err
	}
	if installation.Key == "" {
		return errors.New("installation key unavailable")
	}

	message := renderAnnouncement(e.Type, p)
	if message == "" {
		return nil
	}
	ids, err := o.sender.SendMarkdown(ctx, installation.Key, message)
	if err != nil {
		return fmt.Errorf("announce to group (key suffix %s): %w", installation.KeySuffix, err)
	}
	o.logger.DebugContext(ctx, "wecomwebhook: announced",
		"event", e.Type, "chat_session_id", sessionID, "messages", len(ids))
	return nil
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
		// silently" tells the group nothing it can act on.
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
