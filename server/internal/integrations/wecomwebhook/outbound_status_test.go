package wecomwebhook

// outbound_status_test.go — the status-line announcer: verb mapping,
// last-state-wins coalescing, terminal merge, the completed-after-terminal
// guard, and the skip visibility the spec requires.

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// logRecorder captures slog records so tests assert the skip visibility the
// spec requires.
type logRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}

func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

// find reports whether a record with the level, message, and attr key/value
// pair was captured.
func (r *logRecorder) find(level slog.Level, msg, key, value string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.records {
		if rec.Level != level || rec.Message != msg {
			continue
		}
		matched := false
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == key && a.Value.String() == value {
				matched = true
				return false
			}
			return true
		})
		if matched {
			return true
		}
	}
	return false
}

// pubStatus publishes one lifecycle event on a shared bus with the fixture's
// session and task ids.
func pubStatus(bus *events.Bus, eventType, taskID string) {
	bus.Publish(events.Event{
		Type:          eventType,
		TaskID:        taskID,
		ChatSessionID: sessionID(),
		Payload: map[string]any{
			"task_id":         taskID,
			"chat_session_id": sessionID(),
		},
	})
}

func pubChatDoneOn(bus *events.Bus, taskID string, payload map[string]any) {
	bus.Publish(events.Event{
		Type:          protocol.EventChatDone,
		TaskID:        taskID,
		ChatSessionID: sessionID(),
		Payload:       payload,
	})
}

func newBus(t *testing.T, o *Outbound) *events.Bus {
	t.Helper()
	bus := events.New()
	o.Register(bus)
	return bus
}

func totalMessages(sender *recordingSender) int {
	n := 0
	for _, chunks := range sender.snapshot() {
		n += len(chunks)
	}
	return n
}

func TestStatusVerbsMapping(t *testing.T) {
	want := map[string]string{
		protocol.EventTaskQueued:                "已入队",
		protocol.EventTaskDispatch:              "开始执行",
		protocol.EventTaskRunning:               "开始执行",
		protocol.EventTaskWaitingLocalDirectory: "等待本地目录",
		protocol.EventTaskCompleted:             "已完成",
	}
	for event, verb := range want {
		if got := statusVerbs[event]; got != verb {
			t.Fatalf("statusVerbs[%s] = %q, want %q", event, got, verb)
		}
	}
	// Register actually subscribes every mapped event — a mapping without a
	// subscription would silently never announce.
	o, _, _, err := outboundFixture("active")
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	bus := newBus(t, o)
	for event := range want {
		if bus.SubscriberCount(event) != 1 {
			t.Fatalf("event %s has no subscriber", event)
		}
	}
	// The noise events the spec excludes stay unsubscribed.
	for _, excluded := range []string{
		protocol.EventTaskCoalesced,
		protocol.EventTaskProgress,
		protocol.EventTaskUsage,
		protocol.EventTaskMessage,
	} {
		if bus.SubscriberCount(excluded) != 0 {
			t.Fatalf("noise event %s must not be subscribed", excluded)
		}
	}
}

func TestStatusEventsCoalesceLastWins(t *testing.T) {
	o, sender, _, err := outboundFixture("active")
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 80 * time.Millisecond
	bus := newBus(t, o)

	pubStatus(bus, protocol.EventTaskQueued, "t1")
	pubStatus(bus, protocol.EventTaskDispatch, "t1")
	pubStatus(bus, protocol.EventTaskRunning, "t1")
	// Inside the window nothing has gone out yet.
	if totalMessages(sender) != 0 {
		t.Fatalf("no send may leave the window before it closes, got %d", totalMessages(sender))
	}
	time.Sleep(300 * time.Millisecond)
	if totalMessages(sender) != 1 {
		t.Fatalf("want exactly one coalesced line, got %d", totalMessages(sender))
	}
	line := sender.snapshot()[0][0]
	if !strings.Contains(line, "开始执行") || strings.Contains(line, "已入队") {
		t.Fatalf("last state must win, got %q", line)
	}
	if !strings.Contains(line, "发布新版本") {
		t.Fatalf("status line must carry the task title, got %q", line)
	}
}

func TestStatusLineMergesIntoChatDoneReply(t *testing.T) {
	o, sender, _, err := outboundFixture("active")
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 200 * time.Millisecond
	bus := newBus(t, o)

	pubStatus(bus, protocol.EventTaskRunning, "t1")
	pubChatDoneOn(bus, "t1", map[string]any{
		"chat_session_id": sessionID(),
		"content":         "答复内容",
	})
	time.Sleep(450 * time.Millisecond)
	if totalMessages(sender) != 1 {
		t.Fatalf("status line must merge into the reply, got %d messages", totalMessages(sender))
	}
	msg := strings.Join(sender.snapshot()[0], "")
	if !strings.Contains(msg, "开始执行") || !strings.Contains(msg, "答复内容") {
		t.Fatalf("merged message missing status prefix or reply: %q", msg)
	}
	if strings.Index(msg, "开始执行") > strings.Index(msg, "答复内容") {
		t.Fatalf("status line must be the reply's prefix: %q", msg)
	}
}

func TestCompletedAfterChatDoneSkipped(t *testing.T) {
	o, sender, _, err := outboundFixture("active")
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 60 * time.Millisecond
	bus := newBus(t, o)

	// chat:done publishes BEFORE task:completed in the completion flow; the
	// reply is the completion announcement and the extra line adds nothing.
	pubChatDoneOn(bus, "t1", map[string]any{"chat_session_id": sessionID(), "content": "回复"})
	time.Sleep(50 * time.Millisecond)
	if totalMessages(sender) != 1 {
		t.Fatalf("want 1 reply, got %d", totalMessages(sender))
	}
	pubStatus(bus, protocol.EventTaskCompleted, "t1")
	time.Sleep(200 * time.Millisecond)
	if totalMessages(sender) != 1 {
		t.Fatalf("completed after a terminal announcement must be skipped, got %d", totalMessages(sender))
	}
}

func TestCompletedAnnouncesWhenNoReply(t *testing.T) {
	o, sender, _, err := outboundFixture("active")
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 60 * time.Millisecond
	bus := newBus(t, o)

	// A no_response turn sends nothing and must not arm the terminal guard:
	// the held 已完成 line is the only way the group learns the run ended.
	pubChatDoneOn(bus, "t1", map[string]any{
		"chat_session_id": sessionID(),
		"message_kind":    protocol.ChatMessageKindNoResponse,
	})
	time.Sleep(20 * time.Millisecond)
	if totalMessages(sender) != 0 {
		t.Fatalf("no_response turn must not announce, got %d", totalMessages(sender))
	}
	pubStatus(bus, protocol.EventTaskCompleted, "t1")
	time.Sleep(250 * time.Millisecond)
	if totalMessages(sender) != 1 || !strings.Contains(sender.snapshot()[0][0], "已完成") {
		t.Fatalf("completed must announce when no terminal message went out, got %v", sender.snapshot())
	}
}

func TestSkipLoggingUnboundSession(t *testing.T) {
	rec := &logRecorder{}
	sender := &recordingSender{}
	q := &fakeOutboundQueries{bindingErr: pgx.ErrNoRows}
	o, err := NewOutbound(q, testDecrypt, sender, slog.New(rec))
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	bus := newBus(t, o)
	pubStatus(bus, protocol.EventTaskRunning, "t1")
	if totalMessages(sender) != 0 {
		t.Fatalf("unbound session must not announce")
	}
	if !rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "reason", skipNoBinding) {
		t.Fatal("missing INFO skip log with reason=no_binding")
	}
	if !rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "chat_session_id", sessionID()) {
		t.Fatal("skip log must carry the session id")
	}
	if !rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "task_id", "t1") {
		t.Fatal("skip log must carry the task id")
	}
}

func TestSkipLoggingInstallationInactive(t *testing.T) {
	rec := &logRecorder{}
	sender := &recordingSender{}
	q := &fakeOutboundQueries{
		binding: db.ChannelChatSessionBinding{InstallationID: pgtype.UUID{}},
		inst:    db.ChannelInstallation{Status: "revoked", Config: boundConfig()},
	}
	o, err := NewOutbound(q, testDecrypt, sender, slog.New(rec))
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	bus := newBus(t, o)
	pubChatDoneOn(bus, "t1", map[string]any{"chat_session_id": sessionID(), "content": "hi"})
	if totalMessages(sender) != 0 {
		t.Fatalf("revoked installation must not receive pushes")
	}
	if !rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "reason", skipInstallationInactive) {
		t.Fatal("missing INFO skip log with reason=installation_not_active")
	}
	if !rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "task_id", "t1") {
		t.Fatal("skip log must carry the task id")
	}
}

func TestWindowReplacesHeldLine(t *testing.T) {
	o, sender, _, err := outboundFixture("active")
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	// Two windows close back to back: each flushes exactly one line, the
	// second carrying the later state.
	o.statusWindow = 40 * time.Millisecond
	bus := newBus(t, o)
	pubStatus(bus, protocol.EventTaskQueued, "t1")
	time.Sleep(100 * time.Millisecond) // first line flushes
	if totalMessages(sender) != 1 {
		t.Fatalf("first window must flush once, got %d", totalMessages(sender))
	}
	pubStatus(bus, protocol.EventTaskCompleted, "t1")
	time.Sleep(200 * time.Millisecond)
	if totalMessages(sender) != 2 {
		t.Fatalf("want one flush per window close, got %d", totalMessages(sender))
	}
	if !strings.Contains(sender.snapshot()[1][0], "已完成") {
		t.Fatalf("second window must carry the later state: %q", sender.snapshot()[1][0])
	}
}
