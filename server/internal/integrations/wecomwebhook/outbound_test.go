package wecomwebhook

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// fakeOutboundQueries answers the reads the announce path makes.
type fakeOutboundQueries struct {
	binding      db.ChannelChatSessionBinding
	bindingErr   error
	inst         db.ChannelInstallation
	instErr      error
	sessionTitle string
}

func (f *fakeOutboundQueries) GetChatSession(_ context.Context, id pgtype.UUID) (db.ChatSession, error) {
	title := f.sessionTitle
	if title == "" {
		title = "发布新版本"
	}
	return db.ChatSession{ID: id, Title: title}, nil
}

func (f *fakeOutboundQueries) GetChannelChatSessionBindingBySession(_ context.Context, arg db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error) {
	if f.bindingErr != nil {
		return db.ChannelChatSessionBinding{}, f.bindingErr
	}
	if arg.ChannelType != "wecomwebhook" {
		return db.ChannelChatSessionBinding{}, pgx.ErrNoRows
	}
	return f.binding, nil
}

func (f *fakeOutboundQueries) GetChannelInstallation(_ context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error) {
	if f.instErr != nil {
		return db.ChannelInstallation{}, f.instErr
	}
	return f.inst, nil
}

// recordingSender captures announcements without touching the network. The
// flush timers run on their own goroutines, so reads and writes are locked.
type recordingSender struct {
	mu       sync.Mutex
	messages [][]string
}

func (s *recordingSender) SendMarkdown(_ context.Context, _ string, content string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, chunkUTF8(content, MaxMarkdownBytes))
	return []string{"id-1"}, nil
}

// snapshot returns a copy of the recorded announcements.
func (s *recordingSender) snapshot() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.messages...)
}

func (s *recordingSender) SendText(ctx context.Context, key string, content string) ([]string, error) {
	return s.SendMarkdown(ctx, key, content)
}

// testDecrypt stands in for secretbox.Open: identity, testable.
func testDecrypt(b []byte) ([]byte, error) { return b, nil }

func sessionID() string { return "00000000-0000-0000-0000-0000000000aa" }

func outboundFixture(instStatus string) (*Outbound, *recordingSender, *fakeOutboundQueries, error) {
	sender := &recordingSender{}
	q := &fakeOutboundQueries{
		binding: db.ChannelChatSessionBinding{
			InstallationID: pgtype.UUID{},
		},
		inst: db.ChannelInstallation{Status: instStatus, Config: boundConfig()},
	}
	o, err := NewOutbound(q, testDecrypt, sender, nil)
	return o, sender, q, err
}

func boundConfig() json.RawMessage {
	raw, _ := json.Marshal(installConfig{AppID: "abcdefgh1234", KeyEncrypted: []byte("test-key")})
	return raw
}

func runEvent(t *testing.T, o *Outbound, eventType string, payload map[string]any) {
	t.Helper()
	e := events.Event{Type: eventType, ChatSessionID: sessionID(), TaskID: "t1", Payload: payload}
	bus := events.New()
	o.Register(bus)
	bus.Publish(e)
}

func TestOutboundAnnouncesChatDone(t *testing.T) {
	o, sender, _, err := outboundFixture("active")
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	runEvent(t, o, protocol.EventChatDone, map[string]any{
		"chat_session_id": sessionID(),
		"content":         "## 结论\n\n- 第一点\n* 第二点",
	})
	snap := sender.snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 announcement, got %d", len(snap))
	}
	msg := snap[0][0]
	if !strings.Contains(msg, "## 结论") || !strings.Contains(msg, "- 第一点") || strings.Contains(msg, "* 第二点") {
		t.Fatalf("announcement not rendered through the subset renderer: %q", msg)
	}
}

func TestOutboundSkipsUnboundSession(t *testing.T) {
	o, sender, q, _ := outboundFixture("active")
	q.bindingErr = pgx.ErrNoRows
	runEvent(t, o, protocol.EventChatDone, map[string]any{"content": "hello"})
	if len(sender.snapshot()) != 0 {
		t.Fatalf("unbound session must be silently skipped, got %d sends", len(sender.snapshot()))
	}
}

func TestOutboundSkipsNoResponseTurn(t *testing.T) {
	o, sender, _, _ := outboundFixture("active")
	runEvent(t, o, protocol.EventChatDone, map[string]any{
		"content":      "",
		"message_kind": protocol.ChatMessageKindNoResponse,
	})
	if len(sender.snapshot()) != 0 {
		t.Fatalf("no_response turn must not announce, got %d sends", len(sender.snapshot()))
	}
}

func TestOutboundSkipsIntermediateFailure(t *testing.T) {
	o, sender, _, _ := outboundFixture("active")
	runEvent(t, o, protocol.EventTaskFailed, map[string]any{
		"failure_reason": "boom",
		"retry_pending":  true,
	})
	if len(sender.snapshot()) != 0 {
		t.Fatalf("retry-pending failure must not announce, got %d sends", len(sender.snapshot()))
	}
}

func TestOutboundAnnouncesTerminalFailure(t *testing.T) {
	o, sender, _, _ := outboundFixture("active")
	runEvent(t, o, protocol.EventTaskFailed, map[string]any{
		"failure_reason": "runtime crash",
		"retry_pending":  false,
	})
	snap := sender.snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 announcement, got %d", len(snap))
	}
	if !strings.Contains(snap[0][0], "任务失败") || !strings.Contains(snap[0][0], "runtime crash") {
		t.Fatalf("failure notice missing copy: %q", snap[0][0])
	}
}

func TestOutboundAnnouncesCancellation(t *testing.T) {
	o, sender, _, _ := outboundFixture("active")
	runEvent(t, o, protocol.EventTaskCancelled, map[string]any{})
	snap := sender.snapshot()
	if len(snap) != 1 || !strings.Contains(snap[0][0], "任务已取消") {
		t.Fatalf("cancellation notice missing: %q", snap)
	}
}

func TestOutboundSkipsRevokedInstallation(t *testing.T) {
	o, sender, _, _ := outboundFixture("revoked")
	runEvent(t, o, protocol.EventChatDone, map[string]any{"content": "hello"})
	if len(sender.snapshot()) != 0 {
		t.Fatalf("revoked installation must not receive pushes, got %d sends", len(sender.snapshot()))
	}
}

func TestOutboundSkipsIssueTasks(t *testing.T) {
	o, sender, _, _ := outboundFixture("active")
	e := events.Event{Type: protocol.EventChatDone, TaskID: "t1", Payload: map[string]any{"content": "x"}}
	bus := events.New()
	o.Register(bus)
	bus.Publish(e)
	if len(sender.snapshot()) != 0 {
		t.Fatalf("event without a chat session must not announce, got %d sends", len(sender.snapshot()))
	}
}

func TestOutboundLongReplyChunks(t *testing.T) {
	o, sender, _, _ := outboundFixture("active")
	long := strings.Repeat("很长的回复", 300) // 1800 bytes × ... > 4096
	runEvent(t, o, protocol.EventChatDone, map[string]any{"content": long})
	snap := sender.snapshot()
	if len(snap) != 1 || len(snap[0]) < 2 {
		t.Fatalf("long reply must arrive as multiple messages, got %v", snap[0])
	}
	total := 0
	for _, piece := range snap[0] {
		if len(piece) > MaxMarkdownBytes {
			t.Fatalf("piece exceeds markdown limit")
		}
		total += len(piece)
	}
	if total != len(long) {
		t.Fatalf("chunks lost content: %d vs %d", total, len(long))
	}
}

func TestOutboundRequiresDecrypt(t *testing.T) {
	if _, err := NewOutbound(&fakeOutboundQueries{}, nil, &recordingSender{}, nil); err == nil {
		t.Fatal("nil decrypt must refuse construction")
	}
}

func TestOutboundEventWithoutPayload(t *testing.T) {
	o, sender, _, _ := outboundFixture("active")
	e := events.Event{Type: protocol.EventChatDone, ChatSessionID: sessionID()}
	bus := events.New()
	o.Register(bus)
	bus.Publish(e)
	if len(sender.snapshot()) != 0 {
		t.Fatalf("payload-less event must not announce, got %d sends", len(sender.snapshot()))
	}
}
