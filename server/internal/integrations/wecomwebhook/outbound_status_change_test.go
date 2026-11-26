package wecomwebhook

// outbound_status_change_test.go — the notify-system hook: issue status
// changed pushes to the workspace's installations, the installation toggle,
// workspace isolation, and the dedup with run-lifecycle completion lines.

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const (
	wsAID   = "00000000-0000-0000-0000-0000000000a1"
	wsBID   = "00000000-0000-0000-0000-0000000000b2"
	issueID = "00000000-0000-0000-0000-0000000000c3"
)

// installRow builds a channel_installation row whose config carries the
// announce toggle. The "key" rides the app_id slot and doubles as the
// decrypted key under the test's identity decrypt.
func installRow(wsID, key string, announce *bool) db.ChannelInstallation {
	raw, _ := json.Marshal(installConfig{AppID: key, KeyEncrypted: []byte(key), AnnounceStatusChanges: announce})
	return db.ChannelInstallation{
		ID:          pgUUIDFromString(wsID),
		WorkspaceID: pgUUIDFromString(wsID),
		Status:      "active",
		Config:      raw,
	}
}

func pgUUIDFromString(id string) (u pgtype.UUID) {
	_ = u.Scan(id)
	return u
}

// statusChangeFixture wires an Outbound whose workspace listing serves the
// given rows per workspace id.
func statusChangeFixture(rows map[string][]db.ChannelInstallation) (*Outbound, *recordingSender, *logRecorder, error) {
	sender := &recordingSender{}
	rec := &logRecorder{}
	q := &fakeOutboundQueries{workspaceRows: rows}
	o, err := NewOutbound(q, testDecrypt, sender, slog.New(rec))
	return o, sender, rec, err
}

// statusChangeWithSessionFixture also wires the session path (binding +
// installation) so run-lifecycle events resolve, as they do for a session
// bound to one of the workspace's installations.
func statusChangeWithSessionFixture(rows map[string][]db.ChannelInstallation) (*Outbound, *recordingSender, *logRecorder, error) {
	sender := &recordingSender{}
	rec := &logRecorder{}
	q := &fakeOutboundQueries{
		workspaceRows: rows,
		binding:       db.ChannelChatSessionBinding{InstallationID: pgUUIDFromString(wsAID)},
		inst:          installRow(wsAID, "wskA1", nil),
	}
	o, err := NewOutbound(q, testDecrypt, sender, slog.New(rec))
	return o, sender, rec, err
}

func TestStatusChangeAnnouncesToWorkspaceInstallations(t *testing.T) {
	o, sender, _, err := statusChangeFixture(map[string][]db.ChannelInstallation{
		wsAID: {installRow(wsAID, "wskA1", nil)},
	})
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 60 * time.Millisecond

	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "started", "done")
	// Held inside the window — the dedup with a run completion needs the
	// line to be replaceable.
	if len(sender.snapshot()) != 0 {
		t.Fatalf("status line must hold inside the window, got %d sends", len(sender.snapshot()))
	}
	time.Sleep(250 * time.Millisecond)
	snap := sender.snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 status push, got %d", len(snap))
	}
	if snap[0].key != "wskA1" {
		t.Fatalf("push must go to the workspace installation, got key %q", snap[0].key)
	}
	if !strings.Contains(snap[0].content, "「Ops Issue」状态：started → done") {
		t.Fatalf("unexpected status line: %q", snap[0].content)
	}
}

func TestStatusChangeEveryInstallationReceives(t *testing.T) {
	o, sender, _, err := statusChangeFixture(map[string][]db.ChannelInstallation{
		wsAID: {installRow(wsAID, "wskA1", nil), installRow(wsAID, "wskA2", nil)},
	})
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 60 * time.Millisecond

	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "todo", "in_progress")
	time.Sleep(250 * time.Millisecond)
	snap := sender.snapshot()
	if len(snap) != 2 {
		t.Fatalf("each active installation gets one push, got %d", len(snap))
	}
	if snap[0].content != snap[1].content {
		t.Fatalf("both groups must see the same line")
	}
}

func TestStatusChangeToggleOffSkips(t *testing.T) {
	off := false
	o, sender, _, err := statusChangeFixture(map[string][]db.ChannelInstallation{
		wsAID: {installRow(wsAID, "wskA1", &off)},
	})
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 40 * time.Millisecond

	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "started", "done")
	time.Sleep(150 * time.Millisecond)
	if len(sender.snapshot()) != 0 {
		t.Fatalf("toggle off must not push, got %d sends", len(sender.snapshot()))
	}
}

func TestStatusChangeCrossWorkspaceIsolation(t *testing.T) {
	o, sender, _, err := statusChangeFixture(map[string][]db.ChannelInstallation{
		// A robot exists in workspace B; the change happens in workspace A.
		wsBID: {installRow(wsBID, "wskB1", nil)},
	})
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 40 * time.Millisecond

	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "started", "done")
	time.Sleep(150 * time.Millisecond)
	if len(sender.snapshot()) != 0 {
		t.Fatalf("another workspace's robot must never receive the push, got %d sends", len(sender.snapshot()))
	}
}

func TestStatusChangeRevokedSkipsWithLog(t *testing.T) {
	revoked := installRow(wsAID, "wskA1", nil)
	revoked.Status = "revoked"
	o, sender, rec, err := statusChangeFixture(map[string][]db.ChannelInstallation{
		wsAID: {revoked},
	})
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 40 * time.Millisecond

	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "started", "done")
	time.Sleep(150 * time.Millisecond)
	if len(sender.snapshot()) != 0 {
		t.Fatalf("revoked installation must not receive pushes")
	}
	if !rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "reason", skipInstallationInactive) {
		t.Fatal("missing INFO skip log with reason=installation_not_active")
	}
	if !rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "issue_id", issueID) {
		t.Fatal("skip log must carry the issue id")
	}
}

func TestStatusChangeNoInstallationLogsDebug(t *testing.T) {
	o, sender, rec, err := statusChangeFixture(nil)
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "started", "done")
	if len(sender.snapshot()) != 0 {
		t.Fatal("no installation must not push")
	}
	// The structural default is DEBUG — an INFO line per status change for
	// every robot-less workspace would drown the log.
	if rec.find(slog.LevelInfo, "wecomwebhook: announcement skipped", "reason", skipNoActiveInstallation) {
		t.Fatal("no-installation skip must not log at INFO")
	}
}

// pubCompleted publishes a task:completed event linked to the fixture issue,
// as the task lifecycle listener receives it.
func pubCompleted(bus *events.Bus, taskID string) {
	bus.Publish(events.Event{
		Type:          protocol.EventTaskCompleted,
		TaskID:        taskID,
		ChatSessionID: sessionID(),
		Payload: map[string]any{
			"task_id":         taskID,
			"issue_id":        issueID,
			"chat_session_id": sessionID(),
			"status":          "completed",
		},
	})
}

func TestCompletedThenStatusChangeMergeLastWins(t *testing.T) {
	o, sender, _, err := statusChangeWithSessionFixture(map[string][]db.ChannelInstallation{
		wsAID: {installRow(wsAID, "wskA1", nil)},
	})
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	// The session path also resolves: the same workspace's installation backs
	// the binding, so the completed line has a destination.
	o.statusWindow = 80 * time.Millisecond
	bus := newBus(t, o)

	// task:completed for the chat task of this issue pends first...
	pubCompleted(bus, "t1")
	// ...then the issue status change (agent moved the issue to done) lands
	// inside the window: last arrival wins, one message.
	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "started", "done")
	time.Sleep(400 * time.Millisecond)
	snap := sender.snapshot()
	if len(snap) != 1 {
		t.Fatalf("completed and status_changed must merge into one message, got %d", len(snap))
	}
	if !strings.Contains(snap[0].content, "状态：started → done") {
		t.Fatalf("the later (status) line must win: %q", snap[0].content)
	}
}

func TestStatusChangeThenCompletedMergeLastWins(t *testing.T) {
	o, sender, _, err := statusChangeWithSessionFixture(map[string][]db.ChannelInstallation{
		wsAID: {installRow(wsAID, "wskA1", nil)},
	})
	if err != nil {
		t.Fatalf("NewOutbound: %v", err)
	}
	o.statusWindow = 80 * time.Millisecond
	bus := newBus(t, o)

	// Status change pends first...
	o.AnnounceIssueStatusChange(context.Background(), wsAID, issueID, "Ops Issue", "started", "done")
	// ...then the run's task:completed arrives: last arrival wins.
	pubCompleted(bus, "t1")
	time.Sleep(400 * time.Millisecond)
	snap := sender.snapshot()
	if len(snap) != 1 {
		t.Fatalf("status_changed and completed must merge into one message, got %d", len(snap))
	}
	if !strings.Contains(snap[0].content, "已完成") {
		t.Fatalf("the later (completed) line must win: %q", snap[0].content)
	}
}
