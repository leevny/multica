package wecomwebhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// fakeInstallQueries records the install transaction's calls so tests assert
// on the flow without Postgres.
type fakeInstallQueries struct {
	upserted    db.UpsertChannelInstallationParams
	upsertCalls int
	upsertErr   error
	reclaimed   int
	configSet   db.SetChannelInstallationConfigParams
}

func (f *fakeInstallQueries) WithTx(pgx.Tx) installQueries { return f }

func (f *fakeInstallQueries) UpsertChannelInstallation(_ context.Context, arg db.UpsertChannelInstallationParams) (db.ChannelInstallation, error) {
	f.upsertCalls++
	f.upserted = arg
	if f.upsertErr != nil {
		return db.ChannelInstallation{}, f.upsertErr
	}
	return db.ChannelInstallation{ID: arg.WorkspaceID, WorkspaceID: arg.WorkspaceID, AgentID: arg.AgentID, Status: "active"}, nil
}

func (f *fakeInstallQueries) ReclaimDeadChannelInstallationByAppID(_ context.Context, _ db.ReclaimDeadChannelInstallationByAppIDParams) (pgtype.UUID, error) {
	f.reclaimed++
	return pgtype.UUID{}, pgx.ErrNoRows
}

func (f *fakeInstallQueries) GetChannelInstallationOwnerByAppID(_ context.Context, _ db.GetChannelInstallationOwnerByAppIDParams) (db.GetChannelInstallationOwnerByAppIDRow, error) {
	return db.GetChannelInstallationOwnerByAppIDRow{}, errors.New("no rows")
}

func (f *fakeInstallQueries) ListChannelInstallationsByWorkspace(_ context.Context, _ db.ListChannelInstallationsByWorkspaceParams) ([]db.ChannelInstallation, error) {
	return nil, nil
}

func (f *fakeInstallQueries) GetChannelInstallationInWorkspace(_ context.Context, _ db.GetChannelInstallationInWorkspaceParams) (db.ChannelInstallation, error) {
	return db.ChannelInstallation{}, pgx.ErrNoRows
}

func (f *fakeInstallQueries) SetChannelInstallationStatus(_ context.Context, _ db.SetChannelInstallationStatusParams) error {
	return nil
}

func (f *fakeInstallQueries) SetChannelInstallationConfig(_ context.Context, arg db.SetChannelInstallationConfigParams) error {
	f.configSet = arg
	return nil
}

// countingTx records commits so tests assert the tx boundary without
// Postgres. Begin hands back a tx whose Commit increments the same counter —
// persistInstall commits the BEGIN-returned tx, not the starter. The nil
// pgx.Tx embed satisfies the interface; Commit/Rollback override the only
// methods that ever run.
type countingTx struct {
	mu      sync.Mutex
	commits int
}

func (t *countingTx) Begin(context.Context) (pgx.Tx, error) {
	return countingBegin{Tx: nil, parent: t}, nil
}
func (t *countingTx) record() {
	t.mu.Lock()
	t.commits++
	t.mu.Unlock()
}

type countingBegin struct {
	pgx.Tx
	parent *countingTx
}

func (t countingBegin) Commit(context.Context) error { t.parent.record(); return nil }
func (t countingBegin) Rollback(context.Context) error {
	return nil
}

// okServer answers errcode 0 like a live webhook.
func okServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestInstallService(t *testing.T, sender *Sender, q *fakeInstallQueries) (*InstallService, *countingTx) {
	t.Helper()
	key := make([]byte, 32)
	box, err := secretbox.New(key)
	if err != nil {
		t.Fatalf("secretbox.New: %v", err)
	}
	tx := &countingTx{}
	svc, err := newInstallService(q, tx, box, sender, nil)
	if err != nil {
		t.Fatalf("newInstallService: %v", err)
	}
	return svc, tx
}

func wsUUID() pgtype.UUID {
	var u pgtype.UUID
	_ = u.Scan("00000000-0000-0000-0000-000000000001")
	return u
}

func TestInstallSendsTestMessageThenPersists(t *testing.T) {
	sender := NewSender(WithBaseURL(okServer(t).URL))
	q := &fakeInstallQueries{}
	svc, tx := newTestInstallService(t, sender, q)

	row, err := svc.Register(context.Background(), RegisterParams{
		WorkspaceID: wsUUID(), AgentID: wsUUID(), InitiatorID: wsUUID(),
		Key: "  0123456789abcdef ",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if row.Status != "active" {
		t.Fatalf("want active row, got %q", row.Status)
	}
	if q.upsertCalls != 1 || q.upserted.ChannelType != "wecomwebhook" {
		t.Fatalf("want one wecomwebhook upsert, got %d", q.upsertCalls)
	}
	if q.upserted.InstallerUserID != wsUUID() {
		t.Fatal("installer id must ride the upsert")
	}
	if tx.commits != 1 {
		t.Fatalf("want exactly one commit, got %d", tx.commits)
	}
	if q.reclaimed != 1 {
		t.Fatal("dead-slot reclaim must run before the upsert")
	}
	// The config must carry the key in the app_id slot and a sealed copy —
	// the plaintext rides only the indexed slot, never a second bare field.
	var cfg installConfig
	if err := json.Unmarshal(q.upserted.Config, &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if cfg.AppID != "0123456789abcdef" {
		t.Fatalf("app_id slot must hold the trimmed key, got %q", cfg.AppID)
	}
	if len(cfg.KeyEncrypted) == 0 || string(cfg.KeyEncrypted) == "0123456789abcdef" {
		t.Fatal("key_encrypted must hold a sealed copy, not the plaintext")
	}
}

func TestInstallRefusedKeyNeverPersists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"errcode":93000,"errmsg":"invalid webhook url"}`))
	}))
	defer srv.Close()
	q := &fakeInstallQueries{}
	svc, tx := newTestInstallService(t, NewSender(WithBaseURL(srv.URL)), q)

	_, err := svc.Register(context.Background(), RegisterParams{
		WorkspaceID: wsUUID(), AgentID: wsUUID(), InitiatorID: wsUUID(),
		Key: "0123456789abcdef",
	})
	if !errors.Is(err, ErrKeyRejected) {
		t.Fatalf("want ErrKeyRejected, got %v", err)
	}
	if q.upsertCalls != 0 || tx.commits != 0 {
		t.Fatalf("a rejected key must not touch the DB (upserts %d, commits %d)", q.upsertCalls, tx.commits)
	}
}

func TestInstallUnreachableNeverPersists(t *testing.T) {
	q := &fakeInstallQueries{}
	svc, tx := newTestInstallService(t, NewSender(WithBaseURL("http://127.0.0.1:1")), q)

	_, err := svc.Register(context.Background(), RegisterParams{
		WorkspaceID: wsUUID(), AgentID: wsUUID(), InitiatorID: wsUUID(),
		Key: "0123456789abcdef",
	})
	if !errors.Is(err, ErrCredentialsUnverifiable) {
		t.Fatalf("want ErrCredentialsUnverifiable, got %v", err)
	}
	if q.upsertCalls != 0 || tx.commits != 0 {
		t.Fatalf("an unverifiable key must not touch the DB (upserts %d, commits %d)", q.upsertCalls, tx.commits)
	}
}

func TestInstallKeyShapeValidation(t *testing.T) {
	q := &fakeInstallQueries{}
	svc, _ := newTestInstallService(t, NewSender(WithBaseURL("http://127.0.0.1:1")), q)
	for _, key := range []string{"", "short", strings.Repeat("a", 200), "has space inside", "全角ｋｅｙ"} {
		_, err := svc.Register(context.Background(), RegisterParams{
			WorkspaceID: wsUUID(), AgentID: wsUUID(), InitiatorID: wsUUID(), Key: key,
		})
		if !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("key %q: want ErrInvalidKey, got %v", key, err)
		}
	}
}

func TestInstallUniqueViolationClassifiesOwner(t *testing.T) {
	// A unique violation with an owner-lookup failure must fall back to the
	// cross-workspace conflict, never a raw Postgres string.
	q := &fakeInstallQueries{
		upsertErr: &pgconn.PgError{Code: pgUniqueViolation, Message: "duplicate key"},
	}
	svc, _ := newTestInstallService(t, NewSender(WithBaseURL(okServer(t).URL)), q)

	_, err := svc.Register(context.Background(), RegisterParams{
		WorkspaceID: wsUUID(), AgentID: wsUUID(), InitiatorID: wsUUID(), Key: "0123456789abcdef",
	})
	if !errors.Is(err, ErrBotOwnedByAnotherWorkspace) {
		t.Fatalf("want ErrBotOwnedByAnotherWorkspace, got %v", err)
	}
}

func TestKeySuffix(t *testing.T) {
	if got := keySuffix("abcd1234"); got != "1234" {
		t.Fatalf("keySuffix = %q", got)
	}
	if got := keySuffix("abc"); got != "…" {
		t.Fatalf("short key suffix = %q", got)
	}
}

func TestPublicConfigFromRaw(t *testing.T) {
	off := false
	cfg, err := encodeInstallConfig("abcdefgh1234", []byte{1, 2, 3}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	view := PublicConfigFromRaw(cfg)
	if view.KeySuffix != "1234" || !view.AnnounceStatusChanges {
		t.Fatalf("view = %+v, want suffix 1234 and toggle on (absent = on)", view)
	}
	cfg, err = encodeInstallConfig("abcdefgh1234", []byte{1, 2, 3}, &off)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if view := PublicConfigFromRaw(cfg); view.AnnounceStatusChanges {
		t.Fatal("stored false must read back false")
	}
	if view := PublicConfigFromRaw([]byte("{bad")); view.KeySuffix != "…" || !view.AnnounceStatusChanges {
		t.Fatalf("malformed config must degrade to the placeholder view, got %+v", view)
	}
}

func TestSetAnnounceStatusChangesRewritesConfig(t *testing.T) {
	q := &fakeInstallQueries{}
	svc, _ := newTestInstallService(t, NewSender(WithBaseURL("http://127.0.0.1:1")), q)
	// GetInWorkspace returns ErrNoRows on the fake — inject the row.
	svc.q = stubbedInstallQueries{q, db.ChannelInstallation{
		ID: wsUUID(), WorkspaceID: wsUUID(), Status: "active",
		Config: mustConfig(t, "abcdefgh1234", nil),
	}}

	row, err := svc.SetAnnounceStatusChanges(context.Background(), wsUUID(), wsUUID(), false)
	if err != nil {
		t.Fatalf("SetAnnounceStatusChanges: %v", err)
	}
	if q.configSet.ID != wsUUID() {
		t.Fatal("config write must target the installation")
	}
	var cfg installConfig
	if err := json.Unmarshal(q.configSet.Config, &cfg); err != nil {
		t.Fatalf("decode written config: %v", err)
	}
	if cfg.AnnounceStatusChanges == nil || *cfg.AnnounceStatusChanges {
		t.Fatal("toggle off must be stored as an explicit false")
	}
	if cfg.AppID != "abcdefgh1234" || len(cfg.KeyEncrypted) == 0 {
		t.Fatal("the rest of the config blob must survive the rewrite")
	}
	if row.Config == nil {
		t.Fatal("returned row must carry the updated config")
	}
}

func mustConfig(t *testing.T, key string, announce *bool) []byte {
	t.Helper()
	// The sealed bytes are fake here; only the JSON shape matters.
	raw, err := json.Marshal(installConfig{AppID: key, KeyEncrypted: []byte{1}, AnnounceStatusChanges: announce})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return raw
}

// stubbedInstallQueries overrides GetChannelInstallationInWorkspace with a
// fixed row for the config-patch flow.
type stubbedInstallQueries struct {
	*fakeInstallQueries
	row db.ChannelInstallation
}

func (s stubbedInstallQueries) GetChannelInstallationInWorkspace(_ context.Context, _ db.GetChannelInstallationInWorkspaceParams) (db.ChannelInstallation, error) {
	return s.row, nil
}
