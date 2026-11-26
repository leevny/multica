package wecomwebhook

// install.go — the BYO install backend for WeCom group-robot webhooks. An
// admin creates a robot in the WeCom group settings, copies its webhook key,
// and pastes it here. The InstallService owns the at-rest encryption (no
// caller can write a channel_installation with a plaintext-at-rest key) and
// the shared persist transaction; it mirrors telegram/install.go's shape.
//
// Proof of control: a webhook key cannot be verified offline, and the
// (channel_type, app_id) slot is global across the deployment — without a
// check, anyone in any workspace could claim a foreign group's key and post
// into that group as them. The only possible verification is really posting
// a test message with the key, so Register does exactly that and refuses to
// persist on any refusal. The test doubles as the "test" affordance: the
// admin sees the message land in the group at the moment the install
// succeeds.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

var (
	// ErrInstallationNotFound surfaces "no row matches in this workspace".
	ErrInstallationNotFound = errors.New("wecomwebhook: installation not found")
	// ErrInvalidKey is returned when the pasted value cannot be a webhook
	// key at all (empty, oversized, whitespace/control chars) — mapped to 400.
	ErrInvalidKey = errors.New("wecomwebhook: this does not look like a WeCom webhook key")
	// ErrKeyRejected means WeCom itself refused the test message (nonzero
	// errcode) — the key is wrong, the robot was deleted, or the webhook was
	// revoked. Keep it distinct from an unreachable API so users are never
	// told to rotate a valid key because the deployment's network is down.
	ErrKeyRejected = errors.New("wecomwebhook: WeCom rejected this webhook key")
	// ErrCredentialsUnverifiable means Multica could not complete the live
	// test post. The key has not been persisted or changed.
	ErrCredentialsUnverifiable = errors.New("wecomwebhook: could not reach WeCom to verify this webhook key")
	// ErrBotOwnedByAnotherWorkspace: the pasted webhook is already connected
	// to a live owner in a DIFFERENT Multica workspace.
	ErrBotOwnedByAnotherWorkspace = errors.New("wecomwebhook: this webhook is already connected to a different Multica workspace")
	// ErrBotOwnedBySameWorkspace: the webhook is already connected to a
	// different live agent in the SAME workspace.
	ErrBotOwnedBySameWorkspace = errors.New("wecomwebhook: this webhook is already connected to another agent in this workspace")
	// ErrBotOwnedByArchivedAgent: the webhook's owning agent is archived.
	ErrBotOwnedByArchivedAgent = errors.New("wecomwebhook: this webhook is connected to an archived agent in this workspace")
)

// testMessageTimeout bounds the proof-of-control post inside Register so the
// install request cannot hang past the HTTP caller's patience.
const testMessageTimeout = 15 * time.Second

// installTestMessage is the message posted to prove the key works. It is the
// only copy this adapter writes: the group sees it exactly once, at install
// time. Bilingual so a mixed-reading group understands it either way.
const installTestMessage = "Multica 已连接此群机器人（测试消息，可忽略）/ Multica connected to this group (test message, safe to ignore)"

// installQueries is the slice of generated queries the install flow needs,
// interface-shaped so tests inject a fake (same adapter pattern as Telegram).
type installQueries interface {
	WithTx(tx pgx.Tx) installQueries
	UpsertChannelInstallation(ctx context.Context, arg db.UpsertChannelInstallationParams) (db.ChannelInstallation, error)
	ReclaimDeadChannelInstallationByAppID(ctx context.Context, arg db.ReclaimDeadChannelInstallationByAppIDParams) (pgtype.UUID, error)
	GetChannelInstallationOwnerByAppID(ctx context.Context, arg db.GetChannelInstallationOwnerByAppIDParams) (db.GetChannelInstallationOwnerByAppIDRow, error)
	ListChannelInstallationsByWorkspace(ctx context.Context, arg db.ListChannelInstallationsByWorkspaceParams) ([]db.ChannelInstallation, error)
	GetChannelInstallationInWorkspace(ctx context.Context, arg db.GetChannelInstallationInWorkspaceParams) (db.ChannelInstallation, error)
	SetChannelInstallationStatus(ctx context.Context, arg db.SetChannelInstallationStatusParams) error
	SetChannelInstallationConfig(ctx context.Context, arg db.SetChannelInstallationConfigParams) error
}

type dbInstallQueries struct{ *db.Queries }

func (q dbInstallQueries) WithTx(tx pgx.Tx) installQueries {
	return dbInstallQueries{q.Queries.WithTx(tx)}
}

// InstallService owns the at-rest encryption of the webhook key, the
// proof-of-control test post, and the install transaction. The box MUST be
// non-nil (plaintext-at-rest storage is refused even in dev).
type InstallService struct {
	box    *secretbox.Box
	q      installQueries
	tx     engine.TxStarter
	sender *Sender
	logger *slog.Logger
}

// NewInstallService binds the service to queries, a tx starter, an encryption
// box, and the sender used for the proof-of-control post.
func NewInstallService(q *db.Queries, tx engine.TxStarter, box *secretbox.Box, sender *Sender, logger *slog.Logger) (*InstallService, error) {
	if q == nil {
		return nil, errors.New("wecomwebhook: InstallService requires queries")
	}
	return newInstallService(dbInstallQueries{q}, tx, box, sender, logger)
}

func newInstallService(q installQueries, tx engine.TxStarter, box *secretbox.Box, sender *Sender, logger *slog.Logger) (*InstallService, error) {
	if box == nil {
		return nil, errors.New("wecomwebhook: InstallService requires a non-nil secretbox.Box")
	}
	if q == nil {
		return nil, errors.New("wecomwebhook: InstallService requires queries")
	}
	if tx == nil {
		return nil, errors.New("wecomwebhook: InstallService requires a tx starter")
	}
	if sender == nil {
		return nil, errors.New("wecomwebhook: InstallService requires a sender")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &InstallService{
		box:    box,
		q:      q,
		tx:     tx,
		sender: sender,
		logger: logger,
	}, nil
}

// RegisterParams are the inputs for a webhook install: the agent this webhook
// announces for, who is installing, and the pasted key. AnnounceStatusChanges
// is the installation-level status-push toggle; nil means default ON.
type RegisterParams struct {
	WorkspaceID           pgtype.UUID
	AgentID               pgtype.UUID
	InitiatorID           pgtype.UUID
	Key                   string
	AnnounceStatusChanges *bool
}

// Register installs a user-supplied group-robot webhook for an agent: shape
// check, live proof-of-control post, encrypt the key at rest, and persist
// the installation keyed by (workspace, agent) with the key in the routing
// slot.
func (s *InstallService) Register(ctx context.Context, p RegisterParams) (db.ChannelInstallation, error) {
	key := trimKey(p.Key)
	if err := validateKey(key); err != nil {
		return db.ChannelInstallation{}, err
	}
	// Proof of control. Anything short of "WeCom accepted the message"
	// refuses without persisting, and classifies the refusal the handler can
	// render.
	testCtx, cancel := context.WithTimeout(ctx, testMessageTimeout)
	defer cancel()
	if _, err := s.sender.SendText(testCtx, key, installTestMessage); err != nil {
		var api *apiError
		if errors.As(err, &api) {
			return db.ChannelInstallation{}, fmt.Errorf("%w (errcode %d)", ErrKeyRejected, api.Code)
		}
		return db.ChannelInstallation{}, ErrCredentialsUnverifiable
	}

	sealed, err := s.box.Seal([]byte(key))
	if err != nil {
		return db.ChannelInstallation{}, fmt.Errorf("wecomwebhook: encrypt key: %w", err)
	}
	cfgJSON, err := encodeInstallConfig(key, sealed, p.AnnounceStatusChanges)
	if err != nil {
		return db.ChannelInstallation{}, err
	}
	return s.persistInstall(ctx, installPersist{
		wsID:        p.WorkspaceID,
		agentID:     p.AgentID,
		installerID: p.InitiatorID,
		appIDKey:    key,
		configJSON:  cfgJSON,
	})
}

// trimKey strips the whitespace a paste so often carries, including the
// full-width characters CJK IMEs insert.
func trimKey(key string) string {
	out := make([]rune, 0, len(key))
	for _, r := range key {
		switch r {
		case ' ', '\t', '\n', '\r', '　':
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

// validateKey does the cheapest honest check: a WeCom webhook key is a
// URL-path token of printable ASCII with no whitespace. Everything stricter
// (exact length, charset) would break the day WeCom widens the format, and
// the real verification is the live test post anyway.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty", ErrInvalidKey)
	}
	if len(key) < 16 || len(key) > 128 {
		return fmt.Errorf("%w: wrong length", ErrInvalidKey)
	}
	for _, r := range key {
		if r < '!' || r > '~' {
			return fmt.Errorf("%w: non-printable or non-ASCII character", ErrInvalidKey)
		}
	}
	return nil
}

// installPersist carries the resolved fields persistInstall writes.
type installPersist struct {
	wsID        pgtype.UUID
	agentID     pgtype.UUID
	installerID pgtype.UUID
	appIDKey    string
	configJSON  []byte
}

const pgUniqueViolation = "23505"

// persistInstall upserts the installation keyed by (workspace_id, agent_id,
// channel_type): ONE webhook per agent. A unique violation on the
// (channel_type, app_id) routing index means the pasted webhook is already
// connected to a different live agent or workspace — refuse rather than
// steal, with the accurate conflict message (same policy as Telegram/Slack).
func (s *InstallService) persistInstall(ctx context.Context, p installPersist) (db.ChannelInstallation, error) {
	tx, err := s.tx.Begin(ctx)
	if err != nil {
		return db.ChannelInstallation{}, fmt.Errorf("begin install tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	// Free the routing slot from any DEAD prior owner before the upsert.
	if _, err := qtx.ReclaimDeadChannelInstallationByAppID(ctx, db.ReclaimDeadChannelInstallationByAppIDParams{
		ChannelType: channelTypeWecomWebhook,
		AppID:       p.appIDKey,
		WorkspaceID: p.wsID,
		AgentID:     p.agentID,
	}); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return db.ChannelInstallation{}, fmt.Errorf("reclaim dead wecomwebhook installation: %w", err)
	}

	inst, err := qtx.UpsertChannelInstallation(ctx, db.UpsertChannelInstallationParams{
		WorkspaceID:     p.wsID,
		AgentID:         p.agentID,
		ChannelType:     channelTypeWecomWebhook,
		Config:          p.configJSON,
		InstallerUserID: p.installerID,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return db.ChannelInstallation{}, s.liveOwnerConflictErr(ctx, p.wsID, p.appIDKey)
		}
		return db.ChannelInstallation{}, fmt.Errorf("upsert wecomwebhook installation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return db.ChannelInstallation{}, fmt.Errorf("commit wecomwebhook install: %w", err)
	}
	return inst, nil
}

// liveOwnerConflictErr classifies who holds the routing slot so the handler
// renders an accurate message.
func (s *InstallService) liveOwnerConflictErr(ctx context.Context, requestingWorkspaceID pgtype.UUID, appID string) error {
	owner, err := s.q.GetChannelInstallationOwnerByAppID(ctx, db.GetChannelInstallationOwnerByAppIDParams{
		ChannelType: channelTypeWecomWebhook,
		AppID:       appID,
	})
	if err != nil {
		return ErrBotOwnedByAnotherWorkspace
	}
	switch {
	case owner.WorkspaceID != requestingWorkspaceID:
		return ErrBotOwnedByAnotherWorkspace
	case owner.AgentArchivedAt.Valid:
		return ErrBotOwnedByArchivedAgent
	default:
		return ErrBotOwnedBySameWorkspace
	}
}

// ListByWorkspace returns every wecomwebhook installation in the workspace,
// for the management surface.
func (s *InstallService) ListByWorkspace(ctx context.Context, wsID pgtype.UUID) ([]db.ChannelInstallation, error) {
	return s.q.ListChannelInstallationsByWorkspace(ctx, db.ListChannelInstallationsByWorkspaceParams{
		WorkspaceID: wsID,
		ChannelType: channelTypeWecomWebhook,
	})
}

// GetInWorkspace is the workspace-scoped lookup so a forged installation id
// from another workspace returns NotFound instead of leaking existence.
func (s *InstallService) GetInWorkspace(ctx context.Context, id, wsID pgtype.UUID) (db.ChannelInstallation, error) {
	inst, err := s.q.GetChannelInstallationInWorkspace(ctx, db.GetChannelInstallationInWorkspaceParams{
		ID:          id,
		WorkspaceID: wsID,
		ChannelType: channelTypeWecomWebhook,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.ChannelInstallation{}, ErrInstallationNotFound
		}
		return db.ChannelInstallation{}, err
	}
	return inst, nil
}

// Revoke flips status to 'revoked'. The row is preserved for audit; the
// outbound subscriber skips non-active installations, so announcements stop.
func (s *InstallService) Revoke(ctx context.Context, id pgtype.UUID) error {
	return s.q.SetChannelInstallationStatus(ctx, db.SetChannelInstallationStatusParams{
		ID:     id,
		Status: string(InstallationRevoked),
	})
}

// SetAnnounceStatusChanges flips the installation's status-push toggle
// without re-verifying the key — the key was proven at install time, and a
// preference flip is not a credential change. Config is read, patched, and
// rewritten whole (the blob carries the sealed key; no field-level JSONB
// patch touches it).
func (s *InstallService) SetAnnounceStatusChanges(ctx context.Context, id, workspaceID pgtype.UUID, enabled bool) (db.ChannelInstallation, error) {
	row, err := s.GetInWorkspace(ctx, id, workspaceID)
	if err != nil {
		return db.ChannelInstallation{}, err
	}
	var cfg installConfig
	if len(row.Config) > 0 {
		if err := json.Unmarshal(row.Config, &cfg); err != nil {
			return db.ChannelInstallation{}, fmt.Errorf("wecomwebhook: decode installation config: %w", err)
		}
	}
	cfg.AnnounceStatusChanges = &enabled
	updated, err := json.Marshal(cfg)
	if err != nil {
		return db.ChannelInstallation{}, fmt.Errorf("wecomwebhook: encode installation config: %w", err)
	}
	if err := s.q.SetChannelInstallationConfig(ctx, db.SetChannelInstallationConfigParams{
		ID:     row.ID,
		Config: updated,
	}); err != nil {
		return db.ChannelInstallation{}, fmt.Errorf("wecomwebhook: set announce toggle: %w", err)
	}
	row.Config = updated
	return row, nil
}
