package wecomwebhook

// binding.go — the explicit chat_session → installation binding. Inbound
// channels create bindings as a side effect of a first message; a webhook
// receives nothing, so a session reaches the group only through this
// deliberate act from the web UI ("推送到企微群").
//
// channel_chat_session_binding carries UNIQUE(chat_session_id): a session
// has at most one binding. Binding therefore either claims an unbound
// session, re-points a session already bound to THIS channel (retire +
// create — the announce destination moves with the newest row), or refuses a
// session bound to a foreign channel: retiring a wecom/lark binding here
// would silently disconnect that platform's reply path, and that is a
// decision for the platform's own UI, not a webhook push.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

var (
	// ErrSessionNotFound: unknown session or one outside the workspace.
	ErrSessionNotFound = errors.New("wecomwebhook: chat session not found")
	// ErrSessionBoundToOtherChannel: the session's one binding slot belongs
	// to another channel's reply path.
	ErrSessionBoundToOtherChannel = errors.New("wecomwebhook: this chat is bound to another channel; disconnect it there before pushing it to a WeCom group")
	// ErrBindingNotFound: no wecomwebhook binding to remove.
	ErrBindingNotFound = errors.New("wecomwebhook: chat session is not pushed to a WeCom group")
)

// bindingQueries is the slice of generated queries the bind/unbind flow
// needs, interface-shaped for tests.
type bindingQueries interface {
	WithTx(tx pgx.Tx) bindingQueries
	GetChatSessionInWorkspace(ctx context.Context, arg db.GetChatSessionInWorkspaceParams) (db.ChatSession, error)
	GetChannelChatSessionBindingBySessionAny(ctx context.Context, chatSessionID pgtype.UUID) (db.ChannelChatSessionBinding, error)
	RetireChannelChatSessionBinding(ctx context.Context, arg db.RetireChannelChatSessionBindingParams) (db.ChannelChatSessionBinding, error)
	CreateChannelChatSessionBindingForExistingSession(ctx context.Context, arg db.CreateChannelChatSessionBindingForExistingSessionParams) (db.CreateChannelChatSessionBindingForExistingSessionRow, error)
}

type dbBindingQueries struct{ *db.Queries }

func (q dbBindingQueries) WithTx(tx pgx.Tx) bindingQueries {
	return dbBindingQueries{q.Queries.WithTx(tx)}
}

// BindingService writes the one binding a session may carry.
type BindingService struct {
	q         bindingQueries
	tx        engine.TxStarter
	installer *InstallService
	logger    *slog.Logger
}

// NewBindingService binds the service to queries, a tx starter, and the
// install service (whose GetInWorkspace scopes the target installation).
func NewBindingService(q *db.Queries, tx engine.TxStarter, installer *InstallService, logger *slog.Logger) *BindingService {
	if logger == nil {
		logger = slog.Default()
	}
	return &BindingService{
		q:         dbBindingQueries{q},
		tx:        tx,
		installer: installer,
		logger:    logger,
	}
}

// BindInput is the trusted input the handler has already authorized and
// parsed.
type BindInput struct {
	WorkspaceID    pgtype.UUID
	ChatSessionID  pgtype.UUID
	InstallationID pgtype.UUID
}

// BindResult echoes the identifiers of the binding that now routes the
// session's announcements.
type BindResult struct {
	ChatSessionID  pgtype.UUID
	InstallationID pgtype.UUID
}

// Bind points the session's terminal announcements at the installation's
// group. Idempotent: binding an already-bound session to the installation it
// is already bound to succeeds without writing.
func (s *BindingService) Bind(ctx context.Context, in BindInput) (BindResult, error) {
	// The target webhook must be this workspace's and live: pushing to a
	// revoked webhook would announce into a group whose admin already pulled
	// the plug.
	row, err := s.installer.GetInWorkspace(ctx, in.InstallationID, in.WorkspaceID)
	if err != nil {
		if errors.Is(err, ErrInstallationNotFound) {
			return BindResult{}, ErrInstallationNotFound
		}
		return BindResult{}, fmt.Errorf("wecomwebhook: load installation: %w", err)
	}
	if InstallationStatus(row.Status) != InstallationActive {
		return BindResult{}, ErrInstallationNotFound
	}
	if _, err := s.q.GetChatSessionInWorkspace(ctx, db.GetChatSessionInWorkspaceParams{
		ID:          in.ChatSessionID,
		WorkspaceID: in.WorkspaceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return BindResult{}, ErrSessionNotFound
		}
		return BindResult{}, fmt.Errorf("wecomwebhook: load chat session: %w", err)
	}

	tx, err := s.tx.Begin(ctx)
	if err != nil {
		return BindResult{}, fmt.Errorf("wecomwebhook: begin bind tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	existing, err := qtx.GetChannelChatSessionBindingBySessionAny(ctx, in.ChatSessionID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Unbound session: straight through.
	case err != nil:
		return BindResult{}, fmt.Errorf("wecomwebhook: read current binding: %w", err)
	case existing.ChannelType != channelTypeWecomWebhook:
		return BindResult{}, ErrSessionBoundToOtherChannel
	case existing.InstallationID == in.InstallationID && !existing.RetiredAt.Valid:
		// Already bound here. A retired row for the same installation would
		// be a re-bind, not a no-op.
		return BindResult{ChatSessionID: existing.ChatSessionID, InstallationID: existing.InstallationID}, nil
	default:
		// Re-point within this channel. Retire inside the same tx so a
		// failure leaves the old destination intact.
		if _, err := qtx.RetireChannelChatSessionBinding(ctx, db.RetireChannelChatSessionBindingParams{
			ID: existing.ID,
		}); err != nil {
			return BindResult{}, fmt.Errorf("wecomwebhook: retire previous binding: %w", err)
		}
	}

	// channel_chat_id is this adapter's session-isolation key under
	// UNIQUE(installation_id, channel_chat_id). The webhook's delivery
	// address is the installation's key — the group cannot be addressed
	// further — so the key carries nothing but uniqueness: the session's own
	// id does that, and lets several sessions share one webhook.
	binding, err := qtx.CreateChannelChatSessionBindingForExistingSession(ctx, db.CreateChannelChatSessionBindingForExistingSessionParams{
		ChatSessionID:  in.ChatSessionID,
		InstallationID: in.InstallationID,
		ChannelType:    channelTypeWecomWebhook,
		ChannelChatID:  fmt.Sprintf("webhook:%s", in.ChatSessionID.String()),
		ChatType:       string(channel.ChatTypeGroup),
		Config:         []byte("{}"),
	})
	if err != nil {
		return BindResult{}, fmt.Errorf("wecomwebhook: create binding: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return BindResult{}, fmt.Errorf("wecomwebhook: commit bind: %w", err)
	}
	s.logger.InfoContext(ctx, "wecomwebhook: session bound to group webhook",
		"chat_session_id", in.ChatSessionID.String(),
		"installation_id", in.InstallationID.String())
	return BindResult{ChatSessionID: binding.ChatSessionID, InstallationID: binding.InstallationID}, nil
}

// Unbind retires the session's wecomwebhook binding so terminal outcomes
// stop reaching the group. Idempotent on an unbound session.
func (s *BindingService) Unbind(ctx context.Context, workspaceID, chatSessionID pgtype.UUID) error {
	if _, err := s.q.GetChatSessionInWorkspace(ctx, db.GetChatSessionInWorkspaceParams{
		ID:          chatSessionID,
		WorkspaceID: workspaceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSessionNotFound
		}
		return fmt.Errorf("wecomwebhook: load chat session: %w", err)
	}
	existing, err := s.q.GetChannelChatSessionBindingBySessionAny(ctx, chatSessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("wecomwebhook: read current binding: %w", err)
	}
	if existing.ChannelType != channelTypeWecomWebhook {
		return ErrBindingNotFound
	}
	if existing.RetiredAt.Valid {
		return nil
	}
	if _, err := s.q.RetireChannelChatSessionBinding(ctx, db.RetireChannelChatSessionBindingParams{
		ID: existing.ID,
	}); err != nil {
		return fmt.Errorf("wecomwebhook: retire binding: %w", err)
	}
	return nil
}
