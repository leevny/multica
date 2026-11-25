package handler

// wecomwebhook.go — the HTTP surface for the WeCom group-robot webhook
// channel: BYO install (paste a key; the service really posts a test message
// before persisting), list, revoke, and the explicit chat-session binding
// the announce path resolves. Mirrors telegram.go's BYO shape; the binding
// endpoints are the only piece with no per-channel precedent, because every
// other channel's bindings are created by its inbound flow.

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/wecomwebhook"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// WecomWebhookInstallationResponse is the wire shape for a wecomwebhook
// installation row. The sealed key in config is INTENTIONALLY absent, and so
// is the plaintext key sitting in config->>'app_id' — members can list
// installations, and the key is the bearer credential for the group. Only
// the last-4 suffix crosses the boundary.
type WecomWebhookInstallationResponse struct {
	ID              string `json:"id"`
	WorkspaceID     string `json:"workspace_id"`
	AgentID         string `json:"agent_id"`
	KeySuffix       string `json:"key_suffix"`
	InstallerUserID string `json:"installer_user_id"`
	Status          string `json:"status"`
	InstalledAt     string `json:"installed_at"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

func wecomWebhookInstallationToResponse(row db.ChannelInstallation, suffix string) WecomWebhookInstallationResponse {
	return WecomWebhookInstallationResponse{
		ID:              uuidToString(row.ID),
		WorkspaceID:     uuidToString(row.WorkspaceID),
		AgentID:         uuidToString(row.AgentID),
		KeySuffix:       suffix,
		InstallerUserID: uuidToString(row.InstallerUserID),
		Status:          row.Status,
		InstalledAt:     row.InstalledAt.Time.UTC().Format(time.RFC3339),
		CreatedAt:       row.CreatedAt.Time.UTC().Format(time.RFC3339),
		UpdatedAt:       row.UpdatedAt.Time.UTC().Format(time.RFC3339),
	}
}

// ListWecomWebhookInstallations (GET /api/workspaces/{id}/wecomwebhook/installations)
// is member-visible so the Integrations tab renders for non-admins. Response
// flags mirror Telegram: configured = at-rest key set; install_supported is
// true whenever configured (a pasted webhook key needs no hosted credential).
func (h *Handler) ListWecomWebhookInstallations(w http.ResponseWriter, r *http.Request) {
	if h.WecomWebhookInstall == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"installations":     []WecomWebhookInstallationResponse{},
			"configured":        false,
			"install_supported": false,
		})
		return
	}
	wsUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace id")
	if !ok {
		return
	}
	rows, err := h.WecomWebhookInstall.ListByWorkspace(r.Context(), wsUUID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list wecomwebhook installations")
		return
	}
	out := make([]WecomWebhookInstallationResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, wecomWebhookInstallationToResponse(row, wecomWebhookKeySuffix(row.Config)))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"installations":     out,
		"configured":        true,
		"install_supported": true,
	})
}

// RegisterWecomWebhookRequest is the body for a webhook install: the key the
// admin pasted from the WeCom group settings.
type RegisterWecomWebhookRequest struct {
	Key string `json:"key"`
}

// RegisterWecomWebhook (POST /api/workspaces/{id}/wecomwebhook/install?agent_id=…)
// installs a user-supplied group-robot webhook for an agent. Admin-only at
// the router. The live test post inside Register is both the key's only
// possible verification and the "test" affordance the admin watches land in
// their group.
func (h *Handler) RegisterWecomWebhook(w http.ResponseWriter, r *http.Request) {
	if h.WecomWebhookInstall == nil {
		writeFeatureDisabled(w, "wecomwebhook_not_configured", "wecomwebhook integration not enabled")
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	wsUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace id")
	if !ok {
		return
	}
	agentIDStr := r.URL.Query().Get("agent_id")
	agentUUID, ok := parseUUIDOrBadRequest(w, agentIDStr, "agent_id")
	if !ok {
		return
	}
	// Ownership pre-check at the boundary so a wrong agent_id is a clear 404.
	if _, err := h.Queries.GetAgentInWorkspace(r.Context(), db.GetAgentInWorkspaceParams{
		ID:          agentUUID,
		WorkspaceID: wsUUID,
	}); err != nil {
		writeError(w, http.StatusNotFound, "agent not found in this workspace")
		return
	}
	initiatorUUID, ok := parseUUIDOrBadRequest(w, userID, "user id")
	if !ok {
		return
	}
	var body RegisterWecomWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	row, err := h.WecomWebhookInstall.Register(r.Context(), wecomwebhook.RegisterParams{
		WorkspaceID: wsUUID,
		AgentID:     agentUUID,
		InitiatorID: initiatorUUID,
		Key:         body.Key,
	})
	if err != nil {
		switch {
		case errors.Is(err, wecomwebhook.ErrInvalidKey):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, wecomwebhook.ErrKeyRejected):
			writeError(w, http.StatusBadRequest, "WeCom rejected this webhook key — check the key and that the group robot still exists, then try again")
		case errors.Is(err, wecomwebhook.ErrCredentialsUnverifiable):
			writeError(w, http.StatusServiceUnavailable, "could not reach WeCom to verify this webhook key — check the server network or proxy and try again; the key was not saved")
		case errors.Is(err, wecomwebhook.ErrBotOwnedBySameWorkspace):
			writeError(w, http.StatusConflict, "this webhook is already connected to another agent in this workspace — disconnect it there first, then connect it here")
		case errors.Is(err, wecomwebhook.ErrBotOwnedByArchivedAgent):
			writeError(w, http.StatusConflict, "this webhook is connected to an archived agent in this workspace — restore that agent, or disconnect its webhook, before connecting it here")
		case errors.Is(err, wecomwebhook.ErrBotOwnedByAnotherWorkspace):
			writeError(w, http.StatusConflict, "this webhook is already connected to a different Multica workspace — disconnect it there before connecting it here")
		default:
			writeError(w, http.StatusInternalServerError, "could not save this webhook — something went wrong on the server; the key was not saved")
		}
		return
	}
	// Broadcast so every open client invalidates its installations query and
	// shows the new webhook — matching the Telegram install semantics.
	h.publish(protocol.EventWecomWebhookInstallationCreated, uuidToString(row.WorkspaceID), "user", userID, map[string]any{
		"id": uuidToString(row.ID),
	})
	writeJSON(w, http.StatusOK, wecomWebhookInstallationToResponse(row, wecomWebhookKeySuffix(row.Config)))
}

// RevokeWecomWebhookInstallation (DELETE /api/workspaces/{id}/wecomwebhook/installations/{installationId})
// flips status to 'revoked'. Admin-only at the router. The row is preserved
// for audit; the announce path skips non-active installations, and a
// re-install (re-pasting the key) flips status back to 'active'.
func (h *Handler) RevokeWecomWebhookInstallation(w http.ResponseWriter, r *http.Request) {
	if h.WecomWebhookInstall == nil {
		writeFeatureDisabled(w, "wecomwebhook_not_configured", "wecomwebhook integration not configured")
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	wsUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace id")
	if !ok {
		return
	}
	instUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "installationId"), "installation id")
	if !ok {
		return
	}
	// Workspace-scoped lookup so one workspace cannot revoke another's
	// installation by guessing the UUID.
	if _, err := h.WecomWebhookInstall.GetInWorkspace(r.Context(), instUUID, wsUUID); err != nil {
		if errors.Is(err, wecomwebhook.ErrInstallationNotFound) {
			writeError(w, http.StatusNotFound, "wecomwebhook installation not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load installation")
		return
	}
	if err := h.WecomWebhookInstall.Revoke(r.Context(), instUUID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to revoke installation")
		return
	}
	h.publish(protocol.EventWecomWebhookInstallationRevoked, uuidToString(wsUUID), "user", userID, map[string]any{
		"id": uuidToString(instUUID),
	})
	w.WriteHeader(http.StatusNoContent)
}

// BindWecomWebhookChatSessionRequest names the session to push and the
// webhook to push it to.
type BindWecomWebhookChatSessionRequest struct {
	ChatSessionID  string `json:"chat_session_id"`
	InstallationID string `json:"installation_id"`
}

// BindWecomWebhookChatSession (POST /api/workspaces/{id}/wecomwebhook/bindings)
// points one chat session's terminal announcements at one webhook
// installation. The session creator or a workspace admin may bind: a chat
// session is creator-private, and a webhook pushes into a group on behalf
// of the agent, so neither a stranger nor a mere member decides where one
// lands.
func (h *Handler) BindWecomWebhookChatSession(w http.ResponseWriter, r *http.Request) {
	if h.WecomWebhookBindings == nil {
		writeFeatureDisabled(w, "wecomwebhook_not_configured", "wecomwebhook integration not enabled")
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	wsUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace id")
	if !ok {
		return
	}
	var body BindWecomWebhookChatSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	sessionUUID, ok := parseUUIDOrBadRequest(w, body.ChatSessionID, "chat_session_id")
	if !ok {
		return
	}
	instUUID, ok := parseUUIDOrBadRequest(w, body.InstallationID, "installation_id")
	if !ok {
		return
	}
	if !h.canBindWecomWebhookSession(w, r, wsUUID, sessionUUID, userID) {
		return
	}
	binding, err := h.WecomWebhookBindings.Bind(r.Context(), wecomwebhook.BindInput{
		WorkspaceID:    wsUUID,
		ChatSessionID:  sessionUUID,
		InstallationID: instUUID,
	})
	if err != nil {
		switch {
		case errors.Is(err, wecomwebhook.ErrSessionNotFound):
			writeError(w, http.StatusNotFound, "chat session not found in this workspace")
		case errors.Is(err, wecomwebhook.ErrInstallationNotFound):
			writeError(w, http.StatusNotFound, "wecomwebhook installation not found")
		case errors.Is(err, wecomwebhook.ErrSessionBoundToOtherChannel):
			writeError(w, http.StatusConflict, "this chat is bound to another channel; disconnect it there before pushing it to a WeCom group")
		default:
			writeError(w, http.StatusInternalServerError, "failed to bind chat session")
		}
		return
	}
	h.publish(protocol.EventWecomWebhookBindingChanged, uuidToString(wsUUID), "user", userID, map[string]any{
		"chat_session_id": uuidToString(binding.ChatSessionID),
		"installation_id": uuidToString(binding.InstallationID),
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"chat_session_id": uuidToString(binding.ChatSessionID),
		"installation_id": uuidToString(binding.InstallationID),
	})
}

// canBindWecomWebhookSession admits the session creator or a workspace
// admin. The route middleware already proved workspace membership; this is
// the per-session half it cannot make.
func (h *Handler) canBindWecomWebhookSession(w http.ResponseWriter, r *http.Request, wsUUID pgtype.UUID, sessionUUID pgtype.UUID, userID string) bool {
	member, ok := h.requireWorkspaceRole(w, r, uuidToString(wsUUID), "workspace not found", "owner", "admin", "member")
	if !ok {
		return false
	}
	if roleAllowed(member.Role, "owner", "admin") {
		return true
	}
	session, err := h.Queries.GetChatSessionInWorkspace(r.Context(), db.GetChatSessionInWorkspaceParams{
		ID:          sessionUUID,
		WorkspaceID: wsUUID,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "chat session not found in this workspace")
		return false
	}
	if uuidToString(session.CreatorID) != userID {
		writeError(w, http.StatusForbidden, "only the chat's creator or a workspace admin can change where it is pushed")
		return false
	}
	return true
}

// UnbindWecomWebhookChatSession (DELETE /api/workspaces/{id}/wecomwebhook/bindings/{chatSessionId})
// retires the session's wecomwebhook binding; terminal outcomes stop
// reaching the group. Idempotent on an unbound session. Same
// creator-or-admin admission as bind.
func (h *Handler) UnbindWecomWebhookChatSession(w http.ResponseWriter, r *http.Request) {
	if h.WecomWebhookBindings == nil {
		writeFeatureDisabled(w, "wecomwebhook_not_configured", "wecomwebhook integration not configured")
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	wsUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace id")
	if !ok {
		return
	}
	sessionUUID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "chatSessionId"), "chat session id")
	if !ok {
		return
	}
	if !h.canBindWecomWebhookSession(w, r, wsUUID, sessionUUID, userID) {
		return
	}
	if err := h.WecomWebhookBindings.Unbind(r.Context(), wsUUID, sessionUUID); err != nil {
		switch {
		case errors.Is(err, wecomwebhook.ErrSessionNotFound):
			writeError(w, http.StatusNotFound, "chat session not found in this workspace")
		case errors.Is(err, wecomwebhook.ErrBindingNotFound):
			writeError(w, http.StatusNotFound, "chat session is not pushed to a WeCom group")
		default:
			writeError(w, http.StatusInternalServerError, "failed to unbind chat session")
		}
		return
	}
	h.publish(protocol.EventWecomWebhookBindingChanged, uuidToString(wsUUID), "user", userID, map[string]any{
		"chat_session_id": uuidToString(sessionUUID),
	})
	w.WriteHeader(http.StatusNoContent)
}

// wecomWebhookKeySuffix renders the masked key suffix for a response from a
// row's raw config. A malformed config yields the "…" placeholder rather
// than an error — a display suffix is never worth failing a list call.
func wecomWebhookKeySuffix(config []byte) string {
	inst, err := wecomwebhook.InstallationKeySuffixFromConfig(config)
	if err != nil {
		return "…"
	}
	return inst
}
