/**
 * A WeCom group-robot webhook ("群机器人") installation bound to a single
 * Multica agent. Wire shape mirrors `WecomWebhookInstallationResponse` in
 * `server/internal/handler/wecomwebhook.go`. Any new field the backend adds
 * MUST default to optional so older desktop builds keep parsing the response
 * — see CLAUDE.md → API Compatibility.
 *
 * The webhook key is the bearer credential for its group, so the API never
 * returns it — only the last-4 suffix for telling two webhooks apart.
 */
export interface WecomWebhookInstallation {
  id: string;
  workspace_id: string;
  agent_id: string;
  /** Last 4 characters of the webhook key. */
  key_suffix: string;
  installer_user_id: string;
  status: "active" | "revoked" | string;
  installed_at: string;
  created_at: string;
  updated_at: string;
}

export interface ListWecomWebhookInstallationsResponse {
  installations: WecomWebhookInstallation[];
  /** Whether MULTICA_WECOM_WEBHOOK_SECRET_KEY is set on this deployment. When
   * false the connect entry points are hidden and the panel renders an "ask
   * the operator to enable WeCom group robots" state. */
  configured: boolean;
  /** Whether the install path is available (true whenever configured — a
   * pasted webhook key needs no hosted credential). Optional so an older
   * desktop build that predates it treats it as off. */
  install_supported?: boolean;
}

/** Request body for a webhook install: the key pasted from the WeCom group
 * settings. The backend really posts a test message with it before
 * persisting — the group sees that message when the install succeeds. */
export interface RegisterWecomWebhookRequest {
  key: string;
}

/** Request body for pointing one chat session's terminal announcements at
 * one webhook installation. */
export interface BindWecomWebhookChatSessionRequest {
  chat_session_id: string;
  installation_id: string;
}

/** Echo of the binding the bind call wrote. */
export interface WecomWebhookChatSessionBinding {
  chat_session_id: string;
  installation_id: string;
}
