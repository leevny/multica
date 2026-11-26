// @vitest-environment jsdom

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { I18nProvider } from "@multica/core/i18n/react";
import type { ChatSession } from "@multica/core/types";
import enChat from "../../locales/en/chat.json";
import enCommon from "../../locales/en/common.json";

// wecomwebhook-push-dialog.test.tsx — the push-to-WeCom-group binding entry.
//
// What matters: the dialog lists ACTIVE installations only, binds the session
// to the picked one, offers the stop action when the session is already bound
// to this channel, and refuses to rebind over a foreign channel's binding
// (the server refuses; the dialog surfaces it as an error toast).

const mockBind = vi.hoisted(() => vi.fn());
const mockUnbind = vi.hoisted(() => vi.fn());
const mockList = vi.hoisted(() => vi.fn());

vi.mock("@multica/core/api", () => ({
  api: {
    listWecomWebhookInstallations: mockList,
    bindWecomWebhookChatSession: mockBind,
    unbindWecomWebhookChatSession: mockUnbind,
  },
  errorCode: () => undefined,
}));

vi.mock("@multica/core/hooks", () => ({
  useWorkspaceId: () => "ws-1",
}));

vi.mock("@multica/core/chat/queries", () => ({
  chatKeys: { all: (wsId: string) => ["chat", wsId] },
}));

vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), message: vi.fn() },
}));

import { WecomWebhookPushDialog } from "./wecomwebhook-push-dialog";

const TEST_RESOURCES = { en: { chat: enChat, common: enCommon } };

function session(overrides: Partial<ChatSession> = {}): ChatSession {
  return {
    id: "session-1",
    workspace_id: "ws-1",
    agent_id: "agent-1",
    creator_id: "user-1",
    title: "Ops chat",
    status: "active",
    has_unread: false,
    unread_count: 0,
    last_message: null,
    pinned: false,
    created_at: new Date(0).toISOString(),
    updated_at: new Date(0).toISOString(),
    ...overrides,
  };
}

const installation = (id: string, suffix: string, status = "active") => ({
  id,
  workspace_id: "ws-1",
  agent_id: "agent-1",
  key_suffix: suffix,
  installer_user_id: "user-1",
  status,
  installed_at: "",
  created_at: "",
  updated_at: "",
});

function show(ui: React.ReactElement) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return render(
    <QueryClientProvider client={qc}>
      <I18nProvider locale="en" resources={TEST_RESOURCES}>
        {ui}
      </I18nProvider>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  mockList.mockResolvedValue({
    installations: [installation("inst-1", "aaaa"), installation("inst-2", "bbbb", "revoked")],
    configured: true,
    install_supported: true,
  });
});

afterEach(cleanup);

async function openDialog(sessionFixture: ChatSession) {
  show(
    <WecomWebhookPushDialog
      session={sessionFixture}
      open
      onOpenChange={vi.fn()}
    />
  );
  await screen.findByText(/Group robot ····aaaa/);
}

describe("WecomWebhookPushDialog", () => {
  it("lists only active installations and binds the picked one", async () => {
    await openDialog(session());
    expect(screen.queryByText(/···bbbb/)).toBeNull();
    await act(async () => {
      fireEvent.click(screen.getByTestId("wecomwebhook-push-option-inst-1"));
    });
    await waitFor(() =>
      expect(mockBind).toHaveBeenCalledWith("ws-1", {
        chat_session_id: "session-1",
        installation_id: "inst-1",
      }),
    );
    expect(mockUnbind).not.toHaveBeenCalled();
  });

  it("offers stop pushing when the session is already bound to this channel", async () => {
    await openDialog(
      session({
        channel_source: {
          channel_type: "wecomwebhook",
          installation_id: "inst-1",
          route_revision: 1,
        },
        is_current_channel_route: true,
      }),
    );
    await act(async () => {
      fireEvent.click(screen.getByTestId("wecomwebhook-push-option-inst-1"));
    });
    await waitFor(() => expect(mockUnbind).toHaveBeenCalledWith("ws-1", "session-1"));
    expect(mockBind).not.toHaveBeenCalled();
  });

  it("binds to a different installation when re-pointing within the channel", async () => {
    mockList.mockResolvedValue({
      installations: [installation("inst-1", "aaaa"), installation("inst-2", "dddd")],
      configured: true,
      install_supported: true,
    });
    await openDialog(
      session({
        channel_source: {
          channel_type: "wecomwebhook",
          installation_id: "inst-1",
          route_revision: 1,
        },
        is_current_channel_route: true,
      }),
    );
    await act(async () => {
      fireEvent.click(screen.getByTestId("wecomwebhook-push-option-inst-2"));
    });
    await waitFor(() =>
      expect(mockBind).toHaveBeenCalledWith("ws-1", {
        chat_session_id: "session-1",
        installation_id: "inst-2",
      }),
    );
  });

  it("shows the empty state when nothing is configured", async () => {
    mockList.mockResolvedValue({ installations: [], configured: false });
    show(
      <WecomWebhookPushDialog session={session()} open onOpenChange={vi.fn()} />,
    );
    expect(await screen.findByText(/No group robots yet/)).toBeInTheDocument();
    expect(mockBind).not.toHaveBeenCalled();
  });
});
