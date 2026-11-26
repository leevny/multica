// @vitest-environment jsdom

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { I18nProvider } from "@multica/core/i18n/react";
import enCommon from "../../locales/en/common.json";
import enSettings from "../../locales/en/settings.json";

// wecomwebhook-channel-authorization.test.tsx — the paste-a-key install form.
//
// What matters: the key reaches the API trimmed, a success invalidates the
// installations query and closes, and a WeCom refusal surfaces the server's
// sentence (the handler's error text is the operator-facing copy) instead of
// a generic failure.

const mockRegister = vi.hoisted(() => vi.fn());
const mockInvalidate = vi.hoisted(() => vi.fn());
const mockToastSuccess = vi.hoisted(() => vi.fn());
const onClose = vi.fn();

vi.mock("@tanstack/react-query", () => ({
  useQueryClient: () => ({ invalidateQueries: mockInvalidate }),
}));

vi.mock("@multica/core/wecomwebhook", () => ({
  wecomWebhookKeys: {
    installations: (wsId: string) => ["wecomwebhook", "installations", wsId],
  },
}));

vi.mock("@multica/core/api", () => ({
  api: { registerWecomWebhook: mockRegister },
  errorCode: () => undefined,
}));

vi.mock("sonner", () => ({
  toast: { success: mockToastSuccess, error: vi.fn(), message: vi.fn() },
}));

import { WecomWebhookChannelAuthorization } from "./wecomwebhook-channel-authorization";

const TEST_RESOURCES = { en: { common: enCommon, settings: enSettings } };

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

function renderForm() {
  render(
    <I18nProvider locale="en" resources={TEST_RESOURCES}>
      <WecomWebhookChannelAuthorization
        wsId="workspace-1"
        agentId="agent-1"
        onClose={onClose}
      />
    </I18nProvider>,
  );
}

beforeEach(() => {
  mockRegister.mockResolvedValue({
    id: "inst-1",
    status: "active",
    key_suffix: "1234",
  });
});

async function submitKey(key: string) {
  const input = screen.getByTestId("wecomwebhook-key-input");
  await act(async () => {
    fireEvent.change(input, { target: { value: key } });
  });
  const submit = screen.getByRole("button", { name: "Connect" });
  await act(async () => {
    fireEvent.click(submit);
  });
}

describe("WecomWebhookChannelAuthorization", () => {
  it("posts the trimmed key and closes after a successful install", async () => {
    renderForm();
    await submitKey("  0123456789abcdef  ");
    await waitFor(() => expect(mockRegister).toHaveBeenCalled());
    expect(mockRegister).toHaveBeenCalledWith("workspace-1", "agent-1", {
      key: "0123456789abcdef",
      // Default ON: installed to watch the workspace's work.
      announce_status_changes: true,
    });
    await waitFor(() =>
      expect(mockInvalidate).toHaveBeenCalledWith({
        queryKey: ["wecomwebhook", "installations", "workspace-1"],
      }),
    );
    expect(mockToastSuccess).toHaveBeenCalled();
    await waitFor(() => expect(onClose).toHaveBeenCalled());
  });

  it("disables the submit button while the install is in flight", async () => {
    mockRegister.mockReturnValue(new Promise(() => {}));
    renderForm();
    const submit = screen.getByRole("button", { name: "Connect" });
    expect(submit).toBeDisabled();
    await act(async () => {
      fireEvent.change(screen.getByTestId("wecomwebhook-key-input"), {
        target: { value: "0123456789abcdef" },
      });
    });
    expect(submit).not.toBeDisabled();
    await act(async () => {
      fireEvent.click(submit);
    });
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Connecting..." })).toBeDisabled(),
    );
    expect(onClose).not.toHaveBeenCalled();
  });

  it("sends the toggle's off state when switched off", async () => {
    renderForm();
    await act(async () => {
      fireEvent.click(screen.getByRole("switch", { name: "Push issue status changes" }));
    });
    await submitKey("0123456789abcdef");
    await waitFor(() => expect(mockRegister).toHaveBeenCalled());
    expect(mockRegister).toHaveBeenCalledWith("workspace-1", "agent-1", {
      key: "0123456789abcdef",
      announce_status_changes: false,
    });
  });

  it("surfaces the server's refusal sentence", async () => {
    mockRegister.mockRejectedValue(
      new Error("WeCom rejected this webhook key — check the key and that the group robot still exists"),
    );
    renderForm();
    await submitKey("0123456789abcdef");
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toContain(
        "WeCom rejected this webhook key",
      ),
    );
    expect(onClose).not.toHaveBeenCalled();
  });

  it("falls back to the generic failure for a mute rejection", async () => {
    mockRegister.mockRejectedValue(undefined);
    renderForm();
    await submitKey("0123456789abcdef");
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toBe(
        enSettings.channels.connect_failed,
      ),
    );
  });
});
