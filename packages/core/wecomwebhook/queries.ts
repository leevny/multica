import { queryOptions } from "@tanstack/react-query";
import { api } from "../api";

/**
 * Query key namespace for everything wecomwebhook-installation-related.
 * Realtime sync invalidates `installations(wsId)` on
 * `wecomwebhook_installation:*` events so the Settings panel updates without
 * a manual refetch when another tab / an admin on another machine connects
 * or disconnects a group-robot webhook.
 */
export const wecomWebhookKeys = {
  all: (wsId: string) => ["wecomwebhook", wsId] as const,
  installations: (wsId: string) => [...wecomWebhookKeys.all(wsId), "installations"] as const,
};

export const wecomWebhookInstallationsOptions = (wsId: string) =>
  queryOptions({
    queryKey: wecomWebhookKeys.installations(wsId),
    queryFn: () => api.listWecomWebhookInstallations(wsId),
    enabled: !!wsId,
  });
