"use client";

// wecomwebhook-push-dialog.tsx — the "推送到企微群" picker: point one chat
// session's terminal announcements (chat done / task failed / task
// cancelled) at one WeCom group-robot webhook. The webhook robot receives
// nothing inbound, so this explicit binding is the only way the announce
// path ever has a destination; the session's current binding arrives on
// channel_source.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@multica/core/api";
import { useWorkspaceId } from "@multica/core/hooks";
import { chatKeys } from "@multica/core/chat/queries";
import { wecomWebhookInstallationsOptions } from "@multica/core/wecomwebhook";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@multica/ui/components/ui/dialog";
import type { ChatSession } from "@multica/core/types";
import { useT } from "../../i18n";
import { IntegrationChannelIcon } from "../../settings/components/integration-channel-icon";

export function WecomWebhookPushDialog({
  session,
  open,
  onOpenChange,
}: {
  session: ChatSession;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { t } = useT("chat");
  const wsId = useWorkspaceId();
  const qc = useQueryClient();
  const installations = useQuery({
    ...wecomWebhookInstallationsOptions(wsId ?? ""),
    enabled: open && !!wsId,
  });

  const invalidate = () => {
    void qc.invalidateQueries({ queryKey: chatKeys.all(wsId ?? "") });
  };

  const bind = useMutation({
    mutationFn: (installationId: string) =>
      api.bindWecomWebhookChatSession(wsId ?? "", {
        chat_session_id: session.id,
        installation_id: installationId,
      }),
    onSuccess: () => {
      invalidate();
      toast.success(t(($) => $.push_wecomwebhook.bound_toast));
      onOpenChange(false);
    },
    onError: (reason) => {
      toast.error(
        reason instanceof Error && reason.message
          ? reason.message
          : t(($) => $.push_wecomwebhook.bind_failed),
      );
    },
  });

  const unbind = useMutation({
    mutationFn: () => api.unbindWecomWebhookChatSession(wsId ?? "", session.id),
    onSuccess: () => {
      invalidate();
      toast.success(t(($) => $.push_wecomwebhook.unbound_toast));
      onOpenChange(false);
    },
    onError: () => {
      toast.error(t(($) => $.push_wecomwebhook.unbind_failed));
    },
  });

  // channel_source carries the session's one binding; is_current_channel_route
  // distinguishes the live one from a historical generation.
  const boundInstallationId =
    session.channel_source?.channel_type === "wecomwebhook" &&
    session.is_current_channel_route
      ? session.channel_source.installation_id
      : null;

  const rows = installations.data?.installations.filter(
    (installation) => installation.status === "active",
  );

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>
            {t(($) => $.push_wecomwebhook.dialog_title)}
          </DialogTitle>
        </DialogHeader>
        <div className="space-y-1.5">
          {installations.isError ? (
            <p role="alert" className="text-caption text-destructive">
              {t(($) => $.push_wecomwebhook.load_failed)}
            </p>
          ) : installations.isPending ? (
            <p className="text-caption text-muted-foreground">
              {t(($) => $.push_wecomwebhook.loading)}
            </p>
          ) : rows && rows.length > 0 ? (
            rows.map((installation) => {
              const bound = installation.id === boundInstallationId;
              return (
                <button
                  key={installation.id}
                  type="button"
                  disabled={bind.isPending || unbind.isPending}
                  onClick={() =>
                    bound ? unbind.mutate() : bind.mutate(installation.id)
                  }
                  data-testid={`wecomwebhook-push-option-${installation.id}`}
                  className="flex w-full items-center gap-2 rounded-md border px-3 py-2 text-body hover:bg-surface-hover focus-visible:outline-2 focus-visible:outline-ring disabled:opacity-50"
                >
                  <IntegrationChannelIcon channel="wecomwebhook" />
                  <span className="min-w-0 flex-1 text-left">
                    {t(($) => $.push_wecomwebhook.installation_label, {
                      suffix: installation.key_suffix,
                    })}
                  </span>
                  <span
                    className={
                      bound
                        ? "text-caption text-destructive"
                        : "text-caption text-muted-foreground"
                    }
                  >
                    {bound
                      ? t(($) => $.push_wecomwebhook.stop_pushing)
                      : t(($) => $.push_wecomwebhook.start_pushing)}
                  </span>
                </button>
              );
            })
          ) : (
            <p className="text-caption text-muted-foreground">
              {t(($) => $.push_wecomwebhook.no_installations)}
            </p>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
