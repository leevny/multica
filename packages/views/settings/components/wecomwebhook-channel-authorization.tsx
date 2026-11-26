"use client";

// wecomwebhook-channel-authorization.tsx — the paste-a-key half of connecting
// a WeCom group-robot webhook. There is no QR and no session: the key is the
// whole credential, and the backend's install call IS the test — it really
// posts a message into the group before persisting, so "install succeeds" and
// "the admin watched the test message land" are the same moment.

import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@multica/core/api";
import { Button } from "@multica/ui/components/ui/button";
import { DialogFooter } from "@multica/ui/components/ui/dialog";
import { Input } from "@multica/ui/components/ui/input";
import { Label } from "@multica/ui/components/ui/label";
import { Switch } from "@multica/ui/components/ui/switch";
import { wecomWebhookKeys } from "@multica/core/wecomwebhook";
import { useT } from "../../i18n";

export function WecomWebhookChannelAuthorization({
  wsId,
  agentId,
  onClose,
}: {
  wsId: string;
  agentId: string;
  onClose: () => void;
}) {
  const { t } = useT("settings");
  const qc = useQueryClient();
  const [key, setKey] = useState("");
  // Default ON: a group robot was installed to watch the workspace's work.
  // Absent on the wire means the server also treats it as ON.
  const [announceStatusChanges, setAnnounceStatusChanges] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      const installation = await api.registerWecomWebhook(wsId, agentId, {
        key: key.trim(),
        announce_status_changes: announceStatusChanges,
      });
      if (!installation.id || installation.status !== "active")
        throw new Error(t(($) => $.channels.connect_failed));
      await qc.invalidateQueries({
        queryKey: wecomWebhookKeys.installations(wsId),
      });
      toast.success(t(($) => $.channels.connect_success));
      onClose();
    } catch (reason) {
      setError(
        reason instanceof Error && reason.message
          ? reason.message
          : t(($) => $.channels.connect_failed),
      );
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={submit} className="grid gap-4" aria-busy={submitting}>
      <div className="flex items-center justify-between gap-3">
        <div className="space-y-0.5">
          <Label htmlFor="wecomwebhook-announce">
            {t(($) => $.wecomwebhook.announce_status_changes_label)}
          </Label>
          <p className="text-caption text-muted-foreground">
            {t(($) => $.wecomwebhook.announce_status_changes_hint)}
          </p>
        </div>
        <Switch
          id="wecomwebhook-announce"
          checked={announceStatusChanges}
          onCheckedChange={setAnnounceStatusChanges}
          aria-label={t(($) => $.wecomwebhook.announce_status_changes_label)}
        />
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="wecomwebhook-key">
          {t(($) => $.wecomwebhook.key_label)}
        </Label>
        <Input
          id="wecomwebhook-key"
          type="password"
          autoComplete="off"
          spellCheck={false}
          required
          disabled={submitting}
          value={key}
          onChange={(event) => setKey(event.target.value)}
          data-testid="wecomwebhook-key-input"
        />
        <p className="text-caption text-muted-foreground">
          {t(($) => $.wecomwebhook.key_hint)}
        </p>
      </div>
      {error ? (
        <p role="alert" className="text-caption text-destructive">
          {error}
        </p>
      ) : null}
      <DialogFooter>
        <Button
          type="button"
          variant="outline"
          disabled={submitting}
          onClick={onClose}
        >
          {t(($) => $.wecom.scan_close)}
        </Button>
        <Button type="submit" disabled={submitting || !key.trim()}>
          {submitting
            ? t(($) => $.wecomwebhook.installing)
            : t(($) => $.channels.connect)}
        </Button>
      </DialogFooter>
    </form>
  );
}
