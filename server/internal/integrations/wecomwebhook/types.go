// Package wecomwebhook adapts WeCom group-robot webhooks ("群机器人") to the
// shared channel engine. A webhook key posts text/markdown into the single
// WeCom group that owns it over plain HTTPS — no long connection, no inbound
// events (WeCom offers none for group robots), no identity system.
//
// The channel is therefore announce-only: the Supervisor holds a lease per
// installation so the lifecycle stays uniform, Connect blocks on ctx.Done(),
// and the bus subscriber (Outbound) announces chat:done / task:failed /
// task:cancelled for chat sessions explicitly bound to an installation.
// Sessions bind from the web UI (the only way a binding can exist, since
// nothing inbound ever creates one).
//
// The webhook key is the bearer credential for the group. It is sealed with
// secretbox at rest, and the install flow proves control by really posting a
// test message before the row is written. Key masking applies to every API
// response: members can list installations, and the response carries only a
// short suffix, never the key itself.
package wecomwebhook

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TypeWecomWebhook is the channel discriminator for the WeCom group-robot
// webhook adapter. It sits alongside the wecom-specific types (rather than in
// package channel) so a build that excludes this adapter does not force a
// package channel edit; wecom and slack follow the same pattern.
//
// "wecom" is the aibot smart bot (bidirectional WSS); this type is the group
// robot webhook (outbound HTTP push). They are different WeCom products and
// deliberately different channel types.
const TypeWecomWebhook channel.Type = "wecomwebhook"

// channelTypeWecomWebhook is the string form persisted in channel_installation.
const channelTypeWecomWebhook = string(TypeWecomWebhook)

// InstallationStatus mirrors the channel_installation.status column values.
type InstallationStatus string

const (
	InstallationActive  InstallationStatus = "active"
	InstallationRevoked InstallationStatus = "revoked"
)

// Installation is the decoded, in-memory view of a wecomwebhook
// channel_installation row. KeyEncrypted is a secretbox-sealed blob and never
// plaintext; callers who need the plaintext go through a decrypt function
// injected at construction.
type Installation struct {
	ID              pgtype.UUID
	WorkspaceID     pgtype.UUID
	AgentID         pgtype.UUID
	InstallerUserID pgtype.UUID
	Status          InstallationStatus

	// Key is the plaintext webhook key, minted per-read by the credentials
	// decoder. Never persisted; never logged; never returned over HTTP (the
	// list/install responses carry KeySuffix instead).
	Key string

	// KeySuffix is the last 4 characters of the key, safe to render in UIs so
	// an operator can tell two webhooks apart without exposing either.
	KeySuffix string

	// AnnounceStatusChanges is the installation-level switch for the
	// notify-system issue status_changed push. Default ON when the stored
	// config predates the toggle.
	AnnounceStatusChanges bool
}

// installConfig is the on-disk (JSONB) shape of channel_installation.config
// for wecomwebhook rows. AppID carries the key so the shared
// idx_channel_installation_type_appid index and GetChannelInstallationOwnerByAppID
// conflict resolution stay generic.
//
// The key therefore ALSO sits in plaintext inside the config blob's app_id
// slot — it is a bearer credential, so this widens who can read it from
// "holders of the sealed blob" to "anyone who can read channel_installation
// rows". Accepted for MVP to reuse the generic slot/index queries unchanged
// (per the approved design); KeyEncrypted remains the copy the sender uses so
// a future migration to a hashed slot only touches this file. The HTTP
// surface never echoes app_id — see KeySuffix.
type installConfig struct {
	AppID        string `json:"app_id"`
	KeyEncrypted []byte `json:"key_encrypted"`

	// AnnounceStatusChanges gates the notify-system issue status_changed push
	// (installation-level master switch, NOT per-user preference). Pointer so
	// absent means nil → default ON: a group robot was installed to watch the
	// workspace's work, and an older config blob predating the toggle must
	// not silently go quiet. nil is the stored form for "on".
	AnnounceStatusChanges *bool `json:"announce_status_changes,omitempty"`
}

// announceOn renders the stored toggle; absent means on.
func (c installConfig) announceOn() bool {
	return c.AnnounceStatusChanges == nil || *c.AnnounceStatusChanges
}

// encodeInstallConfig marshals an Installation's config-bearing fields into
// the JSONB blob stored in channel_installation.config.
func encodeInstallConfig(key string, keyEncrypted []byte, announceStatusChanges *bool) ([]byte, error) {
	if key == "" {
		return nil, errors.New("wecomwebhook: key is required")
	}
	if len(keyEncrypted) == 0 {
		return nil, errors.New("wecomwebhook: sealed key is required")
	}
	return json.Marshal(installConfig{
		AppID:                 key,
		KeyEncrypted:          keyEncrypted,
		AnnounceStatusChanges: announceStatusChanges,
	})
}

// installationFromRow hydrates an Installation from a channel_installation
// row. open decrypts the sealed key; passing a nil open yields an Installation
// without a usable key (the caller decides whether that is fatal). The row's
// ChannelType is trusted — the callers already scope queries by
// channel_type = 'wecomwebhook' — so it is not re-checked here.
func installationFromRow(row db.ChannelInstallation, open func([]byte) ([]byte, error)) (Installation, error) {
	var cfg installConfig
	if len(row.Config) > 0 {
		if err := json.Unmarshal(row.Config, &cfg); err != nil {
			return Installation{}, fmt.Errorf("wecomwebhook: decode installation config: %w", err)
		}
	}
	inst := Installation{
		ID:                    row.ID,
		WorkspaceID:           row.WorkspaceID,
		AgentID:               row.AgentID,
		InstallerUserID:       row.InstallerUserID,
		Status:                InstallationStatus(row.Status),
		KeySuffix:             keySuffix(cfg.AppID),
		AnnounceStatusChanges: cfg.announceOn(),
	}
	if len(cfg.KeyEncrypted) > 0 && open != nil {
		plain, err := open(cfg.KeyEncrypted)
		if err != nil {
			return Installation{}, fmt.Errorf("wecomwebhook: open installation key: %w", err)
		}
		inst.Key = string(plain)
	}
	return inst, nil
}

// installationFromConfig decodes the raw config blob the engine.Supervisor
// hands the Factory. The engine never reads inside it; only this adapter
// does.
func installationFromConfig(raw json.RawMessage, open func([]byte) ([]byte, error)) (Installation, error) {
	var cfg installConfig
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return Installation{}, fmt.Errorf("wecomwebhook: decode installation config: %w", err)
		}
	}
	inst := Installation{KeySuffix: keySuffix(cfg.AppID), AnnounceStatusChanges: cfg.announceOn()}
	if len(cfg.KeyEncrypted) > 0 && open != nil {
		plain, err := open(cfg.KeyEncrypted)
		if err != nil {
			return Installation{}, fmt.Errorf("wecomwebhook: open installation key: %w", err)
		}
		inst.Key = string(plain)
	}
	return inst, nil
}

// keySuffix renders the last 4 characters of a key for display, "…" when the
// key is too short to suffix safely.
func keySuffix(key string) string {
	if len(key) < 4 {
		return "…"
	}
	return key[len(key)-4:]
}

// PublicConfigView is the API-safe projection of a config blob: the masked
// key suffix and the announce toggle. The full key never crosses the API
// boundary; these two are the only pieces of the config that do.
type PublicConfigView struct {
	KeySuffix             string
	AnnounceStatusChanges bool
}

// PublicConfigFromRaw decodes a raw channel_installation config blob into the
// API-safe view. A malformed blob yields the "…" suffix with the default
// toggle — a display projection is never worth failing a list call over.
func PublicConfigFromRaw(raw json.RawMessage) PublicConfigView {
	var cfg installConfig
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &cfg)
	}
	return PublicConfigView{
		KeySuffix:             keySuffix(cfg.AppID),
		AnnounceStatusChanges: cfg.announceOn(),
	}
}
