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
}

// encodeInstallConfig marshals an Installation's config-bearing fields into
// the JSONB blob stored in channel_installation.config.
func encodeInstallConfig(key string, keyEncrypted []byte) ([]byte, error) {
	if key == "" {
		return nil, errors.New("wecomwebhook: key is required")
	}
	if len(keyEncrypted) == 0 {
		return nil, errors.New("wecomwebhook: sealed key is required")
	}
	return json.Marshal(installConfig{
		AppID:        key,
		KeyEncrypted: keyEncrypted,
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
		ID:              row.ID,
		WorkspaceID:     row.WorkspaceID,
		AgentID:         row.AgentID,
		InstallerUserID: row.InstallerUserID,
		Status:          InstallationStatus(row.Status),
		KeySuffix:       keySuffix(cfg.AppID),
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
	inst := Installation{KeySuffix: keySuffix(cfg.AppID)}
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

// InstallationKeySuffixFromConfig renders the masked key suffix from a raw
// channel_installation config blob, for HTTP responses. The full key never
// crosses the API boundary; this is the only piece of it that does.
func InstallationKeySuffixFromConfig(raw json.RawMessage) (string, error) {
	var cfg installConfig
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return "", fmt.Errorf("wecomwebhook: decode installation config: %w", err)
		}
	}
	return keySuffix(cfg.AppID), nil
}
