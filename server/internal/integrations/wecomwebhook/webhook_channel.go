package wecomwebhook

// webhook_channel.go — the Channel + Factory the engine.Supervisor drives.
//
// There is no inbound path (WeCom group robots cannot receive anything), so
// no ResolverSet is registered and the channel router never sees this type.
// The Supervisor still builds one Channel per active installation and holds
// its lease — a webhook Channel's Connect blocks on ctx.Done() so the lease
// is held until shutdown, exactly like every connected adapter; an
// immediately-returning Connect would be treated as a dropped link and put
// the installation into a reconnect backoff loop.

import (
	"context"
	"errors"
	"log/slog"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// ChannelDeps carries the construction-time seam the Factory and the Channel
// need: the decrypter for the at-rest key and the sender used for delivery.
// Decrypt is box.Open from the deployment's MULTICA_WECOM_WEBHOOK_SECRET_KEY
// secretbox; tests substitute a stub.
type ChannelDeps struct {
	// Decrypt opens a sealed key. Required — without it the Factory cannot
	// build a working Channel and registration is refused.
	Decrypt func([]byte) ([]byte, error)

	// Sender delivers messages. Nil uses the production WeCom endpoint.
	Sender *Sender

	Logger *slog.Logger
}

// webhookChannel is one installation's webhook pusher. Stateless beyond the
// key: every Send is an independent HTTP POST.
type webhookChannel struct {
	key    string
	sender *Sender
}

var _ channel.Channel = (*webhookChannel)(nil)

func (c *webhookChannel) Type() channel.Type { return TypeWecomWebhook }

// Connect holds the installation's lease until ctx is cancelled. A webhook
// needs no connection; blocking here is what keeps the Supervisor from
// treating the installation as perpetually disconnected (see the file
// comment).
func (c *webhookChannel) Connect(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Disconnect releases nothing — there is no connection. Safe to call any
// number of times.
func (c *webhookChannel) Disconnect(_ context.Context) error { return nil }

// Send delivers Text to the webhook's group. Rich addressing (ThreadID,
// ReplyTo) does not exist for a webhook — a group robot has one destination,
// the group that owns the key, so those fields are ignored.
func (c *webhookChannel) Send(ctx context.Context, out channel.OutboundMessage) (channel.SendResult, error) {
	ids, err := c.sender.SendText(ctx, c.key, out.Text)
	return sendResult(ids), err
}

// Capabilities declares text only. A webhook can also post markdown, but
// that lives on the adapter's own sender (SendMarkdown), not on the
// cross-platform envelope — the core's Send path degrades to text, which is
// the honest baseline.
func (c *webhookChannel) Capabilities() channel.Capability { return channel.CapText }

// RegisterWecomWebhook registers the Factory for TypeWecomWebhook. Every
// active installation of this type gets a Channel built with its own key.
func RegisterWecomWebhook(registry *channel.Registry, deps ChannelDeps) error {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if deps.Decrypt == nil {
		return errors.New("wecomwebhook: RegisterWecomWebhook requires a decrypt function")
	}
	sender := deps.Sender
	if sender == nil {
		sender = NewSender()
	}
	registry.Register(TypeWecomWebhook, func(cfg channel.Config) (channel.Channel, error) {
		inst, err := installationFromConfig(cfg.Raw, deps.Decrypt)
		if err != nil {
			return nil, err
		}
		if inst.Key == "" {
			return nil, errors.New("wecomwebhook: installation has no usable key")
		}
		return &webhookChannel{key: inst.Key, sender: sender}, nil
	})
	logger.Info("wecomwebhook: factory registered (group robot, outbound only)")
	return nil
}
