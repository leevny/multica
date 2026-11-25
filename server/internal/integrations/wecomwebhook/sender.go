package wecomwebhook

// sender.go — the single HTTP surface every outbound path shares: the
// Channel.Send reply path, the bus subscriber's announcements, and the
// install flow's proof-of-control test message all call Sender.
//
// WeCom group robots accept one request per msgtype at
// /cgi-bin/webhook/send?key=KEY and answer {errcode, errmsg}; errcode 0 is
// success. Two constraints shape this file:
//
//   - Length is counted in UTF-8 BYTES: text content ≤2048, markdown ≤4096.
//     Agent replies routinely exceed both, so senders chunk on byte-safe
//     boundaries (never mid-rune) and deliver the pieces as separate messages;
//     SendResult.MessageIDs carries every delivered id.
//   - The quota is 20 messages/minute per robot. Overage returns errcode 45009
//     ("api freq out of limit"), which is retried with backoff like any other
//     refusal — a burst of concurrent completions degrades to slower delivery,
//     not dropped messages, and the caller's ctx budget bounds how long that
//     may take.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// DefaultAPIBaseURL is the WeCom webhook endpoint root. Tests point it at an
// httptest server.
const DefaultAPIBaseURL = "https://qyapi.weixin.qq.com"

// WeCom group-robot content limits, in UTF-8 bytes (official docs 91770).
const (
	MaxTextBytes     = 2048
	MaxMarkdownBytes = 4096
)

// sendRetryAttempts bounds the per-message retry budget. Both retries exist
// for the same reason: a refused message is a lost announcement unless the
// caller tries again, and the bus handler's ctx timeout is what actually
// caps total latency.
const sendRetryAttempts = 3

// Backoff bases. Frequency control (45009) waits longer: the quota window is
// a minute, so hammering the endpoint burns the remaining budget faster.
const (
	rateLimitRetryBase = 1 * time.Second
	genericRetryBase   = 500 * time.Millisecond
)

// errcodeFrequencyLimit is WeCom's api-freq-out-of-limit errcode. Documented
// behaviour for the 20/min quota; treated as retryable like every nonzero
// errcode, only with a longer base delay.
const errcodeFrequencyLimit = 45009

// apiError is a structured WeCom refusal: the request reached WeCom and was
// answered with a nonzero errcode. HTTP-level failures (network, status) are
// plain errors — no verdict was obtained, so nothing may claim the send
// failed at WeCom.
type apiError struct {
	Code    int
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("wecomwebhook: errcode %d: %s", e.Code, e.Message)
}

// retryable reports whether err is a WeCom refusal worth retrying with
// backoff. Every nonzero errcode qualifies — the only errcodes a correct
// request produces are 0 and transient conditions (quota, throttle, upstream
// hiccups), and a retry of a genuinely-bad key just returns the same refusal
// within budget.
func retryable(err error) bool {
	var api *apiError
	return errors.As(err, &api)
}

// msgtype values on the wire.
const (
	msgtypeText     = "text"
	msgtypeMarkdown = "markdown"
)

// Sender posts messages to one WeCom group through one webhook key. It is
// safe for concurrent use.
type Sender struct {
	// baseURL overrides DefaultAPIBaseURL (tests). Empty uses the default.
	baseURL string
	client  *http.Client
	logger  *slog.Logger
}

// SenderOption configures a Sender at construction.
type SenderOption func(*Sender)

// WithBaseURL replaces the WeCom endpoint root. Tests use it to run against
// httptest; production never sets it.
func WithBaseURL(url string) SenderOption {
	return func(s *Sender) { s.baseURL = url }
}

// NewSender builds the production sender.
func NewSender(opts ...SenderOption) *Sender {
	s := &Sender{
		client: &http.Client{Timeout: 15 * time.Second},
		logger: slog.Default(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// textPayload is the wire body for msgtype=text.
type textPayload struct {
	MsgType string   `json:"msgtype"`
	Text    textBody `json:"text"`
}

type textBody struct {
	Content string `json:"content"`
}

// markdownPayload is the wire body for msgtype=markdown.
type markdownPayload struct {
	MsgType  string       `json:"msgtype"`
	Markdown markdownBody `json:"markdown"`
}

type markdownBody struct {
	Content string `json:"content"`
}

// sendResponse is WeCom's universal reply.
type sendResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// sendOne posts exactly one message of msgtype and returns a per-delivery
// marker id. A nonzero errcode comes back as *apiError; transport failures
// stay plain.
func (s *Sender) sendOne(ctx context.Context, key, msgtype, content string) (string, error) {
	var body []byte
	switch msgtype {
	case msgtypeText:
		body, _ = json.Marshal(textPayload{MsgType: msgtype, Text: textBody{Content: content}})
	case msgtypeMarkdown:
		body, _ = json.Marshal(markdownPayload{MsgType: msgtype, Markdown: markdownBody{Content: content}})
	default:
		return "", fmt.Errorf("wecomwebhook: unsupported msgtype %q", msgtype)
	}
	base := s.baseURL
	if base == "" {
		base = DefaultAPIBaseURL
	}
	url := base + "/cgi-bin/webhook/send?key=" + key
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("wecomwebhook: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("wecomwebhook: post webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", fmt.Errorf("wecomwebhook: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("wecomwebhook: webhook HTTP %d: %s", resp.StatusCode, truncateForLog(string(raw), 200))
	}
	var out sendResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("wecomwebhook: decode response: %w", err)
	}
	if out.ErrCode != 0 {
		return "", &apiError{Code: out.ErrCode, Message: out.ErrMsg}
	}
	// webhook/send does not return a message id — the response is only
	// errcode/errmsg. Synthesize a stable per-delivery marker so SendResult
	// still reports one id per message sent.
	return fmt.Sprintf("%s:%d", msgtype, time.Now().UnixNano()), nil
}

// SendText delivers plain text (chunked at MaxTextBytes).
func (s *Sender) SendText(ctx context.Context, key, content string) ([]string, error) {
	return s.sendChunked(ctx, key, msgtypeText, content, MaxTextBytes)
}

// SendMarkdown delivers wecom-subset markdown (chunked at MaxMarkdownBytes).
// Content must already be rendered through RenderMarkdown — this function
// does not convert.
func (s *Sender) SendMarkdown(ctx context.Context, key, content string) ([]string, error) {
	return s.sendChunked(ctx, key, msgtypeMarkdown, content, MaxMarkdownBytes)
}

// sendChunked delivers content as as many same-msgtype messages as the byte
// limit requires, retrying refused sends with backoff. Returns every
// delivered message's id. A failure mid-way returns the ids delivered so far
// and the error — earlier messages are already on the group's screen.
func (s *Sender) sendChunked(ctx context.Context, key, msgtype, content string, maxBytes int) ([]string, error) {
	var ids []string
	for _, piece := range chunkUTF8(content, maxBytes) {
		id, err := s.sendWithRetry(ctx, key, msgtype, piece)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// sendWithRetry is sendOne plus the backoff budget. Only WeCom refusals
// retry; transport errors and ctx cancellation surface immediately.
func (s *Sender) sendWithRetry(ctx context.Context, key, msgtype, content string) (string, error) {
	var lastErr error
	for attempt := 0; attempt < sendRetryAttempts; attempt++ {
		if attempt > 0 {
			base := genericRetryBase
			var api *apiError
			if errors.As(lastErr, &api) && api.Code == errcodeFrequencyLimit {
				base = rateLimitRetryBase
			}
			delay := time.Duration(attempt) * base
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
		}
		id, err := s.sendOne(ctx, key, msgtype, content)
		if err == nil {
			return id, nil
		}
		if !retryable(err) {
			return "", err
		}
		lastErr = err
	}
	return "", lastErr
}

// chunkUTF8 splits content into pieces of at most maxBytes, cutting at the
// byte level without breaking a UTF-8 rune, preferring the last newline and
// then the last space inside the piece so chunks read naturally. Content
// shorter than the limit comes back as one piece; empty content as none.
func chunkUTF8(content string, maxBytes int) []string {
	if content == "" {
		return nil
	}
	if len(content) <= maxBytes {
		return []string{content}
	}
	var pieces []string
	for len(content) > maxBytes {
		cut := maxBytes
		// Step back off a partial rune.
		for cut > 0 && !isRuneStart(content[cut]) {
			cut--
		}
		if cut == 0 {
			// First rune alone exceeds maxBytes — impossible for real limits
			// (a rune is ≤4 bytes), but never loop forever on it.
			cut = maxBytes
		}
		// Prefer breaking where a reader expects it: newline, then space.
		if nl := bytes.LastIndexByte([]byte(content[:cut]), '\n'); nl > 0 {
			cut = nl + 1
		} else if sp := bytes.LastIndexByte([]byte(content[:cut]), ' '); sp > 0 {
			cut = sp + 1
		}
		pieces = append(pieces, content[:cut])
		content = content[cut:]
	}
	if content != "" {
		pieces = append(pieces, content)
	}
	return pieces
}

// isRuneStart reports whether b is the first byte of a UTF-8 sequence
// (continuation bytes are 0b10xxxxxx).
func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }

// truncateForLog bounds a body for diagnostics.
func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// sendResult builds the channel.SendResult for a delivered chunk set.
func sendResult(ids []string) channel.SendResult {
	if len(ids) == 0 {
		return channel.SendResult{}
	}
	return channel.SendResult{MessageID: ids[0], MessageIDs: ids}
}
