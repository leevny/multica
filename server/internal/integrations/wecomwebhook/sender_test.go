package wecomwebhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

func TestChunkUTF8(t *testing.T) {
	tests := []struct {
		name    string
		content string
		limit   int
		want    []string
	}{
		{
			name:    "empty content yields no pieces",
			content: "",
			limit:   10,
			want:    nil,
		},
		{
			name:    "within limit is one piece",
			content: "hello",
			limit:   10,
			want:    []string{"hello"},
		},
		{
			name:    "ascii splits at the last space inside the window",
			content: "alpha beta gamma",
			limit:   10,
			want:    []string{"alpha ", "beta gamma"},
		},
		{
			name:    "ascii splits at newline before space",
			content: "first line\nsecond line",
			limit:   12,
			want:    []string{"first line\n", "second line"},
		},
		{
			name: "cjk splits on byte boundary without breaking a rune",
			// 6 runes of 3 bytes = 18 bytes; limit 9 cuts after the 3rd rune.
			content: "一二三四五六",
			limit:   9,
			want:    []string{"一二三", "四五六"},
		},
		{
			name:    "mixed cjk and ascii never splits a rune",
			content: "a一b二c三d",
			limit:   5,
			// Each 5-byte window lands inside a 3-byte rune, so every cut
			// steps back to the rune start: pieces are rune-aligned even
			// when shorter than the limit.
			want: []string{"a一b", "二c", "三d"},
		},
		{
			name:    "rejoins to the original",
			content: strings.Repeat("多字节文本", 40),
			limit:   16,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chunkUTF8(tt.content, tt.limit)
			if tt.want != nil {
				if strings.Join(got, "|") != strings.Join(tt.want, "|") {
					t.Fatalf("chunkUTF8(%q, %d) = %q, want %q", tt.content, tt.limit, got, tt.want)
				}
			}
			var joined strings.Builder
			for _, piece := range got {
				if len(piece) > tt.limit {
					t.Fatalf("piece %q exceeds limit %d", piece, tt.limit)
				}
				joined.WriteString(piece)
			}
			if joined.String() != tt.content {
				t.Fatalf("pieces re-join to %q, want %q", joined.String(), tt.content)
			}
		})
	}
}

// newTestServer spins an httptest server answering WeCom-style replies and
// returns the sender pointed at it plus the received bodies.
func newTestServer(t *testing.T, responses func(n int) sendResponse) (*Sender, *[]string, *atomic.Int32) {
	t.Helper()
	var bodies []string
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := json.Marshal(responses(int(calls.Load())))
		var payload textPayload
		// Bodies are one of the two payload shapes; decode generically.
		var generic map[string]any
		_ = json.NewDecoder(r.Body).Decode(&generic)
		raw, _ := json.Marshal(generic)
		_ = json.Unmarshal(raw, &payload)
		if msgtype, ok := generic["msgtype"].(string); ok && msgtype == "markdown" {
			bodies = append(bodies, generic["markdown"].(map[string]any)["content"].(string))
		} else {
			bodies = append(bodies, payload.Text.Content)
		}
		if !strings.Contains(r.URL.RawQuery, "key=") {
			t.Errorf("request missing key query param: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	sender := NewSender(WithBaseURL(srv.URL))
	return sender, &bodies, &calls
}

func TestSendTextChunksRequests(t *testing.T) {
	sender, bodies, calls := newTestServer(t, func(n int) sendResponse { return sendResponse{} })
	long := strings.Repeat("中文字符", 200) // 2400 bytes > 2048
	ids, err := sender.SendText(context.Background(), "k1", long)
	if err != nil {
		t.Fatalf("SendText: %v", err)
	}
	if len(ids) != 2 || len(*bodies) != 2 {
		t.Fatalf("want 2 chunked sends, got %d ids / %d bodies", len(ids), len(*bodies))
	}
	if int(calls.Load()) != 2 {
		t.Fatalf("want 2 HTTP calls, got %d", calls.Load())
	}
	var total int
	for _, body := range *bodies {
		if len(body) > MaxTextBytes {
			t.Fatalf("chunk %d bytes exceeds %d", len(body), MaxTextBytes)
		}
		total += len(body)
	}
	if total != len(long) {
		t.Fatalf("chunks lost content: %d vs %d bytes", total, len(long))
	}
}

func TestSendMarkdownUsesLimit(t *testing.T) {
	sender, bodies, calls := newTestServer(t, func(n int) sendResponse { return sendResponse{} })
	long := strings.Repeat("x", MaxMarkdownBytes+100)
	if _, err := sender.SendMarkdown(context.Background(), "k1", long); err != nil {
		t.Fatalf("SendMarkdown: %v", err)
	}
	if int(calls.Load()) != 2 {
		t.Fatalf("want 2 calls, got %d", calls.Load())
	}
	for _, body := range *bodies {
		if len(body) > MaxMarkdownBytes {
			t.Fatalf("chunk %d exceeds markdown limit %d", len(body), MaxMarkdownBytes)
		}
	}
}

func TestSendRetriesOnFrequencyLimit(t *testing.T) {
	// First call refused with 45009, second accepted — the message must
	// arrive after the backoff, not be dropped.
	sender, bodies, calls := newTestServer(t, func(n int) sendResponse {
		if n == 1 {
			return sendResponse{ErrCode: errcodeFrequencyLimit, ErrMsg: "api freq out of limit"}
		}
		return sendResponse{}
	})
	ids, err := sender.SendText(context.Background(), "k1", "hi")
	if err != nil {
		t.Fatalf("SendText with one 45009 refusal: %v", err)
	}
	// bodies records every HTTP call, the refusal included; the delivered
	// count is len(ids).
	if len(ids) != 1 {
		t.Fatalf("want 1 delivered message id, got %d", len(ids))
	}
	if int(calls.Load()) != 2 {
		t.Fatalf("want 2 calls (1 refusal + 1 success), got %d", calls.Load())
	}
	_ = bodies
}

func TestSendRetriesExhaustsBudget(t *testing.T) {
	sender, _, calls := newTestServer(t, func(n int) sendResponse {
		return sendResponse{ErrCode: 93000, ErrMsg: "webhook deleted"}
	})
	// 3 attempts × ~1.5s of backoff for a 45009-shaped retry chain; use the
	// generic path (93000 is not 45009) so the delays stay sub-second.
	_, err := sender.SendText(context.Background(), "k1", "hi")
	var api *apiError
	if !errors.As(err, &api) {
		t.Fatalf("want apiError after budget exhausted, got %v", err)
	}
	if int(calls.Load()) != sendRetryAttempts {
		t.Fatalf("want %d attempts, got %d", sendRetryAttempts, calls.Load())
	}
}

func TestSendDoesNotRetryTransportError(t *testing.T) {
	sender := NewSender(WithBaseURL("http://127.0.0.1:1")) // nothing listens
	start := time.Now()
	_, err := sender.SendText(context.Background(), "k1", "hi")
	if err == nil {
		t.Fatal("want transport error")
	}
	var api *apiError
	if errors.As(err, &api) {
		t.Fatalf("transport failure must not classify as apiError: %v", err)
	}
	if int(time.Since(start)) > 2*int(time.Second) {
		t.Fatalf("transport failure must fail fast, took %s", time.Since(start))
	}
}

func TestSendResultShape(t *testing.T) {
	got := sendResult([]string{"a", "b"})
	if got.MessageID != "a" || len(got.MessageIDs) != 2 {
		t.Fatalf("unexpected result %+v", got)
	}
	if empty := sendResult(nil); empty.MessageID != "" {
		t.Fatalf("empty ids must produce zero result, got %+v", empty)
	}
}

func TestChannelSendDeliversText(t *testing.T) {
	sender, bodies, _ := newTestServer(t, func(n int) sendResponse { return sendResponse{} })
	ch := &webhookChannel{key: "key-1", sender: sender}
	res, err := ch.Send(context.Background(), channel.OutboundMessage{
		ChatID:   "ignored",
		Text:     "hello 你好",
		ThreadID: "also ignored",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.MessageID == "" {
		t.Fatal("want a message id")
	}
	if len(*bodies) != 1 || (*bodies)[0] != "hello 你好" {
		t.Fatalf("want body %q, got %q", "hello 你好", *bodies)
	}
}

func TestChannelContract(t *testing.T) {
	ch := &webhookChannel{key: "k", sender: NewSender()}
	if ch.Type() != TypeWecomWebhook {
		t.Fatalf("Type() = %q", ch.Type())
	}
	if ch.Capabilities() != channel.CapText {
		t.Fatalf("Capabilities() = %v", ch.Capabilities())
	}
	if err := ch.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect must be a no-op, got %v", err)
	}
	// Connect must block until the ctx is cancelled and then return nil —
	// an immediate return would put the Supervisor into reconnect backoff.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ch.Connect(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Connect after cancel = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect did not return after ctx cancel")
	}
}
