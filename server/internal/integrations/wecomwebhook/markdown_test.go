package wecomwebhook

import "testing"

func TestRenderMarkdownPassesThroughSubset(t *testing.T) {
	in := "# 标题\n\n**加粗** [链接](https://example.com) `code`\n\n> 引用\n\n<font color=\"info\">info</font>"
	if got, want := RenderMarkdown(in), in; got != want {
		t.Fatalf("subset input must pass through unchanged:\ngot  %q\nwant %q", got, want)
	}
}

func TestRenderMarkdownFencedCodeBecomesQuote(t *testing.T) {
	in := "before\n```go\nfmt.Println(\"hi\")\nx := 1\n```\nafter"
	want := "before\n> fmt.Println(\"hi\")\n> x := 1\nafter"
	if got := RenderMarkdown(in); got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestRenderMarkdownImagesBecomeLinks(t *testing.T) {
	in := "![截图](https://example.com/a.png) and ![ ](https://example.com/b.png)"
	want := "[截图](https://example.com/a.png) and https://example.com/b.png"
	if got := RenderMarkdown(in); got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestRenderMarkdownTablesFlatten(t *testing.T) {
	in := "| 名称 | 状态 |\n| --- | --- |\n| A | 完成 |\n| B | 失败 |"
	want := "名称 | 状态\nA | 完成\nB | 失败"
	if got := RenderMarkdown(in); got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestRenderMarkdownBulletsNormalize(t *testing.T) {
	in := "* one\n* **bold item**\n+ two\n  * nested\n1. ordered"
	want := "- one\n- **bold item**\n- two\n  - nested\n1. ordered"
	if got := RenderMarkdown(in); got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestRenderMarkdownEmpty(t *testing.T) {
	if got := RenderMarkdown(""); got != "" {
		t.Fatalf("empty input must stay empty, got %q", got)
	}
}
