package wecomwebhook

// markdown.go — render generic markdown into the WeCom group-robot markdown
// v1 subset. The subset supports headings (#..######), **bold**,
// [link](url), `inline code`, > quotes, and the three font colors
// (<font color="info|comment|warning">…</font>). It has NO lists, tables,
// fenced code blocks, or images — and agent output routinely contains all
// four, so this renderer degrades them instead of letting WeCom print the
// raw syntax on screen:
//
//   - fenced code blocks  → > quoted lines (content preserved verbatim)
//   - images              → [alt](url) links
//   - tables              → plain text rows (separator row dropped)
//   - unordered lists     → normalized to "- " bullets (renders as text)
//   - ordered lists       → left as-is (numbers read fine as text)
//
// Everything else passes through unchanged. Line-based on purpose: the
// constructs that differ are all line-anchored, and a scanner this small is
// auditable in one read.

import (
	"strings"
)

// RenderMarkdown converts generic markdown to the WeCom webhook subset.
func RenderMarkdown(content string) string {
	if content == "" {
		return ""
	}
	var out []string
	inFence := false
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if isFenceOpen(trimmed) {
			if inFence {
				// Closing fence: the block just ended.
				inFence = false
				continue
			}
			// Opening fence: everything after is quoted.
			inFence = true
			continue
		}
		if inFence {
			out = append(out, "> "+trimmed)
			continue
		}
		// A table separator row flattens to nothing — drop the line rather
		// than leave a blank one behind.
		if cells, ok := tableRow(line); ok {
			if cells == "" {
				continue
			}
			out = append(out, cells)
			continue
		}
		out = append(out, convertLine(line))
	}
	return strings.Join(out, "\n")
}

// isFenceOpen reports whether the line opens or closes a fenced code block.
func isFenceOpen(trimmed string) bool {
	return strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~")
}

// convertLine rewrites one non-fence, non-table line.
func convertLine(line string) string {
	return normalizeBullet(convertImage(line))
}

// convertImage turns ![alt](url) into [alt](url); the webhook subset links
// render, images do not. An empty/whitespace alt becomes the bare URL.
func convertImage(line string) string {
	if !strings.Contains(line, "![") {
		return line
	}
	var out strings.Builder
	for {
		start := strings.Index(line, "![")
		if start < 0 {
			out.WriteString(line)
			return out.String()
		}
		end := strings.Index(line[start:], "](")
		if end < 0 {
			out.WriteString(line)
			return out.String()
		}
		end += start
		closeIdx := strings.Index(line[end:], ")")
		if closeIdx < 0 {
			out.WriteString(line)
			return out.String()
		}
		closeIdx += end
		alt := strings.TrimSpace(line[start+2 : end])
		url := line[end+2 : closeIdx]
		out.WriteString(line[:start])
		if alt == "" {
			out.WriteString(url)
		} else {
			out.WriteString("[" + alt + "](" + url + ")")
		}
		line = line[closeIdx+1:]
	}
}

// tableRow recognizes a markdown table row and flattens it to text. A
// separator row (|---|---|) returns ok=true with an empty string so the
// caller drops it.
func tableRow(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "|") {
		return "", false
	}
	cells := strings.Split(strings.TrimSuffix(strings.TrimPrefix(trimmed, "|"), "|"), "|")
	plain := make([]string, 0, len(cells))
	isSeparator := true
	for _, cell := range cells {
		c := strings.TrimSpace(cell)
		plain = append(plain, c)
		if !isSeparatorCell(c) {
			isSeparator = false
		}
	}
	if isSeparator {
		return "", true
	}
	return strings.Join(plain, " | "), true
}

// isSeparatorCell reports whether a cell is only dashes/colons — the table
// header/body divider row, which carries no content.
func isSeparatorCell(cell string) bool {
	if cell == "" {
		return false
	}
	for _, r := range cell {
		if r != '-' && r != ':' && r != ' ' {
			return false
		}
	}
	return true
}

// normalizeBullet rewrites `*` and `+` unordered-list markers to `-`; WeCom
// markdown has no list construct, and the dash reads as a bullet in plain
// rendering. Ordered markers (1.) pass through untouched.
func normalizeBullet(line string) string {
	trimmed := strings.TrimLeft(line, " \t")
	indent := line[:len(line)-len(trimmed)]
	switch {
	case strings.HasPrefix(trimmed, "* ") && !strings.HasPrefix(trimmed, "**"),
		strings.HasPrefix(trimmed, "+ "):
		return indent + "-" + trimmed[1:]
	default:
		return line
	}
}
