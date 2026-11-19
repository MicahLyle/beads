//go:build rbd_machine

package uimd

import "testing"

func TestMachineOutputPreservesBodyBytes(t *testing.T) {
	for _, input := range []string{
		"",
		"# Heading\n\nSome **bold** and `code`.\n",
		"run `find -name '*.captured'`\n<owner> & quoted \\\"text\\\"\n",
		"[link](https://example.com)\n\n| A | B |\n| - | - |\n| 1 | 2 |\n",
		"雪\t\r\n\x1b[31mraw\x1b[0m\x00\xff",
	} {
		if output := RenderMarkdown(input); output != input {
			t.Fatalf("body changed: got %q, want %q", output, input)
		}
	}
}

func TestMachineOutputDoesNotWrap(t *testing.T) {
	t.Setenv("BD_AGENT_MODE", "0")
	if width := WrapWidth(); width != 0 {
		t.Fatalf("machine output wraps at %d", width)
	}
}
