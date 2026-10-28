//go:build windows
// +build windows

package windowsconsole

import (
	"testing"

	"github.com/Azure/go-ansiterm"
	"github.com/Azure/go-ansiterm/winterm"
)

func TestKeyToString(t *testing.T) {
	ke := &winterm.KEY_EVENT_RECORD{
		ControlKeyState: winterm.LEFT_ALT_PRESSED,
		UnicodeChar:     65, // capital A
	}

	const expected = ansiterm.KEY_ESC_N + "a"
	out := keyToString(ke, nil)
	if out != expected {
		t.Errorf("expected %s, got %s", expected, out)
	}
}
