//go:build !windows
// +build !windows

package term

import (
	"os"
	"reflect"
	"testing"

	cpty "github.com/creack/pty"
)

func newTTYForTest(t *testing.T) *os.File {
	t.Helper()
	pty, tty, err := cpty.Open()
	if err != nil {
		t.Fatalf("error creating pty: %v", err)
	} else {
		t.Cleanup(func() {
			_ = pty.Close()
			_ = tty.Close()
		})
	}

	return pty
}

func newTempFile(t *testing.T) *os.File {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "temp")
	if err != nil {
		t.Fatalf("error creating tempfile: %v", err)
	} else {
		t.Cleanup(func() { _ = tmpFile.Close() })
	}
	return tmpFile
}

func TestGetWinsize(t *testing.T) {
	tty := newTTYForTest(t)
	winSize, err := GetWinsize(tty.Fd())
	if err != nil {
		t.Error(err)
	}
	if winSize == nil {
		t.Fatal("winSize is nil")
	}

	newSize := Winsize{Width: 200, Height: 200, x: winSize.x, y: winSize.y}
	err = SetWinsize(tty.Fd(), &newSize)
	if err != nil {
		t.Fatal(err)
	}
	winSize, err = GetWinsize(tty.Fd())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*winSize, newSize) {
		t.Fatalf("expected: %+v, got: %+v", newSize, *winSize)
	}
}

func TestSetWinsize(t *testing.T) {
	tty := newTTYForTest(t)
	winSize, err := GetWinsize(tty.Fd())
	if err != nil {
		t.Fatal(err)
	}
	if winSize == nil {
		t.Fatal("winSize is nil")
	}
	newSize := Winsize{Width: 200, Height: 200, x: winSize.x, y: winSize.y}
	err = SetWinsize(tty.Fd(), &newSize)
	if err != nil {
		t.Fatal(err)
	}
	winSize, err = GetWinsize(tty.Fd())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*winSize, newSize) {
		t.Fatalf("expected: %+v, got: %+v", newSize, *winSize)
	}
}

func TestGetFdInfo(t *testing.T) {
	tty := newTTYForTest(t)
	inFd, isTerminal := GetFdInfo(tty)
	if inFd != tty.Fd() {
		t.Errorf("expected: %d, got: %d", tty.Fd(), inFd)
	}
	if !isTerminal {
		t.Error("expected isTerminal to be true")
	}
	tmpFile := newTempFile(t)
	inFd, isTerminal = GetFdInfo(tmpFile)
	if inFd != tmpFile.Fd() {
		t.Errorf("expected: %d, got: %d", tty.Fd(), inFd)
	}
	if isTerminal {
		t.Error("expected isTerminal to be false")
	}
}

func TestIsTerminal(t *testing.T) {
	tty := newTTYForTest(t)
	isTerminal := IsTerminal(tty.Fd())
	if !isTerminal {
		t.Fatalf("expected isTerminal to be true")
	}
	tmpFile := newTempFile(t)
	isTerminal = IsTerminal(tmpFile.Fd())
	if isTerminal {
		t.Fatalf("expected isTerminal to be false")
	}
}

func TestSaveState(t *testing.T) {
	tty := newTTYForTest(t)
	state, err := SaveState(tty.Fd())
	if err != nil {
		t.Error(err)
	}
	if state == nil {
		t.Fatal("state is nil")
	}
	tty = newTTYForTest(t)
	err = RestoreTerminal(tty.Fd(), state)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDisableEcho(t *testing.T) {
	tty := newTTYForTest(t)
	state, err := SetRawTerminal(tty.Fd())
	defer RestoreTerminal(tty.Fd(), state)
	if err != nil {
		t.Error(err)
	}
	if state == nil {
		t.Fatal("state is nil")
	}
	err = DisableEcho(tty.Fd(), state)
	if err != nil {
		t.Fatal(err)
	}
}
