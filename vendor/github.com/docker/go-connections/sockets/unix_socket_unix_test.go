//go:build !windows

package sockets

import (
	"os"
	"syscall"
	"testing"
)

func TestUnixSocketWithOpts(t *testing.T) {
	socketFile, err := os.CreateTemp("", "test*.sock")
	if err != nil {
		t.Fatal(err)
	}
	_ = socketFile.Close()
	defer func() { _ = os.Remove(socketFile.Name()) }()

	uid, gid := os.Getuid(), os.Getgid()
	perms := os.FileMode(0660)
	l, err := NewUnixSocketWithOpts(socketFile.Name(), WithChown(uid, gid), WithChmod(perms))
	if err != nil {
		t.Fatal(err)
	}
	p, err := os.Stat(socketFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if p.Mode().Perm() != perms {
		t.Fatalf("unexpected file permissions: expected: %#o, got: %#o", perms, p.Mode().Perm())
	}
	if stat, ok := p.Sys().(*syscall.Stat_t); ok {
		if stat.Uid != uint32(uid) || stat.Gid != uint32(gid) {
			t.Fatalf("unexpected file ownership: expected: %d:%d, got: %d:%d", uid, gid, stat.Uid, stat.Gid)
		}
	}

	defer func() { _ = l.Close() }()

	echoStr := "hello"
	runTest(t, socketFile.Name(), l, echoStr)
}

// TestNewUnixSocket run under root user.
func TestNewUnixSocket(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root")
	}
	gid := os.Getgid()
	path := "/tmp/test.sock"
	echoStr := "hello"
	l, err := NewUnixSocket(path, gid)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	runTest(t, path, l, echoStr)
}
