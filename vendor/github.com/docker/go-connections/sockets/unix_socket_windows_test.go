package sockets

import (
	"os"
	"testing"
)

func TestUnixSocketWithOpts(t *testing.T) {
	socketFile, err := os.CreateTemp("", "test*.sock")
	if err != nil {
		t.Fatal(err)
	}
	_ = socketFile.Close()
	defer func() { _ = os.Remove(socketFile.Name()) }()

	l, err := NewUnixSocketWithOpts(socketFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	echoStr := "hello"
	runTest(t, socketFile.Name(), l, echoStr)
}
