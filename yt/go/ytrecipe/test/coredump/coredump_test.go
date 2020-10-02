package coredump

import (
	"runtime/debug"
	"testing"
)

func TestCoredump(t *testing.T) {
	// Check sparse core file handling.
	ballast := make([]byte, 16<<20)
	_ = ballast

	debug.SetTraceback("crash")

	go func() {
		panic("core dump")
	}()

	<-make(chan struct{})
}
