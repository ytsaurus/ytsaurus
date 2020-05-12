package coredump

import (
	"os"
	"runtime/debug"
	"testing"
)

func TestCoredump(t *testing.T) {
	if os.Getenv("AUTOCHECK") != "" {
		return
	}

	// Check sparse core file handling.
	ballast := make([]byte, 16<<20)
	_ = ballast

	debug.SetTraceback("crash")

	go func() {
		panic("core dump")
	}()

	<-make(chan struct{})
}
