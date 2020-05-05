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

	debug.SetTraceback("crash")

	go func() {
		panic("core dump")
	}()

	<-make(chan struct{})
}
