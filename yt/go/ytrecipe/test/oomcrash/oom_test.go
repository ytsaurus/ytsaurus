package oomcrash

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/test/yatest"
)

func TestOther(t *testing.T) {
	logPath := yatest.OutputPath("my_test.log")
	require.NoError(t, os.WriteFile(logPath, []byte("test finished"), 0666))
}

func TestOOM(t *testing.T) {

	t.Logf("OOM test started")

	var memory [][]byte

	for i := 0; i < 1<<20; i++ {
		memory = append(memory, bytes.Repeat([]byte{'f'}, 1<<20))
	}

	t.Logf("allocated %dMB", len(memory))
}
