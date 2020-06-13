package outputlimit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/test/yatest"
)

func TestOutputLimit(t *testing.T) {
	if os.Getenv("AUTOCHECK") != "" {
		return
	}

	require.NoError(t, os.MkdirAll(yatest.WorkPath("ytrecipe_output"), 0777))

	f, err := os.Create(filepath.Join(yatest.WorkPath("ytrecipe_output"), "big.log"))
	require.NoError(t, err)
	defer f.Close()

	oneMB := make([]byte, 1<<20)
	for i := 0; i < 1<<10; i++ {
		_, err = f.Write(oneMB)
		require.NoError(t, err)
	}
}
