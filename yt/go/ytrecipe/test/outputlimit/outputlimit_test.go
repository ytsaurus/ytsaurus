package outputlimit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ytrecipe"
)

func TestOutputLimit(t *testing.T) {
	if os.Getenv("AUTOCHECK") != "" {
		return
	}

	f, err := os.Create(filepath.Join(ytrecipe.YTRecipeOutput, "big.log"))
	require.NoError(t, err)
	defer f.Close()

	oneMB := make([]byte, 1<<20)
	for i := 0; i < 1<<10; i++ {
		_, err = f.Write(oneMB)
		require.NoError(t, err)
	}
}
