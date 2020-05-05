package joberror

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/yt/go/ytrecipe"
)

func TestJobError(t *testing.T) {
	if os.Getenv("AUTOCHECK") != "" {
		return
	}

	_, err := os.Create(yatest.OutputPath(ytrecipe.CrashJobFileMarker))
	require.NoError(t, err)
}
