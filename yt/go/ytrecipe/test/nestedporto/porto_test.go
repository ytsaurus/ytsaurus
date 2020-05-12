package nestedporto

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	porto "a.yandex-team.ru/infra/porto/api_go"
	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/yt/go/ytrecipe"
)

func TestNestedPorto(t *testing.T) {
	conn, err := porto.Dial()
	require.NoError(t, err)

	volumePath := yatest.WorkPath(filepath.Join(ytrecipe.YTRecipeOutput, "volume"))

	require.NoError(t, os.Mkdir(volumePath, 0777))
	_, err = conn.CreateVolume(volumePath, map[string]string{
		"space_limit": "1G",
		"backend":     "tmpfs",
	})

	require.NoError(t, err)

	volumePath = yatest.WorkPath(filepath.Join(ytrecipe.YTRecipeHDD, "volume"))

	require.NoError(t, os.Mkdir(volumePath, 0777))
	_, err = conn.CreateVolume(volumePath, map[string]string{
		"space_limit": "1G",
		"storage":     volumePath,
	})

	require.NoError(t, err)
}
