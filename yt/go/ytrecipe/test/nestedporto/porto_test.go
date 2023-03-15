package nestedporto

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	porto "a.yandex-team.ru/library/go/porto"
	"a.yandex-team.ru/library/go/test/yatest"
)

func TestNestedPorto(t *testing.T) {
	conn, err := porto.Dial()
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(yatest.OutputPath("ytrecipe_output"), 0777))
	require.NoError(t, os.MkdirAll(os.Getenv("HDD_PATH"), 0777))

	volumePath := filepath.Join(yatest.OutputPath("ytrecipe_output"), "volume")

	require.NoError(t, os.Mkdir(volumePath, 0777))
	_, err = conn.CreateVolume(volumePath, map[string]string{
		"space_limit": "1G",
		"backend":     "tmpfs",
	})

	require.NoError(t, err)

	volumePath = filepath.Join(os.Getenv("HDD_PATH"), "volume")

	require.NoError(t, os.Mkdir(volumePath, 0777))
	_, err = conn.CreateVolume(volumePath, map[string]string{
		"space_limit": "1G",
		"storage":     volumePath,
	})

	require.NoError(t, err)
}
