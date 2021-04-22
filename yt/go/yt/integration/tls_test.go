package integration

import (
	"os"
	"testing"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

func TestTLS(t *testing.T) {
	t.Parallel()

	if os.Getenv("YT_TEST_TLS") == "" {
		t.Skip("TLS test should run against real cluster")
	}

	env, cancel := yttest.NewEnv(t, yttest.WithConfig(yt.Config{
		UseTLS:            true,
		ReadTokenFromFile: true,
	}))
	defer cancel()

	name := env.TmpPath()

	_, err := env.YT.CreateNode(env.Ctx, name, yt.NodeTable, nil)
	require.NoError(t, err)

	w, err := env.YT.WriteTable(env.Ctx, name, nil)
	require.NoError(t, err)

	require.NoError(t, w.Write(exampleRow{"foo", 1}))
	require.NoError(t, w.Write(exampleRow{"bar", 2}))
	require.NoError(t, w.Write(exampleRow{B: 3}))
	require.NoError(t, w.Commit())
}
