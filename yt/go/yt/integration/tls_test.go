package integration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestTLS(t *testing.T) {
	t.Parallel()

	if os.Getenv("YT_TEST_TLS") == "" {
		t.Skip("TLS test should run against real cluster")
	}

	env := yttest.New(t, yttest.WithConfig(yt.Config{
		UseTLS:            true,
		ReadTokenFromFile: true,
	}))

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
