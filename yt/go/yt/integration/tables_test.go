package integration

import (
	"context"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

type exampleRow struct {
	A string `yt:"a"`
	B int    `yt:"b"`
}

func TestTables(t *testing.T) {
	t.Parallel()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	t.Run("WriteReadTable", func(t *testing.T) {
		name := tmpPath()

		_, err := env.YT.CreateNode(ctx, name, yt.NodeTable, nil)
		require.NoError(t, err)

		w, err := env.YT.WriteTable(ctx, name, nil)
		require.NoError(t, err)

		require.NoError(t, w.Write(exampleRow{"foo", 1}))
		require.NoError(t, w.Write(exampleRow{"bar", 2}))
		require.NoError(t, w.Close())

		r, err := env.YT.ReadTable(ctx, name, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		var s exampleRow
		require.True(t, r.Next())
		require.NoError(t, r.Scan(&s))
		require.Equal(t, exampleRow{"foo", 1}, s)

		require.True(t, r.Next())
		require.NoError(t, r.Scan(&s))
		require.Equal(t, exampleRow{"bar", 2}, s)

		require.False(t, r.Next())
		require.NoError(t, r.Err())
	})

	t.Run("WriteWithSchema", func(t *testing.T) {

	})
}
