package integration

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestFiles(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	t.Run("P", func(t *testing.T) {
		t.Run("WriteReadFile", func(t *testing.T) {
			ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
			ctx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()

			name := tmpPath()

			_, err := env.YT.CreateNode(ctx, name, yt.NodeFile, nil)
			require.NoError(t, err)

			w, err := env.YT.WriteFile(ctx, name, nil)
			require.NoError(t, err)

			_, err = w.Write([]byte("test"))
			require.NoError(t, err)
			require.NoError(t, w.Close())

			r, err := env.YT.ReadFile(ctx, name, nil)
			require.NoError(t, err)
			defer func() { _ = r.Close() }()

			file, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, file, []byte("test"))
		})

		t.Run("ReadFileError", func(t *testing.T) {
			ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
			ctx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()

			name := tmpPath()

			_, err := env.YT.ReadFile(ctx, name, nil)
			require.Error(t, err)
			require.True(t, yterrors.ContainsErrorCode(err, 500))
		})

		t.Run("WriteFileError", func(t *testing.T) {
			ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
			ctx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()

			name := tmpPath()

			w, err := env.YT.WriteFile(ctx, name, nil)
			require.NoError(t, err)

			err = w.Close()
			require.Error(t, err)
			require.True(t, yterrors.ContainsErrorCode(err, 500))
		})
	})
}

func TestHighLevelFileWriter(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	t.Run("BigWrite", func(t *testing.T) {
		name := tmpPath()

		w, err := yt.WriteFile(env.Ctx, env.YT, name, yt.WithFileBatchSize(100))
		require.NoError(t, err)

		const testSize = 1024
		content := make([]byte, testSize)
		for i := range content {
			content[i] = byte(i)
			_, err := w.Write(content[i : i+1])
			require.NoError(t, err)
		}

		exists, err := env.YT.NodeExists(env.Ctx, name, nil)
		require.NoError(t, err)
		require.False(t, exists, "File should not be visible because it is written inside tx")

		require.NoError(t, w.Close())

		r, err := env.YT.ReadFile(env.Ctx, name, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, content, got)
	})
}
