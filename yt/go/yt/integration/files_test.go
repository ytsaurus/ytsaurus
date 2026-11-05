package integration

import (
	"context"
	"crypto/md5"
	"fmt"
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
			if err == nil {
				err = w.Close()
			}
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

		w, err := yt.WriteFile(env.Ctx, env.YT, name, yt.WithWriteFileBatchSize(100))
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

	t.Run("ComputeMD5", func(t *testing.T) {
		name := tmpPath()
		const batchSize = 64 * 1024
		content := make([]byte, 3*batchSize)
		for i := range content {
			content[i] = byte(i)
		}

		w, err := yt.WriteFile(
			env.Ctx,
			env.YT,
			name,
			yt.WithWriteFileBatchSize(batchSize),
			yt.WithWriteFileComputeMD5(true),
		)
		require.NoError(t, err)
		for begin := 0; begin < len(content); begin += batchSize {
			end := min(begin+batchSize, len(content))
			_, err = w.Write(content[begin:end])
			require.NoError(t, err)
		}
		require.NoError(t, w.Close())

		var fileMD5 string
		require.NoError(t, env.YT.GetNode(env.Ctx, name.Attr("md5"), &fileMD5, nil))
		require.Equal(t, fmt.Sprintf("%x", md5.Sum(content)), fileMD5)
	})

	t.Run("ComputeMD5Overwrite", func(t *testing.T) {
		name := tmpPath()

		w, err := yt.WriteFile(env.Ctx, env.YT, name)
		require.NoError(t, err)
		_, err = w.Write([]byte("old"))
		require.NoError(t, err)
		require.NoError(t, w.Close())

		w, err = yt.WriteFile(env.Ctx, env.YT, name,
			yt.WithWriteFileComputeMD5(true),
			yt.WithWriteFileCreateOptions(&yt.CreateNodeOptions{IgnoreExisting: true}),
		)
		require.NoError(t, err)
		_, err = w.Write([]byte("new"))
		require.NoError(t, err)
		require.NoError(t, w.Close())

		r, err := env.YT.ReadFile(env.Ctx, name, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("new"), got)

		var fileMD5 string
		require.NoError(t, env.YT.GetNode(env.Ctx, name.Attr("md5"), &fileMD5, nil))
		require.Equal(t, fmt.Sprintf("%x", md5.Sum([]byte("new"))), fileMD5)
	})

	t.Run("ComputeMD5CloseWithoutWriteDoesNotOverwrite", func(t *testing.T) {
		name := tmpPath()

		w, err := yt.WriteFile(env.Ctx, env.YT, name)
		require.NoError(t, err)
		_, err = w.Write([]byte("old"))
		require.NoError(t, err)
		require.NoError(t, w.Close())

		w, err = yt.WriteFile(env.Ctx, env.YT, name,
			yt.WithWriteFileComputeMD5(true),
			yt.WithWriteFileCreateOptions(&yt.CreateNodeOptions{IgnoreExisting: true}),
		)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		r, err := env.YT.ReadFile(env.Ctx, name, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("old"), got)

		exists, err := env.YT.NodeExists(env.Ctx, name.Attr("md5"), nil)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("ComputeMD5Append", func(t *testing.T) {
		name := tmpPath()

		w, err := yt.WriteFile(env.Ctx, env.YT, name, yt.WithWriteFileComputeMD5(true))
		require.NoError(t, err)
		_, err = w.Write([]byte("abacaba"))
		require.NoError(t, err)
		require.NoError(t, w.Close())

		w, err = yt.WriteFile(env.Ctx, env.YT, "<append=%true>"+name,
			yt.WithWriteFileComputeMD5(true),
			yt.WithWriteFileCreateOptions(&yt.CreateNodeOptions{IgnoreExisting: true}),
		)
		require.NoError(t, err)
		_, err = w.Write([]byte("new"))
		require.NoError(t, err)
		require.NoError(t, w.Close())

		r, err := env.YT.ReadFile(env.Ctx, name, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte("abacabanew"), got)

		var fileMD5 string
		require.NoError(t, env.YT.GetNode(env.Ctx, name.Attr("md5"), &fileMD5, nil))
		require.Equal(t, fmt.Sprintf("%x", md5.Sum([]byte("abacabanew"))), fileMD5)
	})

	t.Run("ComputeMD5AppendToEmptyFileWithoutMD5", func(t *testing.T) {
		name := tmpPath()

		_, err := env.YT.CreateNode(env.Ctx, name, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
		require.NoError(t, err)

		lw, err := env.YT.WriteFile(env.Ctx, name, nil)
		require.NoError(t, err)
		require.NoError(t, lw.Close())

		exists, err := env.YT.NodeExists(env.Ctx, name.Attr("md5"), nil)
		require.NoError(t, err)
		require.False(t, exists)

		w, err := yt.WriteFile(env.Ctx, env.YT, "<append=%true>"+name,
			yt.WithWriteFileComputeMD5(true),
			yt.WithWriteFileCreateOptions(&yt.CreateNodeOptions{IgnoreExisting: true}),
		)
		require.NoError(t, err)
		_, err = w.Write([]byte("new"))
		require.NoError(t, err)
		require.ErrorContains(t, w.Close(), "has no computed MD5 hash")
	})
}

func TestHighLevelFileReader(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	t.Run("BigRead", func(t *testing.T) {
		name := tmpPath()

		const testSize = 1024
		content := make([]byte, testSize)
		for i := range content {
			content[i] = byte(i)
		}

		w, err := yt.WriteFile(env.Ctx, env.YT, name)
		require.NoError(t, err)
		_, err = w.Write(content)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		r, err := yt.ReadFile(env.Ctx, env.YT, name, yt.WithReadFileRetries(3))
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, content, got)
	})
}
