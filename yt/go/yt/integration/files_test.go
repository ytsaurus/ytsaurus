package integration

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestFiles(t *testing.T) {
	t.Parallel()

	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "WriteReadFile", Test: suite.TestWriteReadFile},
		{Name: "ReadFileError", Test: suite.TestReadFileError},
		{Name: "WriteFileError", Test: suite.TestWriteFileError},
		{Name: "ReadFileOffsetLength", Test: suite.TestReadFileOffsetLength},
		{Name: "WriteLargeFile", Test: suite.TestWriteLargeFile},
		{Name: "AppendFile", Test: suite.TestAppendFile},
		{Name: "ReadFileInTx", Test: suite.TestReadFileInTx},
		{Name: "CloseFileReaderBeforeEnd", Test: suite.TestCloseFileReaderBeforeEnd},
	})
}

func writeFile(ctx context.Context, t *testing.T, yc yt.CypressClient, fc yt.FileClient, path ypath.YPath, content []byte) {
	t.Helper()

	_, err := yc.CreateNode(ctx, path, yt.NodeFile, &yt.CreateNodeOptions{IgnoreExisting: true})
	require.NoError(t, err)

	w, err := fc.WriteFile(ctx, path, nil)
	require.NoError(t, err)

	_, err = w.Write(content)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func readFile(ctx context.Context, t *testing.T, fc yt.FileClient, path ypath.YPath, opts *yt.ReadFileOptions) []byte {
	t.Helper()

	r, err := fc.ReadFile(ctx, path, opts)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	file, err := io.ReadAll(r)
	require.NoError(t, err)
	return file
}

func (s *Suite) TestWriteReadFile(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	name := tmpPath()

	_, err := yc.CreateNode(ctx, name, yt.NodeFile, nil)
	require.NoError(t, err)

	w, err := yc.WriteFile(ctx, name, nil)
	require.NoError(t, err)

	_, err = w.Write([]byte("test"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := yc.ReadFile(ctx, name, nil)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	file, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, file, []byte("test"))
}

func (s *Suite) TestReadFileError(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	name := tmpPath()

	_, err := yc.ReadFile(ctx, name, nil)
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, 500))
}

func (s *Suite) TestWriteFileError(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	name := tmpPath()

	w, err := yc.WriteFile(ctx, name, nil)
	if err == nil {
		err = w.Close()
	}
	require.Error(t, err)
	require.True(t, yterrors.ContainsErrorCode(err, 500))
}

func (s *Suite) TestReadFileOffsetLength(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	name := tmpPath()

	content := make([]byte, 100)
	for i := range content {
		content[i] = byte(i)
	}
	writeFile(ctx, t, yc, yc, name, content)

	file := readFile(ctx, t, yc, name, &yt.ReadFileOptions{Offset: ptr.Int64(10), Length: ptr.Int64(20)})
	require.Equal(t, content[10:30], file)
}

func (s *Suite) TestWriteLargeFile(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	name := tmpPath()

	// Larger than the default streaming window of the RPC client.
	content := make([]byte, 40*1024*1024)
	_, err := rand.Read(content)
	require.NoError(t, err)

	writeFile(ctx, t, yc, yc, name, content)

	file := readFile(ctx, t, yc, name, nil)
	require.True(t, bytes.Equal(content, file), "file content differs")
}

func (s *Suite) TestAppendFile(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	name := tmpPath()
	writeFile(ctx, t, yc, yc, name, []byte("abacaba"))

	w, err := yc.WriteFile(ctx, ypath.NewRich(name.String()).SetAppend(), nil)
	require.NoError(t, err)
	_, err = w.Write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.Equal(t, []byte("abacabanew"), readFile(ctx, t, yc, name, nil))
}

func (s *Suite) TestReadFileInTx(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	name := tmpPath()

	tx, err := yc.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Abort() }()

	writeFile(ctx, t, tx, tx, name, []byte("written in tx"))

	require.Equal(t, []byte("written in tx"), readFile(ctx, t, tx, name, nil))

	exists, err := yc.NodeExists(ctx, name, nil)
	require.NoError(t, err)
	require.False(t, exists, "file must not be visible outside of the transaction")
}

func (s *Suite) TestCloseFileReaderBeforeEnd(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	name := tmpPath()
	writeFile(ctx, t, yc, yc, name, make([]byte, 64*1024*1024))

	r, err := yc.ReadFile(ctx, name, nil)
	require.NoError(t, err)

	_, err = io.ReadFull(r, make([]byte, 1024))
	require.NoError(t, err)

	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close must not wait for the rest of the file")
	}

	// The client stays usable after an aborted read.
	require.Equal(t, []byte{0, 0, 0}, readFile(ctx, t, yc, name, &yt.ReadFileOptions{Length: ptr.Int64(3)}))
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
