package mapreduce

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type fakeWriteClient struct {
	yt.Client

	writer io.WriteCloser
}

func (c *fakeWriteClient) CreateNode(
	context.Context,
	ypath.YPath,
	yt.NodeType,
	*yt.CreateNodeOptions,
) (yt.NodeID, error) {
	return yt.NodeID{}, nil
}

func (c *fakeWriteClient) WriteFile(
	_ context.Context,
	_ ypath.YPath,
	_ *yt.WriteFileOptions,
) (io.WriteCloser, error) {
	return c.writer, nil
}

type cancelWriter struct {
	ctx              context.Context
	writeStarted     chan struct{}
	writeStartedOnce sync.Once
	closed           chan struct{}
}

func (w *cancelWriter) Write([]byte) (int, error) {
	w.writeStartedOnce.Do(func() {
		close(w.writeStarted)
	})
	<-w.ctx.Done()
	return 0, w.ctx.Err()
}

func (w *cancelWriter) Close() error {
	close(w.closed)
	return nil
}

func TestWriteFileToTmpDirCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := &cancelWriter{
		ctx:          ctx,
		writeStarted: make(chan struct{}),
		closed:       make(chan struct{}),
	}
	mr := &client{yc: &fakeWriteClient{writer: writer}}

	errCh := make(chan error, 1)
	go func() {
		_, err := mr.writeFileToTmpDir(ctx, "//tmp", strings.NewReader("data"), false)
		errCh <- err
	}()

	<-writer.writeStarted
	cancel()

	require.ErrorIs(t, <-errCh, context.Canceled)
	select {
	case <-writer.closed:
	default:
		t.Fatal("writer was not closed after context cancellation")
	}
}

type happyWriter struct {
	written []byte
	closed  bool
}

func (w *happyWriter) Write(p []byte) (int, error) {
	w.written = append(w.written, p...)
	return len(p), nil
}

func (w *happyWriter) Close() error {
	w.closed = true
	return nil
}

func TestWriteFileToTmpDirHappyPath(t *testing.T) {
	ctx := context.Background()
	writer := &happyWriter{}
	mr := &client{yc: &fakeWriteClient{writer: writer}}

	data := "hello world"
	_, err := mr.writeFileToTmpDir(ctx, "//tmp", strings.NewReader(data), false)
	require.NoError(t, err)
	require.True(t, writer.closed, "writer must be closed on success")
	require.Equal(t, data, string(writer.written), "all data must be written")
}

func TestExecutableCacheRetriesFailureAndCachesSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binary")
	ctx := context.Background()
	var cache executableCache

	_, err := cache.get(ctx, path)
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, os.WriteFile(path, []byte("abc"), 0o600))
	binary, err := cache.get(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, binary.file.Close()) })
	require.Equal(t, "900150983cd24fb0d6963f7d28e17f72", binary.checksum)

	first := binary.newReader()
	var prefix [1]byte
	_, err = io.ReadFull(first, prefix[:])
	require.NoError(t, err)
	require.Equal(t, byte('a'), prefix[0])

	cached, err := cache.get(ctx, path+".missing")
	require.NoError(t, err)
	data, err := io.ReadAll(cached.newReader())
	require.NoError(t, err)
	require.Equal(t, "abc", string(data))
	remaining, err := io.ReadAll(first)
	require.NoError(t, err)
	require.Equal(t, "bc", string(remaining))
}

func TestUploadSelfWaitRespectsCancellation(t *testing.T) {
	mr := &client{}
	require.NoError(t, mr.binaryUploadGate.acquire(context.Background()))
	defer mr.binaryUploadGate.release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := mr.uploadSelf(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
