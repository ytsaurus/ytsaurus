package mapreduce

import (
	"context"
	"io"
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
