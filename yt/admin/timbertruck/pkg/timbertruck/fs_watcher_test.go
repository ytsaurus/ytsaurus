package timbertruck

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testEventHandler struct {
	events    chan string
	rawEvents chan FileEvent
}

func newTestEventHandler() *testEventHandler {
	result := &testEventHandler{
		events:    make(chan string, 10),
		rawEvents: make(chan FileEvent, 10),
	}

	go func() {
		for e := range result.rawEvents {
			switch e {
			case FileCreateEvent:
				result.events <- "create"
			case FileRemoveOrRenameEvent:
				result.events <- "remove"
			default:
				panic(fmt.Sprintf("unknown event: %v", e))
			}
		}
	}()

	return result
}

func (h *testEventHandler) Close() {
	close(h.events)
}

func (h *testEventHandler) WaitEvent(timeout time.Duration, ctx context.Context) string {
	timeoutContext, cancelFunc := context.WithTimeout(ctx, timeout)
	defer cancelFunc()

	for {
		select {
		case <-timeoutContext.Done():
			return ""
		case event := <-h.events:
			return event
		}
	}
}

func TestFsWatcherSimpleRotate(t *testing.T) {
	fsWatcher, err := NewFsWatcher(slog.Default())
	require.NoError(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())
	t.Cleanup(cancelFunc)

	workDir := t.TempDir()

	watchFile := path.Join(workDir, "test-file")
	watchFileMoved := path.Join(workDir, "test-file-moved")

	handler := newTestEventHandler()
	err = fsWatcher.AddLogPath(watchFile, handler.rawEvents)
	require.NoError(t, err)

	go func() {
		_ = fsWatcher.Run(ctx)
	}()

	event := handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "")

	file, err := os.Create(watchFile)
	require.NoError(t, err)

	event = handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "create")

	err = file.Close()
	require.NoError(t, err)

	err = os.Rename(watchFile, watchFileMoved)
	require.NoError(t, err)

	event = handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "remove")
}

func TestFsWatcherOpenedRemoval(t *testing.T) {
	fsWatcher, err := NewFsWatcher(slog.Default())
	require.NoError(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())
	t.Cleanup(cancelFunc)

	workDir := t.TempDir()

	watchFile := path.Join(workDir, "test-file")

	handler := newTestEventHandler()
	err = fsWatcher.AddLogPath(watchFile, handler.rawEvents)
	require.NoError(t, err)

	go func() {
		_ = fsWatcher.Run(ctx)
	}()

	event := handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "")

	file, err := os.Create(watchFile)
	require.NoError(t, err)

	event = handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "create")

	err = os.Remove(watchFile)
	require.NoError(t, err)

	event = handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "remove")

	_, err = file.WriteString("foo")
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)
}

func TestFsWatcherHardLink(t *testing.T) {
	fsWatcher, err := NewFsWatcher(slog.Default())
	require.NoError(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())
	t.Cleanup(cancelFunc)

	workDir := t.TempDir()

	watchFile := path.Join(workDir, "test-file")

	handler := newTestEventHandler()
	err = fsWatcher.AddLogPath(watchFile, handler.rawEvents)
	require.NoError(t, err)

	go func() {
		_ = fsWatcher.Run(ctx)
	}()

	event := handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "")

	err = os.WriteFile(watchFile, []byte("foo"), 0644)
	require.NoError(t, err)

	event = handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "create")

	err = os.Link(watchFile, watchFile+".1")
	require.NoError(t, err)

	event = handler.WaitEvent(time.Millisecond*10, ctx)
	require.Equal(t, event, "")
}
