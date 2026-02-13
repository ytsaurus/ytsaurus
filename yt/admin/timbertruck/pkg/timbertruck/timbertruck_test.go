package timbertruck_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
)

func newTestConfig(workDir string) timbertruck.Config {
	return timbertruck.Config{
		WorkDir:                   workDir,
		LogCompletionDelay:        new(time.Duration),
		CompletedTaskRetainPeriod: new(time.Duration),
	}
}

func createTestStreamPipeline(ch chan<- string) timbertruck.NewPipelineFunc {
	return func(task timbertruck.TaskArgs) (pipeline *pipelines.Pipeline, err error) {
		task.Controller.Logger().Debug("Creating text pipeline", "position", task.Position)
		pipeline, stream, err := pipelines.NewTextPipeline(task.Controller.Logger(), task.Path, task.Position, pipelines.TextPipelineOptions{
			LineLimit:   64,
			BufferLimit: 1024,
			OnTruncatedRow: func(data io.WriterTo, _ pipelines.SkippedRowInfo) {
				_, _ = data.WriteTo(io.Discard)
			},
		})
		if err != nil {
			return
		}
		pipelines.ApplyOutputFunc(func(_ context.Context, meta pipelines.RowMeta, line pipelines.TextLine) {
			task.Controller.NotifyProgress(meta.End)
			ch <- string(line.Bytes)
			task.Controller.Logger().Debug("Emitted line", "position", meta.End)
		}, stream)
		return
	}
}

func readChan(t *testing.T, ch chan string, size int, timeout time.Duration) (result []string) {
	t.Helper()
	timer := time.NewTimer(timeout)
loop:
	for i := 0; i < size; i++ {
		select {
		case cur, ok := <-ch:
			if !ok {
				break loop
			}
			result = append(result, cur)
		case <-timer.C:
			t.Fatal("Timeout waiting data in channel")
		}
	}
	return result
}

func debugLogger() *slog.Logger {
	var lvl slog.LevelVar
	lvl.Set(slog.LevelDebug)
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: &lvl,
	}))

}

func TestSmoke(t *testing.T) {
	workingDir := path.Join(t.TempDir(), "timbertruck_work")

	err := os.MkdirAll(workingDir, 0755)
	require.NoError(t, err)

	config := newTestConfig(workingDir)
	tt, err := timbertruck.NewTimberTruck(config, debugLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveEndChan := make(chan error)
	go func() {
		err := tt.Serve(ctx)
		serveEndChan <- err
	}()

	cancel()

	err = <-serveEndChan
	require.ErrorIs(t, err, context.Canceled)
}

func TestEmptyServing(t *testing.T) {
	workingDir := path.Join(t.TempDir(), "timbertruck_work")
	stream1Dir := path.Join(t.TempDir(), "stream1_logs")

	err := os.MkdirAll(workingDir, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(stream1Dir, 0755)
	require.NoError(t, err)

	config := newTestConfig(workingDir)
	tt, err := timbertruck.NewTimberTruck(config, debugLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream1Chan := make(chan string, 32)

	tt.AddStream(
		timbertruck.StreamConfig{
			Name:    "stream1",
			LogFile: path.Join(stream1Dir, "log"),
		},
		createTestStreamPipeline(stream1Chan),
	)

	serveEndChan := make(chan error)
	go func() {
		err := tt.Serve(ctx)
		serveEndChan <- err
	}()

	cancel()

	err = <-serveEndChan
	require.ErrorIs(t, err, context.Canceled)
}

func TestSimpleServe(t *testing.T) {
	workingDir := path.Join(t.TempDir(), "timbertruck_work")
	stream1Dir := path.Join(t.TempDir(), "stream1_logs")

	err := os.MkdirAll(workingDir, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(stream1Dir, 0755)
	require.NoError(t, err)

	config := newTestConfig(workingDir)
	tt, err := timbertruck.NewTimberTruck(config, debugLogger(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream1Chan := make(chan string, 32)

	logFile := path.Join(stream1Dir, "log")
	tt.AddStream(
		timbertruck.StreamConfig{
			Name:    "stream1",
			LogFile: logFile,
		},
		createTestStreamPipeline(stream1Chan),
	)

	serveEndChan := make(chan error)
	go func() {
		err := tt.Serve(ctx)
		serveEndChan <- err
	}()

	err = os.WriteFile(logFile, []byte("foo\nbar"), 0644)
	require.NoError(t, err)

	require.Equal(t, []string{"foo\n"}, readChan(t, stream1Chan, 1, 5*time.Second))

	err = os.Rename(logFile, logFile+".1")
	require.NoError(t, err)
	require.Equal(t, []string{"bar"}, readChan(t, stream1Chan, 1, 5*time.Second))

	err = os.WriteFile(logFile, []byte("baz\n"), 0644)
	require.NoError(t, err)
	require.Equal(t, []string{"baz\n"}, readChan(t, stream1Chan, 1, 5*time.Second))

	cancel()

	err = <-serveEndChan
	require.ErrorIs(t, err, context.Canceled)
}

func TestSimpleRestartServer(t *testing.T) {
	workingDir := path.Join(t.TempDir(), "timbertruck_work")
	stream1Dir := path.Join(t.TempDir(), "stream1_logs")

	err := os.MkdirAll(workingDir, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(stream1Dir, 0755)
	require.NoError(t, err)

	logFile := path.Join(stream1Dir, "log")
	stream1Chan := make(chan string, 32)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	newTimberTruck := func() *timbertruck.TimberTruck {
		config := newTestConfig(workingDir)
		tt, err := timbertruck.NewTimberTruck(config, debugLogger(), nil)
		require.NoError(t, err)

		tt.AddStream(
			timbertruck.StreamConfig{
				Name:    "stream1",
				LogFile: logFile,
			},
			createTestStreamPipeline(stream1Chan),
		)
		return tt
	}

	serveEndChan := make(chan error)
	tt := newTimberTruck()
	go func() {
		err := tt.Serve(ctx)
		serveEndChan <- err
	}()

	f, err := os.Create(logFile)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write([]byte("foo\nbar"))
	require.NoError(t, err)

	require.Equal(t, []string{"foo\n"}, readChan(t, stream1Chan, 1, 5*time.Second))

	cancel()
	err = <-serveEndChan
	require.ErrorIs(t, err, context.Canceled)

	tt = newTimberTruck()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		err := tt.Serve(ctx)
		serveEndChan <- err
	}()
	_, err = f.Write([]byte("\n"))
	require.NoError(t, err)
	require.Equal(t, []string{"bar\n"}, readChan(t, stream1Chan, 1, 5*time.Second))
}

func TestMoveWhenStopped(t *testing.T) {
	workingDir := path.Join(t.TempDir(), "timbertruck_work")
	stream1Dir := path.Join(t.TempDir(), "stream1_logs")

	err := os.MkdirAll(workingDir, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(stream1Dir, 0755)
	require.NoError(t, err)

	logFile := path.Join(stream1Dir, "log")
	stream1Chan := make(chan string, 32)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	newTimberTruck := func() *timbertruck.TimberTruck {
		config := newTestConfig(workingDir)
		tt, err := timbertruck.NewTimberTruck(config, debugLogger(), nil)
		require.NoError(t, err)

		tt.AddStream(
			timbertruck.StreamConfig{
				Name:    "stream1",
				LogFile: logFile,
			},
			createTestStreamPipeline(stream1Chan),
		)
		return tt
	}

	serveEndChan := make(chan error)
	tt := newTimberTruck()
	go func() {
		err := tt.Serve(ctx)
		serveEndChan <- err
	}()

	func() {
		f, err := os.Create(logFile)
		require.NoError(t, err)
		defer f.Close()
		_, err = f.Write([]byte("foo\nbar"))
		require.NoError(t, err)

		require.Equal(t, []string{"foo\n"}, readChan(t, stream1Chan, 1, 5*time.Second))

		cancel()
		err = <-serveEndChan
		require.ErrorIs(t, err, context.Canceled)
	}()

	err = os.Rename(logFile, logFile+".1")
	require.NoError(t, err)
	f, err := os.Create(logFile)
	require.NoError(t, err)

	tt = newTimberTruck()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		err := tt.Serve(ctx)
		serveEndChan <- err
	}()
	_, err = f.Write([]byte("\n"))
	require.NoError(t, err)
	require.Equal(t, []string{"bar"}, readChan(t, stream1Chan, 1, 5*time.Second))
	require.Equal(t, []string{"\n"}, readChan(t, stream1Chan, 1, 5*time.Second))
}
