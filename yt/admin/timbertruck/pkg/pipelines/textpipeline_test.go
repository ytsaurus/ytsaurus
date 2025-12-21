package pipelines_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/zstdsync"
)

type stringLine struct {
	String      string
	BeginOffset pipelines.FilePosition
	EndOffset   pipelines.FilePosition
	Truncated   bool
}

func TestFollowingPipelines(t *testing.T) {
	tempDir := t.TempDir()
	filepath := path.Join(tempDir, "logfile")

	f, err := os.Create(filepath)
	require.NoError(t, err)

	p, lineCh := newTestTextPipeline(t, filepath, pipelines.FilePosition{})

	runComplete := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		runComplete <- p.Run(ctx)
		close(runComplete)
	}()

	currentLine := stringLine{}
	require.False(t, receive(&currentLine, lineCh))

	_, err = io.WriteString(f, "foo")
	require.NoError(t, err)
	require.False(t, receive(&currentLine, lineCh))

	_, err = io.WriteString(f, "bar\n")
	require.NoError(t, err)
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{"foobar\n", pipelines.UncompressedFilePosition(0), pipelines.UncompressedFilePosition(7), false}, currentLine)

	_, err = io.WriteString(f, "123456789\n")
	require.NoError(t, err)
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{"12345678", pipelines.UncompressedFilePosition(7), pipelines.UncompressedFilePosition(15), true}, currentLine)

	_, err = io.WriteString(f, strings.Repeat("a", 50))
	require.NoError(t, err)
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{strings.Repeat("a", 8), pipelines.UncompressedFilePosition(17), pipelines.UncompressedFilePosition(25), true}, currentLine)

	_, err = io.WriteString(f, strings.Repeat("a", 50))
	require.NoError(t, err)
	require.False(t, receive(&currentLine, lineCh))

	_, err = io.WriteString(f, "\nlastline")
	require.NoError(t, err)
	require.False(t, receive(&currentLine, lineCh))

	p.NotifyComplete()
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{"lastline", pipelines.UncompressedFilePosition(118), pipelines.UncompressedFilePosition(126), false}, currentLine)

	err = <-runComplete
	require.NoError(t, err)

	p2, lineCh2 := newTestTextPipeline(t, filepath, pipelines.UncompressedFilePosition(126))
	p2.NotifyComplete()

	runComplete2 := make(chan error)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	go func() {
		runComplete2 <- p2.Run(ctx2)
		close(runComplete2)
	}()

	require.False(t, receive(&currentLine, lineCh2))

	err = <-runComplete
	require.NoError(t, err)
}

type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) {
	return f(p)
}

func TestFollowingPipelinesWithCompression(t *testing.T) {
	tempDir := t.TempDir()
	filepath := path.Join(tempDir, "logfile.zst")

	f, err := os.Create(filepath)
	require.NoError(t, err)

	e, err := zstd.NewWriter(nil)
	require.NoError(t, err)

	fileSize := 0
	fileWriter := writerFunc(func(p []byte) (int, error) {
		n1, err := f.Write(e.EncodeAll(p, nil))
		fileSize += n1
		if err != nil {
			return n1, err
		}
		syncTag := zstdsync.WriteSyncTag(int64(fileSize))
		n2, err := f.Write(syncTag)
		fileSize += n2
		return n1 + n2, err
	})

	p, lineCh := newTestTextPipeline(t, filepath, pipelines.FilePosition{})

	runComplete := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		runComplete <- p.Run(ctx)
		close(runComplete)
	}()

	currentLine := stringLine{}
	require.False(t, receive(&currentLine, lineCh))

	_, err = io.WriteString(fileWriter, "foo")
	require.NoError(t, err)
	require.False(t, receive(&currentLine, lineCh))

	_, err = io.WriteString(fileWriter, "bar\n")
	require.NoError(t, err)
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{"foobar\n", pipelines.FilePosition{}, pipelines.FilePosition{LogicalOffset: 7, BlockPhysicalOffset: 48, InsideBlockOffset: 4}, false},
		currentLine,
	)

	_, err = io.WriteString(fileWriter, "123456789\n")
	require.NoError(t, err)
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{
			"12345678",
			pipelines.FilePosition{LogicalOffset: 7, BlockPhysicalOffset: 48, InsideBlockOffset: 4},
			pipelines.FilePosition{LogicalOffset: 15, BlockPhysicalOffset: 97, InsideBlockOffset: 8},
			true,
		}, currentLine)

	_, err = io.WriteString(fileWriter, strings.Repeat("a", 50))
	require.NoError(t, err)
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{
			strings.Repeat("a", 8),
			pipelines.FilePosition{LogicalOffset: 17, BlockPhysicalOffset: 97, InsideBlockOffset: 10},
			pipelines.FilePosition{LogicalOffset: 25, BlockPhysicalOffset: 152, InsideBlockOffset: 8},
			true,
		}, currentLine)

	_, err = io.WriteString(fileWriter, strings.Repeat("a", 50))
	require.NoError(t, err)
	require.False(t, receive(&currentLine, lineCh))

	_, err = io.WriteString(fileWriter, "\nlastline")
	require.NoError(t, err)
	require.False(t, receive(&currentLine, lineCh))

	p.NotifyComplete()
	require.True(t, receive(&currentLine, lineCh))
	require.Equal(t,
		stringLine{
			"lastline",
			pipelines.FilePosition{LogicalOffset: 118, BlockPhysicalOffset: 244, InsideBlockOffset: 1},
			pipelines.FilePosition{LogicalOffset: 126, BlockPhysicalOffset: 244, InsideBlockOffset: 9},
			false},
		currentLine,
	)

	err = <-runComplete
	require.NoError(t, err)

	p2, lineCh2 := newTestTextPipeline(t, filepath, pipelines.FilePosition{LogicalOffset: 126, BlockPhysicalOffset: 244, InsideBlockOffset: 9})
	p2.NotifyComplete()

	runComplete2 := make(chan error)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	go func() {
		runComplete2 <- p2.Run(ctx2)
		close(runComplete2)
	}()

	require.False(t, receive(&currentLine, lineCh2))

	err = <-runComplete
	require.NoError(t, err)
}

func newTestTextPipeline(t *testing.T, filepath string, filePosition pipelines.FilePosition) (*pipelines.Pipeline, <-chan stringLine) {
	t.Helper()

	p, s, err := pipelines.NewTextPipeline(slog.Default(), filepath, filePosition, pipelines.TextPipelineOptions{
		LineLimit:   8,
		BufferLimit: 64,
		OnTruncatedRow: func(data io.WriterTo, info pipelines.SkippedRowInfo) {
			_, _ = data.WriteTo(io.Discard)
		},
	})
	require.NoError(t, err)

	lineCh := make(chan stringLine, 128)

	pipelines.ApplyOutputFunc(func(ctx context.Context, meta pipelines.RowMeta, line pipelines.TextLine) {
		lineCh <- stringLine{
			String:      string(line.Bytes),
			BeginOffset: meta.Begin,
			EndOffset:   meta.End,
			Truncated:   line.Truncated,
		}
	}, s)

	return p, lineCh
}

func receive(line *stringLine, lineCh <-chan stringLine) bool {
	select {
	case <-time.After(500 * time.Millisecond):
		return false
	case *line = <-lineCh:
		return true
	}
}

func TestOversizedLineSkippedRowsContent(t *testing.T) {
	tempDir := t.TempDir()
	filepath := path.Join(tempDir, "logfile")

	longLine := strings.Repeat("x", 200) + "\n"
	shortLines := "short1\nshort2\nshort3\n"
	err := os.WriteFile(filepath, []byte(longLine+shortLines), 0644)
	require.NoError(t, err)

	var skippedData strings.Builder

	p, s, err := pipelines.NewTextPipeline(slog.Default(), filepath, pipelines.FilePosition{}, pipelines.TextPipelineOptions{
		LineLimit:   10, // Long line will be truncated.
		BufferLimit: 64, // Buffer is small, so long line won't fit.
		OnTruncatedRow: func(data io.WriterTo, info pipelines.SkippedRowInfo) {
			_, _ = data.WriteTo(&skippedData)
		},
	})
	require.NoError(t, err)

	lineCh := make(chan stringLine, 128)
	pipelines.ApplyOutputFunc(func(ctx context.Context, meta pipelines.RowMeta, line pipelines.TextLine) {
		lineCh <- stringLine{
			String:    string(line.Bytes),
			Truncated: line.Truncated,
		}
	}, s)

	p.NotifyComplete()
	ctx := context.Background()
	err = p.Run(ctx)
	require.NoError(t, err)

	close(lineCh)
	var outputLines []stringLine
	for line := range lineCh {
		outputLines = append(outputLines, line)
	}

	require.Len(t, outputLines, 4)
	require.True(t, outputLines[0].Truncated)
	require.False(t, outputLines[1].Truncated)
	require.False(t, outputLines[2].Truncated)
	require.False(t, outputLines[3].Truncated)

	skippedContent := skippedData.String()

	expectedSkipped := longLine
	require.Equal(t, expectedSkipped, skippedContent,
		"skipped_rows should contain only the truncated line, not subsequent lines")

	require.NotContains(t, skippedContent, "short1", "skipped_rows should not contain 'short1'")
	require.NotContains(t, skippedContent, "short2", "skipped_rows should not contain 'short2'")
	require.NotContains(t, skippedContent, "short3", "skipped_rows should not contain 'short3'")
}
