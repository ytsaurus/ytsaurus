package pipelines_test

import (
	"context"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

func TestFollowingPipelines(t *testing.T) {
	tempDir := t.TempDir()
	filepath := path.Join(tempDir, "logfile")

	f, err := os.Create(filepath)
	require.NoError(t, err)

	p, s, err := pipelines.NewTextPipeline(filepath, pipelines.FilePosition{}, pipelines.TextPipelineOptions{
		LineLimit:   8,
		BufferLimit: 64,
	})
	require.NoError(t, err)

	type StringLine struct {
		String      string
		BeginOffset int64
		EndOffset   int64
		Truncated   bool
	}

	lineCh := make(chan StringLine, 128)
	receive := func(line *StringLine) bool {
		select {
		case <-time.After(500 * time.Millisecond):
			return false
		case *line = <-lineCh:
			return true
		}
	}

	pipelines.ApplyOutputFunc(func(ctx context.Context, meta pipelines.RowMeta, line pipelines.TextLine) {
		lineCh <- StringLine{
			String:      string(line.Bytes),
			BeginOffset: meta.Begin.LogicalOffset,
			EndOffset:   meta.End.LogicalOffset,
			Truncated:   line.Truncated,
		}
	}, s)

	runComplete := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		runComplete <- p.Run(ctx)
		close(runComplete)
	}()

	currentLine := StringLine{}
	require.False(t, receive(&currentLine))

	_, err = io.WriteString(f, "foo")
	require.NoError(t, err)
	require.False(t, receive(&currentLine))

	_, err = io.WriteString(f, "bar\n")
	require.NoError(t, err)
	require.True(t, receive(&currentLine))
	require.Equal(t, StringLine{"foobar\n", 0, 7, false}, currentLine)

	_, err = io.WriteString(f, strings.Repeat("a", 100))
	require.NoError(t, err)
	require.True(t, receive(&currentLine))
	require.Equal(t, currentLine, StringLine{strings.Repeat("a", 8), 7, 15, true})

	_, err = io.WriteString(f, "\nlastline")
	require.NoError(t, err)
	require.False(t, receive(&currentLine))

	p.NotifyComplete()
	require.True(t, receive(&currentLine))
	require.Equal(t, StringLine{"lastline", 108, 116, false}, currentLine)

	err = <-runComplete
	require.NoError(t, err)
}
