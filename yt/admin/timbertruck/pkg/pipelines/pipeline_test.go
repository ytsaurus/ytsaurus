package pipelines_test

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

func TestSimpleFilePipeline(t *testing.T) {
	workdir := t.TempDir()
	testData := "" +
		"first line\n" +
		"second line\n" +
		"third line\n"

	testFile := path.Join(workdir, "test")
	err := os.WriteFile(testFile, []byte(testData), 0644)
	require.NoError(t, err)

	p, lineBytesStream, err := pipelines.NewTextPipeline(testFile, pipelines.FilePosition{}, pipelines.TextPipelineOptions{
		LineLimit:   128,
		BufferLimit: 1024,
	})
	require.NoError(t, err)

	collectedStrings := []string{}
	collectedLenghts := []int{}

	pipelines.ApplyOutputFunc(func(ctx context.Context, meta pipelines.RowMeta, line pipelines.TextLine) {
		collectedLenghts = append(collectedLenghts, len(line.Bytes))
	}, lineBytesStream)

	lineStringStream := pipelines.ApplyMapFunc(func(line pipelines.TextLine) string {
		return "transformed " + string(line.Bytes)
	}, lineBytesStream)

	pipelines.ApplyOutputFunc(func(ctx context.Context, meta pipelines.RowMeta, s string) {
		collectedStrings = append(collectedStrings, s)
	}, lineStringStream)

	p.NotifyComplete()
	err = p.Run(context.Background())
	require.NoError(t, err)

	require.Equal(t, []int{
		11,
		12,
		11,
	}, collectedLenghts)

	require.Equal(t, []string{
		"transformed first line\n",
		"transformed second line\n",
		"transformed third line\n",
	}, collectedStrings)

}
