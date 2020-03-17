package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestRunningOperationInsideTx(t *testing.T) {
	t.Parallel()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	inputPath := env.TmpPath()
	outputPath := env.TmpPath()

	input := []TestRow{
		{A: 2, B: "bar"},
	}

	tx, err := env.YT.BeginTx(env.Ctx, nil)
	require.NoError(t, err)

	require.NoError(t, env.UploadSlice(ypath.Rich{Path: inputPath, TransactionID: tx.ID()}, input))

	mr := env.MR.WithTx(tx)

	op, err := mr.MapReduce(&MapJob{"test-map"}, &ReduceJob{Field: "test-reduce"},
		spec.MapReduce().
			ReduceByColumns("a").
			AddInput(inputPath).
			AddOutput(outputPath))
	require.NoError(t, err)
	require.NoError(t, op.Wait())

	op, err = mr.Sort(spec.Sort().
		SortByColumns("a").
		AddInput(outputPath).
		SetOutput(outputPath))
	require.NoError(t, err)
	require.NoError(t, op.Wait())

	var rowCount int
	require.NoError(t, tx.GetNode(env.Ctx, outputPath.Attr("row_count"), &rowCount, nil))
	require.Equal(t, 1, rowCount)

	require.NoError(t, tx.Abort())
	ok, err := env.YT.NodeExists(env.Ctx, outputPath, nil)
	require.NoError(t, err)
	require.False(t, ok)
}
