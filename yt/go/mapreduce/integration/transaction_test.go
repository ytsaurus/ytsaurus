package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/mapreduce/spec"
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
	require.NoError(t, env.UploadSlice(inputPath, input))

	tx, err := env.YT.BeginTx(env.Ctx, nil)
	require.NoError(t, err)

	mr := env.MR.WithTx(tx)

	op, err := mr.MapReduce(&MapJob{"test-map"}, &ReduceJob{"test-reduce"},
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

	require.NoError(t, tx.Commit())
}
