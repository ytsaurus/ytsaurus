package integration

import (
	"testing"

	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

func TestOutputTableCreation(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	inputPath := env.TmpPath()
	outputPath := env.TmpPath()

	input := []TestRow{
		{A: 2, B: "bar"},
	}
	require.NoError(t, env.UploadSlice(inputPath, input))

	op, err := env.MR.MapReduce(&MapJob{"test-map"}, &ReduceJob{"test-reduce"},
		spec.MapReduce().
			ReduceByColumns("a").
			AddInput(inputPath).
			AddOutput(outputPath).
			AddSecureVaultVar("TEST", "FOO"))
	require.NoError(t, err)
	require.NoError(t, op.Wait())

	var output []interface{}
	require.NoError(t, env.DownloadSlice(outputPath, &output))
	require.Empty(t, output)
}
