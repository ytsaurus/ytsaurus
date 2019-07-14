package integration

import (
	"testing"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

func init() {
	mapreduce.Register(&ReduceJob{})
}

type ReduceJob struct {
	Field string
}

func (r *ReduceJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	if r.Field != "test" {
		out[0].MustWrite(struct{ Error string }{"job field is not set"})
	}

	value, ok := ctx.LookupVault("TEST")
	if !ok || value != "FOO" {
		out[0].MustWrite(struct{ Error string }{"secure vault variable is missing"})
	}

	return nil
}

func TestJobState(t *testing.T) {
	t.SkipNow()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	inputPath := env.TmpPath()
	outputPath := env.TmpPath()

	input := []TestRow{
		{A: 2, B: "bar"},
	}
	require.NoError(t, env.UploadSlice(inputPath, input))

	_, err := yt.CreateTable(env.Ctx, env.YT, outputPath)
	require.NoError(t, err)

	op, err := env.MR.Reduce(&ReduceJob{"test"},
		spec.Reduce().
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
