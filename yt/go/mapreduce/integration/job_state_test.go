package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

func init() {
	mapreduce.Register(&ReduceJob{})
	mapreduce.Register(&MapJob{})
}

type MapJob struct {
	Field string
}

func (m *MapJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	if m.Field != "test-map" {
		panic("map job failed")
	}
	for in.Next() {
		var row TestRow
		in.MustScan(&row)
		out[0].MustWrite(&row)
	}
	return nil
}

type ReduceJob struct {
	Field string
}

func (r *ReduceJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	if r.Field != "test-reduce" {
		out[0].MustWrite(struct{ Error string }{"job field is not set"})
	}

	value, ok := ctx.LookupVault("TEST")
	if !ok || value != "FOO" {
		out[0].MustWrite(struct{ Error string }{"secure vault variable is missing"})
	}

	return nil
}

func TestJobState(t *testing.T) {
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

	op, err := env.MR.Reduce(&ReduceJob{"test-reduce"},
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

func TestMapReduceJobState(t *testing.T) {
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

type (
	TestFieldJob struct {
		Field string
	}

	TestSecondFieldJob struct {
		SecondField string
	}
)

func (j *TestFieldJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	if j.Field != "foo" {
		return xerrors.New("Job state is invalid")
	}

	return nil
}

func (j *TestSecondFieldJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	if j.SecondField != "bar" {
		return xerrors.New("Job state is invalid")
	}

	return nil
}

func init() {
	mapreduce.Register(&TestFieldJob{})
	mapreduce.Register(&TestSecondFieldJob{})
}

func TestVanilaJobState(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	s := spec.Vanilla().
		AddVanillaTask("test", 1).
		AddVanillaTask("second_test", 1)
	s.MaxFailedJobCount = 1

	op, err := env.MR.Vanilla(s, map[string]mapreduce.Job{
		"test":        &TestFieldJob{Field: "foo"},
		"second_test": &TestSecondFieldJob{SecondField: "bar"},
	})
	require.NoError(t, err)
	require.NoError(t, op.Wait())
}
