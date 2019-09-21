package integration

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

func DumpFDs() {
	const procSelfFD = "/proc/self/fd"
	files, err := ioutil.ReadDir(procSelfFD)
	if err != nil {
		return
	}

	for _, f := range files {
		dst, err := os.Readlink(filepath.Join(procSelfFD, f.Name()))
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", f.Name(), err)
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "%s -> %s\n", f.Name(), dst)
		}
	}
}

type TestRow struct {
	A int    `yson:"a,key"`
	B string `yson:"b"`
}

func init() {
	mapreduce.Register(&CatJob{})
}

type CatJob struct{}

func (*CatJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) (err error) {
	if len(out) != 1 {
		DumpFDs()
		panic(fmt.Sprintf("unexpected input table count: %d != 1", len(out)))
	}

	var row TestRow
	for in.Next() {
		if err = in.Scan(&row); err != nil {
			return
		}

		if err = out[0].Write(row); err != nil {
			return
		}
	}

	return
}

func TestMap(t *testing.T) {
	t.Parallel()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	job := &CatJob{}

	inputPath := env.TmpPath()
	outputPath := env.TmpPath()

	input := []TestRow{
		{A: 1, B: "foo"},
		{A: 2, B: "bar"},
	}
	require.NoError(t, env.UploadSlice(inputPath, input))

	_, err := yt.CreateTable(env.Ctx, env.YT, outputPath)
	require.NoError(t, err)

	op, err := env.MR.Map(job, spec.Map().AddInput(inputPath).AddOutput(outputPath))

	require.NoError(t, err)
	require.NoError(t, op.Wait())

	var output []TestRow
	require.NoError(t, env.DownloadSlice(outputPath, &output))
	require.Equal(t, input, output)
}
