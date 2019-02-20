package integration

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"a.yandex-team.ru/yt/go/ypath"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

type TestRow struct {
	A int    `yt:"a"`
	B string `yt:"a"`
}

func init() {
	mapreduce.Register(&CatJob{})
}

type CatJob struct{}

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

func (_ *CatJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) (err error) {
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
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	inputPath := env.TmpPath()
	outputPath := env.TmpPath()

	require.NoError(t, env.UploadSlice(inputPath, []TestRow{
		{A: 1, B: "foo"},
		{A: 2, B: "bar"},
	}))

	_, err := env.YT.CreateNode(env.Ctx, outputPath, yt.NodeTable, nil)
	require.NoError(t, err)

	op, err := env.MR.Run(env.Ctx,
		mapreduce.Map(&CatJob{},
			&spec.Spec{
				InputTablePaths:  []ypath.Path{inputPath},
				OutputTablePaths: []ypath.Path{outputPath},
			},
		))

	require.NoError(t, err)
	require.NoError(t, op.Wait())
}

func TestMain(m *testing.M) {
	if mapreduce.InsideJob() {
		mapreduce.JobMain()
	}

	os.Exit(m.Run())
}
