package integration

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/blobtable"
	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

func needRealCluster(t *testing.T) {
	if strings.Contains(os.Getenv("YT_PROXY"), "localhost") {
		t.Skipf("this test works only on real cluster")
	}
}

type CrashJob struct {
	mapreduce.Untyped
}

func (c *CrashJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	// NB(psushin): we should read the input to ensure that job initialization in job proxy
	// was completed. Otherwise stderr can remain uncaptured.
	for in.Next() {
		_, _ = fmt.Fprintf(os.Stderr, "Hello, World!\n")
	}

	debug.SetTraceback("crash")
	panic("dump core")
}

func init() {
	mapreduce.Register(&CrashJob{})
}

func TestStderrAndCoreTable(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	inputPath := env.TmpPath()
	outputPath := env.TmpPath()

	require.NoError(t, env.UploadSlice(inputPath, []struct {
		A int `yson:"a"`
	}{{A: 1}}))

	_, err := yt.CreateTable(env.Ctx, env.YT, outputPath)
	require.NoError(t, err)

	job := &CrashJob{}
	s := spec.Map().AddInput(inputPath).AddOutput(outputPath)

	s.MaxFailedJobCount = 1

	s.StderrTablePath = env.TmpPath()
	_, err = mapreduce.CreateStderrTable(env.Ctx, env.YT, s.StderrTablePath)
	require.NoError(t, err)

	s.CoreTablePath = env.TmpPath()
	_, err = mapreduce.CreateCoreTable(env.Ctx, env.YT, s.CoreTablePath)
	require.NoError(t, err)

	op, err := env.MR.Map(job, s)

	require.NoError(t, err)
	require.Error(t, op.Wait())

	t.Run("Stderr", func(t *testing.T) {
		br, err := blobtable.ReadBlobTable(env.Ctx, env.YT, s.StderrTablePath)
		require.NoError(t, err)
		defer br.Close()

		require.True(t, br.Next())

		stderr, err := io.ReadAll(br)
		require.NoError(t, err)

		require.True(t, bytes.HasPrefix(stderr, []byte("Hello, World!\n")), stderr)
		require.False(t, br.Next())
	})

	t.Run("Coredump", func(t *testing.T) {
		needRealCluster(t)

		br, err := blobtable.ReadBlobTable(env.Ctx, env.YT, s.CoreTablePath)
		require.NoError(t, err)
		defer br.Close()

		require.True(t, br.Next())

		coredump, err := io.ReadAll(br)
		require.NoError(t, err)

		require.NotEmpty(t, coredump)

		require.False(t, br.Next())
	})
}
