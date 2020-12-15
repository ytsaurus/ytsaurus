package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yttest"
)

var jobNodePath = ypath.Path("//tmp/job_node")

func init() {
	mapreduce.Register(&SetCypressNodeJob{})
	mapreduce.Register(&RequestForbiddenJob{})
	mapreduce.Register(&ClientCaptureJob{})
}

type SetCypressNodeJob struct {
	mapreduce.Untyped

	Proxy string
}

func (s *SetCypressNodeJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	yc, err := ythttp.NewClient(&yt.Config{Proxy: s.Proxy, AllowRequestsFromJob: true})
	if err != nil {
		return err
	}

	return yc.SetNode(context.Background(), jobNodePath, 1, nil)
}

type RequestForbiddenJob struct {
	mapreduce.Untyped

	Proxy string
}

func (s *RequestForbiddenJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	_, err := ythttp.NewClient(&yt.Config{Proxy: s.Proxy})
	if err == nil {
		return fmt.Errorf("client allowed requests")
	}
	return nil
}

type ClientCaptureJob struct {
	mapreduce.Untyped

	YT yt.Client
}

func (s *ClientCaptureJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	panic("this should never be called")
}

func runVanilla(t *testing.T, env *yttest.Env, allow bool, job mapreduce.Job) error {
	s := spec.Vanilla().
		AddVanillaTask("test", 1)
	s.MaxFailedJobCount = 1

	op, err := env.MR.Vanilla(s, map[string]mapreduce.Job{"test": job})
	if err != nil {
		return err
	}

	return op.Wait()
}

func TestRequestsFromJob(t *testing.T) {
	t.Parallel()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	t.Run("SetCypressNode", func(t *testing.T) {
		require.NoError(t, runVanilla(t, env, true, &SetCypressNodeJob{Proxy: os.Getenv("YT_PROXY")}))

		var i int
		require.NoError(t, env.YT.GetNode(env.Ctx, jobNodePath, &i, nil))
		require.Equal(t, 1, i)
	})

	t.Run("Forbidden", func(t *testing.T) {
		require.NoError(t, runVanilla(t, env, false, &RequestForbiddenJob{Proxy: os.Getenv("YT_PROXY")}))
	})

	t.Run("ClientCapture", func(t *testing.T) {
		require.Error(t, runVanilla(t, env, true, &ClientCaptureJob{YT: env.YT}))
	})
}
