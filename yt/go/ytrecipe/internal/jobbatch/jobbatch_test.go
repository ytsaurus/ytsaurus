package jobbatch

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

type TestJob struct {
	mapreduce.Untyped
}

func (t *TestJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	return nil
}

func init() {
	mapreduce.Register(&TestJob{})
}

func TestJobBatching(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	jobTablePath := env.TmpPath()
	plan := map[ypath.Path]migrate.Table{jobTablePath: {Schema: JobSchema}}
	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, plan, migrate.OnConflictFail))

	testConfig := Config{
		TablePath: jobTablePath,
		GroupKey:  "foobar123",

		PollInterval:  time.Millisecond,
		BatchingDelay: time.Second,
		StartDeadline: time.Minute,
	}

	batcher := NewClient(env.YT, env.MR, env.L, testConfig)

	t.Run("RunSingle", func(t *testing.T) {
		meta := &SpecPart{}

		op, err := batcher.Schedule(env.Ctx, meta, &TestJob{})
		require.NoError(t, err)

		mrOp, err := env.MR.Track(op.ID)
		require.NoError(t, err)
		require.NoError(t, mrOp.Wait())
	})

	t.Run("ConcurrentScheduling", func(t *testing.T) {
		const N = 10

		opIDs := make(chan yt.OperationID, N)

		var wg sync.WaitGroup
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				meta := &SpecPart{}

				op, err := batcher.Schedule(env.Ctx, meta, &TestJob{})
				require.NoError(t, err)

				mrOp, err := env.MR.Track(op.ID)
				require.NoError(t, err)
				require.NoError(t, mrOp.Wait())

				opIDs <- op.ID
			}()
		}

		wg.Wait()

		close(opIDs)
		firstID := <-opIDs
		for otherID := range opIDs {
			require.Equal(t, firstID, otherID)
		}
	})
}

func TestMain(m *testing.M) {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	os.Exit(m.Run())
}
