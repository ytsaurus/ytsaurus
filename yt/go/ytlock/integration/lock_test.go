package integration

import (
	"context"
	"testing"

	"a.yandex-team.ru/yt/go/yt"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ytlock"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestWithLock(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	t.Run("SuccessfullLock", func(t *testing.T) {
		options := ytlock.Options{
			LockPath: env.TmpPath(),
		}

		for i := 0; i < 2; i++ {
			err := ytlock.With(env.Ctx, env.YT, options, func(ctx context.Context) error {
				return nil
			})
			require.NoError(t, err)
		}
	})

	t.Run("LockConflict", func(t *testing.T) {
		options := ytlock.Options{LockPath: env.TmpPath(), OnConflict: ytlock.Fail}

		lockAcquired := make(chan struct{})
		holdLock := make(chan struct{})

		go func() {
			err := ytlock.With(env.Ctx, env.YT, options, func(ctx context.Context) error {
				close(lockAcquired)
				<-holdLock
				return nil
			})
			require.NoError(t, err)
		}()

		<-lockAcquired
		err := ytlock.With(env.Ctx, env.YT, options, func(ctx context.Context) error {
			close(lockAcquired)
			<-holdLock
			return nil
		})
		require.Error(t, err)

		require.True(t, yt.ContainsErrorCode(err, yt.CodeConcurrentTransactionLockConflict), "%+v", err)

		winner := ytlock.FindConflictWinner(err)
		require.NotNil(t, winner)
	})
}
