package integration

import (
	"context"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/yterrors"

	"a.yandex-team.ru/yt/go/yson"

	"a.yandex-team.ru/yt/go/yttest"

	"a.yandex-team.ru/yt/go/yt"

	"github.com/stretchr/testify/require"
)

func TestTransactions(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	t.Run("CommitTransaction", func(t *testing.T) {
		tx, err := env.YT.BeginTx(ctx, nil)
		require.NoError(t, err)

		name := tmpPath()
		_, err = tx.CreateNode(ctx, name, yt.NodeMap, nil)
		require.NoError(t, err)

		ok, err := env.YT.NodeExists(ctx, name, nil)
		require.NoError(t, err)
		require.False(t, ok)

		require.NoError(t, tx.Commit())

		ok, err = env.YT.NodeExists(ctx, name, nil)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		tx, err := env.YT.BeginTx(ctx, nil)
		require.NoError(t, err)

		require.NoError(t, tx.Abort())
	})

	t.Run("TransactionBackgroundPing", func(t *testing.T) {
		lowTimeout := yson.Duration(5 * time.Second)
		tx, err := env.YT.BeginTx(ctx, &yt.StartTxOptions{Timeout: &lowTimeout})
		require.NoError(t, err)

		time.Sleep(time.Second * 10)

		require.NoError(t, tx.Commit())
	})

	t.Run("TransactionAbortByContextCancel", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)

		tx, err := env.YT.BeginTx(canceledCtx, nil)
		require.NoError(t, err)

		cancel()
		time.Sleep(time.Second)

		require.Equal(t, yt.ErrTxAborted, tx.Commit())

		err = env.YT.PingTx(ctx, tx.ID(), nil)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
	})
}
