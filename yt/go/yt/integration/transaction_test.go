package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/xerrors"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestTransactions(t *testing.T) {
	env := yttest.New(t)

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

		require.Error(t, tx.Commit())

		err = env.YT.PingTx(ctx, tx.ID(), nil)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
	})
}

func TestTransactionAbortCancel(t *testing.T) {
	env := yttest.New(t)

	ctx, cancel := context.WithCancel(env.Ctx)
	defer cancel()

	tx, err := env.YT.BeginTx(ctx, nil)
	require.NoError(t, err)

	go func() {
		<-tx.Finished()
		cancel()
	}()

	require.NoError(t, tx.Abort())
}

func TestNestedTransactions(t *testing.T) {
	env := yttest.New(t)

	rootTx, err := env.YT.BeginTx(env.Ctx, nil)
	require.NoError(t, err)

	nestedTx, err := rootTx.BeginTx(env.Ctx, nil)
	require.NoError(t, err)

	p := env.TmpPath()

	_, err = nestedTx.CreateNode(env.Ctx, p, yt.NodeMap, nil)
	require.NoError(t, err)

	ok, err := rootTx.NodeExists(env.Ctx, p, nil)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, nestedTx.Commit())

	ok, err = rootTx.NodeExists(env.Ctx, p, nil)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = env.YT.NodeExists(env.Ctx, p, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestExecTx_retries(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	wrapExecTx := func(ctx context.Context, cb func() error, opts yt.ExecTxRetryOptions) error {
		return yt.ExecTx(ctx, env.YT, func(ctx context.Context, tx yt.Tx) error {
			return cb()
		}, &yt.ExecTxOptions{RetryOptions: opts})
	}

	wrapExecTabletTx := func(ctx context.Context, cb func() error, opts yt.ExecTxRetryOptions) error {
		return yt.ExecTabletTx(env.Ctx, env.YT, func(ctx context.Context, tx yt.TabletTx) error {
			return cb()
		}, &yt.ExecTabletTxOptions{RetryOptions: opts})
	}

	wrappers := map[string]func(ctx context.Context, cb func() error, opts yt.ExecTxRetryOptions) error{
		"master": wrapExecTx,
		"tablet": wrapExecTabletTx,
	}

	for txType, execTx := range wrappers {
		t.Run(txType, func(t *testing.T) {
			t.Run("simple", func(t *testing.T) {
				v := 0
				err := execTx(env.Ctx, func() error {
					v++
					if v <= 2 {
						return xerrors.New("some error")
					}
					return nil
				}, nil)
				require.NoError(t, err)
				require.Equal(t, 1+2, v)
			})

			t.Run("default retry options", func(t *testing.T) {
				v := 0
				err := execTx(env.Ctx, func() error {
					v++
					return xerrors.New("some error")
				}, nil)
				require.Error(t, err)
				require.Equal(t, 1+yt.DefaultExecTxRetryCount, v)
			})

			t.Run("no retries", func(t *testing.T) {
				v := 0
				err := execTx(env.Ctx, func() error {
					v++
					return xerrors.New("some error")
				}, &yt.ExecTxRetryOptionsNone{})
				require.Error(t, err)
				require.Equal(t, 1, v)
			})

			t.Run("retry cancellation", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				v := 0
				err := execTx(ctx, func() error {
					v++
					return xerrors.New("some error")
				}, nil)
				require.Error(t, err)
				require.True(t, v >= 3)
			})
		})
	}
}
