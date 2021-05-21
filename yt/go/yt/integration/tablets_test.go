package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

type testKey struct {
	Key string `yson:"table_key"`
}

type testRow struct {
	Key   string `yson:"table_key,key"`
	Value string `yson:"value"`
}

func TestTabletTx(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	testTable := env.TmpPath().Child("table")
	require.NoError(t, migrate.Create(env.Ctx, env.YT, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, testTable))

	keys := []interface{}{
		&testKey{"bar"},
		&testKey{"foo"},
		&testKey{"baz"},
	}

	rows := []interface{}{
		&testRow{"bar", "2"},
		&testRow{"foo", "1"},
	}

	tx, err := env.YT.BeginTabletTx(ctx, nil)
	require.NoError(t, err)

	err = tx.InsertRows(env.Ctx, testTable, rows, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	r, err := env.YT.LookupRows(env.Ctx, testTable, keys, nil)
	require.NoError(t, err)

	checkResult := func(r yt.TableReader, keepMissingRows bool) {
		var row testRow

		require.True(t, r.Next())
		require.NoError(t, r.Scan(&row))
		assert.Equal(t, rows[0], &row)

		require.True(t, r.Next())
		require.NoError(t, r.Scan(&row))
		assert.Equal(t, rows[1], &row)

		if keepMissingRows {
			require.True(t, r.Next())
			out := &testRow{}
			require.NoError(t, r.Scan(&out))
			assert.Nil(t, out)
		}

		require.False(t, r.Next())
		require.NoError(t, r.Err())
	}

	checkResult(r, false)

	r, err = env.YT.LookupRows(env.Ctx, testTable, keys, &yt.LookupRowsOptions{KeepMissingRows: true})
	require.NoError(t, err)
	checkResult(r, true)

	r, err = env.YT.SelectRows(env.Ctx, fmt.Sprintf("* from [%s]", testTable), nil)
	require.NoError(t, err)
	checkResult(r, false)

	tx, err = env.YT.BeginTabletTx(ctx, nil)
	require.NoError(t, err)

	err = tx.DeleteRows(env.Ctx, testTable, keys, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	r, err = env.YT.LookupRows(env.Ctx, testTable, keys, nil)
	require.NoError(t, err)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestAbortCommittedTx(t *testing.T) {
	t.Parallel()

	core, recorded := observer.New(zapcore.ErrorLevel)
	l := logzap.Logger{L: zap.New(core)}

	env := yttest.New(t, yttest.WithLogger(l.Structured()))

	tx, err := env.YT.BeginTabletTx(env.Ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	err = tx.Abort()
	require.Truef(t, errors.Is(err, yt.ErrTxCommitted), "%+v", err)

	err = tx.InsertRows(env.Ctx, "//tmp", []interface{}{}, nil)
	require.Truef(t, errors.Is(err, yt.ErrTxCommitted), "%+v", err)

	tx, err = env.YT.BeginTabletTx(env.Ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Abort())

	err = tx.Commit()
	require.Truef(t, errors.Is(err, yt.ErrTxAborted), "%+v", err)

	err = tx.InsertRows(env.Ctx, "//tmp", []interface{}{}, nil)
	require.Truef(t, errors.Is(err, yt.ErrTxAborted), "%+v", err)

	require.Empty(t, recorded.All())
}

func TestGenerateTimestamp(t *testing.T) {
	t.Parallel()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	ts, err := env.YT.GenerateTimestamp(env.Ctx, nil)
	require.NoError(t, err)
	require.NotZero(t, ts)
}

func TestTxDuration(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	testTable := env.TmpPath().Child("table")
	require.NoError(t, migrate.Create(env.Ctx, env.YT, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, testTable))

	rows := []interface{}{
		&testRow{"bar", "2"},
		&testRow{"foo", "1"},
	}

	tx, err := env.YT.BeginTabletTx(env.Ctx, nil)
	require.NoError(t, err)

	time.Sleep(time.Second * 20)

	require.NoError(t, tx.InsertRows(env.Ctx, testTable, rows, nil))
	require.NoError(t, tx.Commit())
}

func TestLockRows(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	testTable := env.TmpPath().Child("table")

	sc := schema.MustInfer(&testRow{})
	sc.Columns[1].Lock = "lock"
	require.NoError(t, migrate.Create(env.Ctx, env.YT, testTable, sc))
	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, testTable))

	row := []interface{}{&testRow{"foo", "1"}}
	key := []interface{}{&testKey{"foo"}}

	require.NoError(t, env.YT.InsertRows(env.Ctx, testTable, row, nil))

	tx0, err := env.YT.BeginTabletTx(env.Ctx, nil)
	require.NoError(t, err)

	tx1, err := env.YT.BeginTabletTx(env.Ctx, nil)
	require.NoError(t, err)

	require.NoError(t, tx1.InsertRows(env.Ctx, testTable, row, nil))
	require.NoError(t, tx1.Commit())

	require.NoError(t, tx0.LockRows(env.Ctx, testTable, []string{"lock"}, yt.LockTypeSharedStrong, key, nil))
	require.Error(t, tx0.Commit())
}

func TestExecTabletTx(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	testTable := env.TmpPath().Child("table")
	require.NoError(t, migrate.Create(env.Ctx, env.YT, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, testTable))

	rows := []interface{}{&testRow{"foo", "1"}}
	keys := []interface{}{&testKey{"foo"}}

	err := yt.ExecTabletTx(env.Ctx, env.YT, func(ctx context.Context, tx yt.TabletTx) error {
		return tx.InsertRows(ctx, testTable, rows, nil)
	}, nil)
	require.NoError(t, err)

	r, err := env.YT.LookupRows(env.Ctx, testTable, keys, nil)
	require.NoError(t, err)

	var res testRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&res))
	assert.Equal(t, rows[0], &res)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestReadTimestamp(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	testTable := env.TmpPath().Child("table")
	require.NoError(t, migrate.Create(env.Ctx, env.YT, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, testTable))

	rows := []interface{}{&testRow{"foo", "1"}}
	keys := []interface{}{&testKey{"foo"}}

	require.NoError(t, env.YT.InsertRows(env.Ctx, testTable, rows, nil))

	ts, err := env.YT.GenerateTimestamp(env.Ctx, nil)
	_ = ts
	require.NoError(t, err)

	require.NoError(t, env.YT.DeleteRows(env.Ctx, testTable, keys, nil))

	checkReader := func(r yt.TableReader) {
		require.True(t, r.Next())

		var row testRow
		require.NoError(t, r.Scan(&row))
		require.Equal(t, &row, rows[0])

		require.False(t, r.Next())
	}

	r, err := env.YT.LookupRows(env.Ctx, testTable, keys, &yt.LookupRowsOptions{Timestamp: &ts})
	require.NoError(t, err)
	defer r.Close()
	checkReader(r)

	r, err = env.YT.SelectRows(env.Ctx, fmt.Sprintf("* from [%s]", testTable), &yt.SelectRowsOptions{Timestamp: &ts})
	require.NoError(t, err)
	defer r.Close()
	checkReader(r)
}
