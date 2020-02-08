package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

type testKey struct {
	Key string `yson:"table_key,key"`
}

type testRow struct {
	Key   string `yson:"table_key,key"`
	Value string `yson:"value"`
}

func TestTablets(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

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
