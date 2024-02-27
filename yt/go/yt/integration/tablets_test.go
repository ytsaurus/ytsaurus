package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"go.ytsaurus.tech/library/go/core/log"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestGenerateTimestamp(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "GenerateTimestamp", Test: suite.TestGenerateTimestamp},
	})
}

func (s *Suite) TestGenerateTimestamp(t *testing.T, yc yt.Client) {
	ts, err := yc.GenerateTimestamp(s.Ctx, nil)
	require.NoError(t, err)
	require.NotZero(t, ts)
}

func TestTabletClient(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "TabletTx", Test: suite.TestTabletTx},
		{Name: "TabletTxDuration", Test: suite.TestTabletTxDuration},
		{Name: "ExecTabletTx", Test: suite.TestExecTabletTx},
		{Name: "LookupColumnFilter", Test: suite.TestLookupColumnFilter},
		{Name: "ReadTimestamp", Test: suite.TestReadTimestamp},
		{Name: "InsertRows_map", Test: suite.TestInsertRows_map},
		{Name: "InsertRows_empty", Test: suite.TestInsertRows_empty},
		{Name: "DeleteRows_empty", Test: suite.TestDeleteRows_empty},
		{Name: "InsertRowsBatch", Test: suite.TestInsertRowsBatch},
		{Name: "LookupRows_map", Test: suite.TestLookupRows_map, SkipRPC: true}, // todo https://st.yandex-team.ru/YT-15505
		{Name: "SelectRowsWithPlaceholders", Test: suite.SelectRowsWithPlaceholders},
	})
}

func (s *Suite) TestTabletTx(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	keys := []any{
		&testKey{"bar"},
		&testKey{"foo"},
		&testKey{"baz"},
	}

	rows := []any{
		&testRow{"bar", "2"},
		&testRow{"foo", "1"},
	}

	tx, err := yc.BeginTabletTx(s.Ctx, nil)
	require.NoError(t, err)

	err = tx.InsertRows(s.Ctx, testTable, rows, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	r, err := yc.LookupRows(s.Ctx, testTable, keys, nil)
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

	r, err = yc.LookupRows(s.Ctx, testTable, keys, &yt.LookupRowsOptions{KeepMissingRows: true})
	require.NoError(t, err)
	checkResult(r, true)

	r, err = yc.SelectRows(s.Ctx, fmt.Sprintf("* from [%s]", testTable), nil)
	require.NoError(t, err)
	checkResult(r, false)

	tx, err = yc.BeginTabletTx(s.Ctx, nil)
	require.NoError(t, err)

	err = tx.DeleteRows(s.Ctx, testTable, keys, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	r, err = yc.LookupRows(s.Ctx, testTable, keys, nil)
	require.NoError(t, err)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) TestTabletTxDuration(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{
		&testRow{"bar", "2"},
		&testRow{"foo", "1"},
	}

	tx, err := yc.BeginTabletTx(s.Ctx, nil)
	require.NoError(t, err)

	time.Sleep(time.Second * 20)

	require.NoError(t, tx.InsertRows(s.Ctx, testTable, rows, nil))
	require.NoError(t, tx.Commit())
}

func (s *Suite) TestExecTabletTx(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{&testRow{"foo", "1"}}
	keys := []any{&testKey{"foo"}}

	err := yt.ExecTabletTx(s.Ctx, yc, func(ctx context.Context, tx yt.TabletTx) error {
		return tx.InsertRows(s.Ctx, testTable, rows, nil)
	}, nil)
	require.NoError(t, err)

	r, err := yc.LookupRows(s.Ctx, testTable, keys, nil)
	require.NoError(t, err)

	var res testRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&res))
	assert.Equal(t, rows[0], &res)

	require.False(t, r.Next())
	require.NoError(t, r.Err())

	var mu sync.Mutex
	mu.Lock()
}

func (s *Suite) TestLookupColumnFilter(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	schema := schema.MustInfer(&testRowWithTwoColumns{})
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{&testRowWithTwoColumns{"foo", "1", "2"}}
	keys := []any{&testKey{"foo"}}

	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	readRow := func(r yt.TableReader) (row testRowWithTwoColumns) {
		defer r.Close()

		require.True(t, r.Next())
		require.NoError(t, r.Scan(&row))

		require.False(t, r.Next())
		require.NoError(t, r.Err())
		return
	}

	r, err := yc.LookupRows(s.Ctx, testTable, keys, nil)
	require.NoError(t, err)
	require.Equal(t, readRow(r), testRowWithTwoColumns{"foo", "1", "2"})

	r, err = yc.LookupRows(s.Ctx, testTable, keys, &yt.LookupRowsOptions{
		Columns: []string{"table_key", "value0"},
	})
	require.NoError(t, err)
	require.Equal(t, readRow(r), testRowWithTwoColumns{Key: "foo", Value0: "1"})
}

func (s *Suite) TestReadTimestamp(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{&testRow{"foo", "1"}}
	keys := []any{&testKey{"foo"}}

	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	ts, err := yc.GenerateTimestamp(s.Ctx, nil)
	_ = ts
	require.NoError(t, err)

	require.NoError(t, yc.DeleteRows(s.Ctx, testTable, keys, nil))

	checkReader := func(r yt.TableReader) {
		require.True(t, r.Next())

		var row testRow
		require.NoError(t, r.Scan(&row))
		require.Equal(t, &row, rows[0])

		require.False(t, r.Next())
		require.NoError(t, r.Err())
	}

	r, err := yc.LookupRows(s.Ctx, testTable, keys, &yt.LookupRowsOptions{Timestamp: &ts})
	require.NoError(t, err)
	defer r.Close()
	checkReader(r)

	r, err = yc.SelectRows(s.Ctx, fmt.Sprintf("* from [%s]", testTable), &yt.SelectRowsOptions{
		Timestamp: &ts,
	})
	require.NoError(t, err)
	defer r.Close()
	checkReader(r)
}

func (s *Suite) TestInsertRows_map(t *testing.T, yc yt.Client) {
	t.Parallel()

	type testRow struct {
		Key   string `yson:"table_key,key"`
		Value string `yson:"value,omitempty"`
	}

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{
		map[string]any{"table_key": "foo", "value": nil},
	}
	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	keys := []any{
		map[string]any{"table_key": "foo"},
	}

	r, err := yc.LookupRows(s.Ctx, testTable, keys, nil)
	require.NoError(t, err)
	defer r.Close()

	var row testRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, testRow{Key: "foo", Value: ""}, row)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) TestLookupRows_map(t *testing.T, yc yt.Client) {
	t.Parallel()

	type testRow struct {
		Key   string `yson:"table_key,key"`
		Value string `yson:"value,omitempty"`
	}

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{
		map[string]any{"table_key": "foo", "value": nil},
	}
	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	keys := []any{
		map[string]any{"table_key": "foo"},
	}

	r, err := yc.LookupRows(s.Ctx, testTable, keys, nil)
	require.NoError(t, err)
	defer r.Close()

	row := make(map[string]any)
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, map[string]any{"table_key": "foo", "value": nil}, row)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) SelectRowsWithPlaceholders(t *testing.T, yc yt.Client) {
	t.Parallel()

	type testRow struct {
		A int64  `yson:"a,key"`
		B int64  `yson:"b"`
		C int64  `yson:"c"`
		D string `yson:"d"`
	}

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{
		&testRow{A: 1, B: 0, C: 1, D: "a"},
		&testRow{A: 2, B: 0, C: 5, D: "f"},
		&testRow{A: 3, B: 1, C: 3, D: "a"},
		&testRow{A: 4, B: 1, C: 1, D: "d"},
		&testRow{A: 5, B: 1, C: 3, D: "d"},
		&testRow{A: 6, B: 0, C: 1, D: "a"},
		&testRow{A: 7, B: 0, C: 1, D: "a"},
		&testRow{A: 8, B: 1, C: 5, D: "f"},
	}
	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	type requestData struct {
		Query             string
		PlaceholderValues any
	}

	requests := []requestData{
		{
			Query: `
			       a, b, c, d
			       from [%s]
			       where b = {first} and (c, d) > {second}
			       order by a
			       limit 3
			`,
			PlaceholderValues: struct {
				First  int   `yson:"first"`
				Second []any `yson:"second"`
			}{
				First:  1,
				Second: []any{2, "b"},
			},
		},
		{
			Query: `
			       a, b, c, d
			       from [%s]
			       where b = {first} and (c, d) > {second}
			       order by a
			       limit 3
			`,
			PlaceholderValues: map[string]any{
				"first":  1,
				"second": []any{2, "b"},
			},
		},
		{
			Query: `
			       a, b, c, d
			       from [%s]
			       where b = {first} and (c, d) > ({second}, {third})
			       order by a
			       limit 3
			`,
			PlaceholderValues: struct {
				First  int    `yson:"first"`
				Second int    `yson:"second"`
				Third  string `yson:"third"`
			}{
				First:  1,
				Second: 2,
				Third:  "b",
			},
		},
		{
			Query: `
			       a, b, c, d
			       from [%s]
			       where b = {first} and (c, d) > ({second}, {third})
			       order by a
			       limit 3
			`,
			PlaceholderValues: map[string]any{
				"first":  1,
				"second": 2,
				"third":  "b",
			},
		},
	}

	checkResult := func(r yt.TableReader) {
		expectedRows := []testRow{
			{A: 3, B: 1, C: 3, D: "a"},
			{A: 5, B: 1, C: 3, D: "d"},
			{A: 8, B: 1, C: 5, D: "f"},
		}

		var row testRow

		for _, expectedRow := range expectedRows {
			require.True(t, r.Next())
			require.NoError(t, r.Scan(&row))
			assert.Equal(t, expectedRow, row)
		}

		require.False(t, r.Next())
		require.NoError(t, r.Err())
	}

	runRequest := func(request requestData) {
		options := &yt.SelectRowsOptions{
			PlaceholderValues: request.PlaceholderValues,
		}

		r, err := yc.SelectRows(s.Ctx, fmt.Sprintf(request.Query, testTable), options)
		require.NoError(t, err)
		defer r.Close()

		checkResult(r)
	}

	for _, request := range requests {
		runRequest(request)
	}
}

func (s *Suite) TestInsertRows_empty(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	rows := []any{}
	require.NoError(t, yc.InsertRows(s.Ctx, testTable, rows, nil))

	bw := yc.NewRowBatchWriter()
	require.NoError(t, bw.Commit())
	require.NoError(t, yc.InsertRowBatch(s.Ctx, testTable, bw.Batch(), nil))
}

func (s *Suite) TestDeleteRows_empty(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	keys := []any{&testKey{"foo"}}
	require.NoError(t, yc.DeleteRows(s.Ctx, testTable, keys, nil))
}

func (s *Suite) TestInsertRowsBatch(t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(s.Ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(s.Ctx, yc, testTable))

	bw := yc.NewRowBatchWriter()
	require.NoError(t, bw.Write(testRow{"a", "b"}))
	require.NoError(t, bw.Write(testRow{"c", "d"}))
	require.NoError(t, bw.Commit())

	require.NoError(t, yc.InsertRowBatch(s.Ctx, testTable, bw.Batch(), nil))

	keys := []any{
		&testKey{"a"},
		&testKey{"c"},
	}

	r, err := yc.LookupRows(s.Ctx, testTable, keys, nil)
	require.NoError(t, err)
	defer r.Close()

	var row testRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, row, testRow{"a", "b"})

	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, row, testRow{"c", "d"})

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

type testKey struct {
	Key string `yson:"table_key"`
}

type testRow struct {
	Key   string `yson:"table_key,key"`
	Value string `yson:"value"`
}

type testRowWithTwoColumns struct {
	Key    string `yson:"table_key,key"`
	Value0 string `yson:"value0"`
	Value1 string `yson:"value1"`
}

func TestAbortCommittedTabletTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func(t *testing.T, l log.Structured) (yt.Client, error)
	}{
		{name: "http", makeClient: func(t *testing.T, l log.Structured) (yt.Client, error) {
			return ythttp.NewTestClient(t, &yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
		}},
		{name: "rpc", makeClient: func(t *testing.T, l log.Structured) (yt.Client, error) {
			return ytrpc.NewTestClient(t, &yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, recorded := observer.New(zapcore.ErrorLevel)
			l := logzap.Logger{L: zap.New(core)}

			client, err := tc.makeClient(t, l.Structured())
			require.NoError(t, err)

			tx, err := client.BeginTabletTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			err = tx.Abort()
			require.Error(t, err)

			rows := []any{&testRow{"foo", "1"}}
			err = tx.InsertRows(ctx, "//tmp", rows, nil)
			require.Error(t, err)

			tx, err = client.BeginTabletTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, tx.Abort())

			err = tx.Commit()
			require.Error(t, err)

			err = tx.InsertRows(ctx, "//tmp", rows, nil)
			require.Error(t, err)

			require.Empty(t, recorded.All())
		})
	}
}

func TestLockRows(t *testing.T) { // todo rewrite as suite test after LockRows is implemented in rpc client
	t.Parallel()

	env := yttest.New(t)

	testTable := env.TmpPath().Child("table")

	sc := schema.MustInfer(&testRow{})
	sc.Columns[1].Lock = "lock"
	require.NoError(t, migrate.Create(env.Ctx, env.YT, testTable, sc))
	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, testTable))

	row := []any{&testRow{"foo", "1"}}
	key := []any{&testKey{"foo"}}

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
