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
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestGenerateTimestamp(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "GenerateTimestamp", Test: suite.TestGenerateTimestamp},
	})
}

func (s *Suite) TestGenerateTimestamp(ctx context.Context, t *testing.T, yc yt.Client) {
	ts, err := yc.GenerateTimestamp(ctx, nil)
	require.NoError(t, err)
	require.NotZero(t, ts)
}

func TestTabletClient(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
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
		{Name: "MultiLookupRows_Basic", Test: suite.TestMultiLookupRows_Basic, SkipHTTP: true},
		{Name: "MultiLookupRows_WithKeepMissingRows", Test: suite.TestMultiLookupRows_WithKeepMissingRows, SkipHTTP: true},
		{Name: "MultiLookupRows_WithColumns", Test: suite.TestMultiLookupRows_WithColumns, SkipHTTP: true},
		{Name: "MultiLookupRows_WithTimestamp", Test: suite.TestMultiLookupRows_WithTimestamp, SkipHTTP: true},
		{Name: "MultiLookupRows_EmptySubrequests", Test: suite.TestMultiLookupRows_EmptySubrequests, SkipHTTP: true},
		{Name: "SelectRowsWithPlaceholders", Test: suite.SelectRowsWithPlaceholders},
	})
}

func (s *Suite) TestTabletTx(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	keys := []any{
		&testKey{"bar"},
		&testKey{"foo"},
		&testKey{"baz"},
	}

	rows := []any{
		&testRow{"bar", "2"},
		&testRow{"foo", "1"},
	}

	tx, err := yc.BeginTabletTx(ctx, nil)
	require.NoError(t, err)

	err = tx.InsertRows(ctx, testTable, rows, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	r, err := yc.LookupRows(ctx, testTable, keys, nil)
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

	r, err = yc.LookupRows(ctx, testTable, keys, &yt.LookupRowsOptions{KeepMissingRows: true})
	require.NoError(t, err)
	checkResult(r, true)

	r, err = yc.SelectRows(ctx, fmt.Sprintf("* from [%s]", testTable), nil)
	require.NoError(t, err)
	checkResult(r, false)

	tx, err = yc.BeginTabletTx(ctx, nil)
	require.NoError(t, err)

	err = tx.DeleteRows(ctx, testTable, keys, nil)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	r, err = yc.LookupRows(ctx, testTable, keys, nil)
	require.NoError(t, err)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) TestTabletTxDuration(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{
		&testRow{"bar", "2"},
		&testRow{"foo", "1"},
	}

	tx, err := yc.BeginTabletTx(ctx, nil)
	require.NoError(t, err)

	time.Sleep(time.Second * 20)

	require.NoError(t, tx.InsertRows(ctx, testTable, rows, nil))
	require.NoError(t, tx.Commit())
}

func (s *Suite) TestExecTabletTx(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{&testRow{"foo", "1"}}
	keys := []any{&testKey{"foo"}}

	err := yt.ExecTabletTx(ctx, yc, func(ctx context.Context, tx yt.TabletTx) error {
		return tx.InsertRows(ctx, testTable, rows, nil)
	}, nil)
	require.NoError(t, err)

	r, err := yc.LookupRows(ctx, testTable, keys, nil)
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

func (s *Suite) TestLookupColumnFilter(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	schema := schema.MustInfer(&testRowWithTwoColumns{})
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{&testRowWithTwoColumns{"foo", "1", "2"}}
	keys := []any{&testKey{"foo"}}

	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

	readRow := func(r yt.TableReader) (row testRowWithTwoColumns) {
		defer r.Close()

		require.True(t, r.Next())
		require.NoError(t, r.Scan(&row))

		require.False(t, r.Next())
		require.NoError(t, r.Err())
		return
	}

	r, err := yc.LookupRows(ctx, testTable, keys, nil)
	require.NoError(t, err)
	require.Equal(t, readRow(r), testRowWithTwoColumns{"foo", "1", "2"})

	r, err = yc.LookupRows(ctx, testTable, keys, &yt.LookupRowsOptions{
		Columns: []string{"table_key", "value0"},
	})
	require.NoError(t, err)
	require.Equal(t, readRow(r), testRowWithTwoColumns{Key: "foo", Value0: "1"})
}

func (s *Suite) TestReadTimestamp(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{&testRow{"foo", "1"}}
	keys := []any{&testKey{"foo"}}

	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

	ts, err := yc.GenerateTimestamp(ctx, nil)
	_ = ts
	require.NoError(t, err)

	require.NoError(t, yc.DeleteRows(ctx, testTable, keys, nil))

	checkReader := func(r yt.TableReader) {
		require.True(t, r.Next())

		var row testRow
		require.NoError(t, r.Scan(&row))
		require.Equal(t, &row, rows[0])

		require.False(t, r.Next())
		require.NoError(t, r.Err())
	}

	r, err := yc.LookupRows(ctx, testTable, keys, &yt.LookupRowsOptions{Timestamp: &ts})
	require.NoError(t, err)
	defer r.Close()
	checkReader(r)

	r, err = yc.SelectRows(ctx, fmt.Sprintf("* from [%s]", testTable), &yt.SelectRowsOptions{
		Timestamp: &ts,
	})
	require.NoError(t, err)
	defer r.Close()
	checkReader(r)
}

func (s *Suite) TestInsertRows_map(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type testRow struct {
		Key   string `yson:"table_key,key"`
		Value string `yson:"value,omitempty"`
	}

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{
		map[string]any{"table_key": "foo", "value": nil},
	}
	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

	keys := []any{
		map[string]any{"table_key": "foo"},
	}

	r, err := yc.LookupRows(ctx, testTable, keys, nil)
	require.NoError(t, err)
	defer r.Close()

	var row testRow
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, testRow{Key: "foo", Value: ""}, row)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) TestLookupRows_map(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type testRow struct {
		Key   string `yson:"table_key,key"`
		Value string `yson:"value,omitempty"`
	}

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{
		map[string]any{"table_key": "foo", "value": nil},
	}
	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

	keys := []any{
		map[string]any{"table_key": "foo"},
	}

	r, err := yc.LookupRows(ctx, testTable, keys, nil)
	require.NoError(t, err)
	defer r.Close()

	row := make(map[string]any)
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, map[string]any{"table_key": "foo", "value": nil}, row)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func (s *Suite) SelectRowsWithPlaceholders(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	type testRow struct {
		A int64  `yson:"a,key"`
		B int64  `yson:"b"`
		C int64  `yson:"c"`
		D string `yson:"d"`
	}

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

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
	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

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

		r, err := yc.SelectRows(ctx, fmt.Sprintf(request.Query, testTable), options)
		require.NoError(t, err)
		defer r.Close()

		checkResult(r)
	}

	for _, request := range requests {
		runRequest(request)
	}
}

func (s *Suite) TestInsertRows_empty(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	rows := []any{}
	require.NoError(t, yc.InsertRows(ctx, testTable, rows, nil))

	bw := yc.NewRowBatchWriter()
	require.NoError(t, bw.Commit())
	require.NoError(t, yc.InsertRowBatch(ctx, testTable, bw.Batch(), nil))
}

func (s *Suite) TestDeleteRows_empty(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	keys := []any{&testKey{"foo"}}
	require.NoError(t, yc.DeleteRows(ctx, testTable, keys, nil))
}

func (s *Suite) TestInsertRowsBatch(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	testTable := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, testTable, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, testTable))

	bw := yc.NewRowBatchWriter()
	require.NoError(t, bw.Write(testRow{"a", "b"}))
	require.NoError(t, bw.Write(testRow{"c", "d"}))
	require.NoError(t, bw.Commit())

	require.NoError(t, yc.InsertRowBatch(ctx, testTable, bw.Batch(), nil))

	keys := []any{
		&testKey{"a"},
		&testKey{"c"},
	}

	r, err := yc.LookupRows(ctx, testTable, keys, nil)
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
			ctx := ctxlog.WithFields(ctx, log.String("subtest_name", t.Name()))

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

func (s *Suite) TestMultiLookupRows_Basic(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	// Create two test tables
	table1 := tmpPath().Child("table1")
	table2 := tmpPath().Child("table2")

	require.NoError(t, migrate.Create(ctx, yc, table1, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.Create(ctx, yc, table2, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, table1))
	require.NoError(t, migrate.MountAndWait(ctx, yc, table2))

	// Insert data into both tables
	rows1 := []any{
		&testRow{"key1", "value1"},
		&testRow{"key2", "value2"},
	}
	rows2 := []any{
		&testRow{"key3", "value3"},
		&testRow{"key4", "value4"},
	}

	require.NoError(t, yc.InsertRows(ctx, table1, rows1, nil))
	require.NoError(t, yc.InsertRows(ctx, table2, rows2, nil))

	subrequests := []yt.MultiLookupSubrequest{
		{
			Path: table1,
			Keys: []any{
				&testKey{"key1"},
				&testKey{"key2"},
			},
		},
		{
			Path: table2,
			Keys: []any{
				&testKey{"key3"},
				&testKey{"key4"},
			},
		},
	}

	readers, err := yc.MultiLookupRows(ctx, subrequests, nil)
	require.NoError(t, err)
	require.Len(t, readers, 2)

	// Check first table results
	reader1 := readers[0]
	defer reader1.Close()

	var row testRow
	require.True(t, reader1.Next())
	require.NoError(t, reader1.Scan(&row))
	assert.Equal(t, testRow{"key1", "value1"}, row)

	require.True(t, reader1.Next())
	require.NoError(t, reader1.Scan(&row))
	assert.Equal(t, testRow{"key2", "value2"}, row)

	require.False(t, reader1.Next())
	require.NoError(t, reader1.Err())

	// Check second table results
	reader2 := readers[1]
	defer reader2.Close()

	require.True(t, reader2.Next())
	require.NoError(t, reader2.Scan(&row))
	assert.Equal(t, testRow{"key3", "value3"}, row)

	require.True(t, reader2.Next())
	require.NoError(t, reader2.Scan(&row))
	assert.Equal(t, testRow{"key4", "value4"}, row)

	require.False(t, reader2.Next())
	require.NoError(t, reader2.Err())
}

func (s *Suite) TestMultiLookupRows_WithKeepMissingRows(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	table := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, table, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, table))

	// Insert only one row
	rows := []any{&testRow{"existing", "value"}}
	require.NoError(t, yc.InsertRows(ctx, table, rows, nil))

	subrequests := []yt.MultiLookupSubrequest{
		{
			Path:            table,
			KeepMissingRows: ptr.Bool(true),
			Keys: []any{
				&testKey{"existing"},
				&testKey{"missing"},
			},
		},
	}

	readers, err := yc.MultiLookupRows(ctx, subrequests, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)

	reader := readers[0]
	defer reader.Close()

	// First row should exist
	var row testRow
	require.True(t, reader.Next())
	require.NoError(t, reader.Scan(&row))
	assert.Equal(t, testRow{"existing", "value"}, row)

	// Second row should be nil (missing)
	require.True(t, reader.Next())
	out := &testRow{}
	require.NoError(t, reader.Scan(&out))
	assert.Nil(t, out)

	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

func (s *Suite) TestMultiLookupRows_WithColumns(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	table := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, table, schema.MustInfer(&testRowWithTwoColumns{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, table))

	rows := []any{&testRowWithTwoColumns{"key1", "val0", "val1"}}
	require.NoError(t, yc.InsertRows(ctx, table, rows, nil))

	subrequests := []yt.MultiLookupSubrequest{
		{
			Path:    table,
			Columns: []string{"table_key", "value0"}, // Only select specific columns
			Keys: []any{
				&testKey{"key1"},
			},
		},
	}

	readers, err := yc.MultiLookupRows(ctx, subrequests, nil)
	require.NoError(t, err)
	require.Len(t, readers, 1)

	reader := readers[0]
	defer reader.Close()

	var row testRowWithTwoColumns
	require.True(t, reader.Next())
	require.NoError(t, reader.Scan(&row))
	// Should only have the selected columns
	assert.Equal(t, testRowWithTwoColumns{Key: "key1", Value0: "val0"}, row)

	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

func (s *Suite) TestMultiLookupRows_WithTimestamp(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	table := tmpPath().Child("table")
	require.NoError(t, migrate.Create(ctx, yc, table, schema.MustInfer(&testRow{})))
	require.NoError(t, migrate.MountAndWait(ctx, yc, table))

	rows := []any{&testRow{"key1", "value1"}}
	require.NoError(t, yc.InsertRows(ctx, table, rows, nil))

	// Generate timestamp after insertion
	ts, err := yc.GenerateTimestamp(ctx, nil)
	require.NoError(t, err)

	// Delete the row
	keys := []any{&testKey{"key1"}}
	require.NoError(t, yc.DeleteRows(ctx, table, keys, nil))

	// MultiLookupRows with timestamp should still find the row
	subrequests := []yt.MultiLookupSubrequest{
		{
			Path: table,
			Keys: []any{&testKey{"key1"}},
		},
	}

	options := &yt.MultiLookupRowsOptions{
		Timestamp: &ts,
	}

	readers, err := yc.MultiLookupRows(ctx, subrequests, options)
	require.NoError(t, err)
	require.Len(t, readers, 1)

	reader := readers[0]
	defer reader.Close()

	var row testRow
	require.True(t, reader.Next())
	require.NoError(t, reader.Scan(&row))
	assert.Equal(t, testRow{"key1", "value1"}, row)

	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

func (s *Suite) TestMultiLookupRows_EmptySubrequests(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	// Test with empty subrequests
	readers, err := yc.MultiLookupRows(ctx, []yt.MultiLookupSubrequest{}, nil)
	require.NoError(t, err)
	require.Len(t, readers, 0)
}
