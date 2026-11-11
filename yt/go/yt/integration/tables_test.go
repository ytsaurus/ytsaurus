package integration

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/httpclient"
	"go.ytsaurus.tech/yt/go/yttest"
)

type exampleRow struct {
	A string `yson:"a"`
	B int    `yson:"b"`
}

func TestTables(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	validate := func(name ypath.Path) {
		w, err := env.YT.WriteTable(ctx, name, nil)
		require.NoError(t, err)

		require.NoError(t, w.Write(exampleRow{"foo", 1}))
		require.NoError(t, w.Write(exampleRow{"bar", 2}))
		require.NoError(t, w.Write(exampleRow{B: 3}))
		require.NoError(t, w.Commit())

		r, err := env.YT.ReadTable(ctx, name, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		var s exampleRow
		require.True(t, r.Next())
		require.NoError(t, r.Scan(&s))
		require.Equal(t, exampleRow{"foo", 1}, s)

		require.True(t, r.Next())
		require.NoError(t, r.Scan(&s))
		require.Equal(t, exampleRow{"bar", 2}, s)

		require.True(t, r.Next())
		require.NoError(t, r.Scan(&s))
		require.Equal(t, exampleRow{"", 3}, s)

		require.False(t, r.Next())
		require.NoError(t, r.Err())
	}

	t.Run("WriteReadTable", func(t *testing.T) {
		ctx := ctxlog.WithFields(ctx, log.String("subtest_name", t.Name()))

		name := tmpPath()

		_, err := env.YT.CreateNode(ctx, name, yt.NodeTable, nil)
		require.NoError(t, err)

		validate(name)
	})

	t.Run("WriteWithSchema", func(t *testing.T) {
		ctx := ctxlog.WithFields(ctx, log.String("subtest_name", t.Name()))

		name := tmpPath()

		_, err := env.YT.CreateNode(ctx, name, yt.NodeTable, &yt.CreateNodeOptions{
			Attributes: map[string]any{
				"schema": schema.MustInfer(&exampleRow{}),
			},
		})
		require.NoError(t, err)

		validate(name)
	})

	t.Run("AppendToTable", func(t *testing.T) {
		ctx := ctxlog.WithFields(ctx, log.String("subtest_name", t.Name()))

		name := tmpPath()

		_, err := env.YT.CreateNode(ctx, name, yt.NodeTable, nil)
		require.NoError(t, err)

		write := func() {
			appendAttr := true
			w, err := env.YT.WriteTable(ctx, ypath.Rich{Path: name, Append: &appendAttr}, nil)
			require.NoError(t, err)

			require.NoError(t, w.Write(exampleRow{"foo", 1}))
			require.NoError(t, w.Write(exampleRow{"bar", 2}))
			require.NoError(t, w.Commit())
		}

		write()

		write()

		var rowCount int
		require.NoError(t, env.YT.GetNode(env.Ctx, name.Attr("row_count"), &rowCount, nil))
		require.Equal(t, 4, rowCount)
	})

	t.Run("ReadRanges", func(t *testing.T) {
		name := tmpPath()

		err := env.UploadSlice(name, []exampleRow{
			{"foo", 1},
			{"bar", 2},
		})
		require.NoError(t, err)

		richPath := name.Rich().
			AddRange(ypath.Exact(ypath.RowIndex(1))).
			AddRange(ypath.Exact(ypath.RowIndex(0)))

		var result []exampleRow
		err = env.DownloadSlice(richPath, &result)
		require.NoError(t, err)

		require.Equal(t, []exampleRow{
			{"bar", 2},
			{"foo", 1},
		}, result)
	})

	t.Run("ReadTableRaw", func(t *testing.T) {
		ctx := ctxlog.WithFields(ctx, log.String("subtest_name", t.Name()))

		name := tmpPath()
		require.NoError(t, env.UploadSlice(name, []exampleRow{
			{"foo", 1},
			{"bar", 2},
		}))

		r, err := yt.ReadTableRaw(ctx, env.YT, name.Rich().SetColumns([]string{"a"}), &yt.ReadTableOptions{Format: "json"})
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, "{\"a\":\"foo\"}\n{\"a\":\"bar\"}\n", string(data))
	})

	t.Run("ReadTableRawSnapshot", func(t *testing.T) {
		ctx := ctxlog.WithFields(ctx, log.String("subtest_name", t.Name()))

		name := tmpPath()
		require.NoError(t, env.UploadSlice(name, []exampleRow{{"foo", 1}}))

		r, err := yt.ReadTableRaw(ctx, env.YT, name, &yt.ReadTableOptions{Format: "json"})
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		require.NoError(t, env.UploadSlice(name, []exampleRow{{"bar", 2}}))

		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, "{\"a\":\"foo\",\"b\":1}\n", string(data))
	})

	t.Run("Rollback", func(t *testing.T) {
		ctx := ctxlog.WithFields(ctx, log.String("subtest_name", t.Name()))

		name := tmpPath()

		_, err := env.YT.CreateNode(ctx, name, yt.NodeTable, nil)
		require.NoError(t, err)

		w, err := env.YT.WriteTable(ctx, name, nil)
		require.NoError(t, err)

		go func() {
			time.Sleep(time.Second * 5)
			_ = w.Rollback()
		}()

		for {
			if err := w.Write(exampleRow{"foo", 1}); err != nil {
				break
			}
		}

		var count int
		require.NoError(t, env.YT.GetNode(ctx, name.Attr("row_count"), &count, nil))
		require.Equal(t, 0, count)
	})

	t.Run("EmptyTableInfo", func(t *testing.T) {
		name := tmpPath()

		_, err := yt.CreateTable(env.Ctx, env.YT, name)
		require.NoError(t, err)

		r, err := env.YT.ReadTable(env.Ctx, name, nil)
		require.NoError(t, err)
		defer r.Close()

		_, ok := yt.StartRowIndex(r)
		require.False(t, ok)
		require.NoError(t, r.Err())
	})

	t.Run("ReaderInfo", func(t *testing.T) {
		name := tmpPath()

		err := env.UploadSlice(name, []exampleRow{
			{"foo", 1},
			{"bar", 2},
		})
		require.NoError(t, err)

		r, err := env.YT.ReadTable(env.Ctx, name, nil)
		require.NoError(t, err)
		defer r.Close()

		startRowIndex, ok := yt.StartRowIndex(r)
		require.True(t, ok)
		require.Equal(t, int64(0), startRowIndex)

		require.NoError(t, r.Err())

		r, err = env.YT.ReadTable(env.Ctx, name+"[#1:]", nil)
		require.NoError(t, err)
		defer r.Close()

		startRowIndex, ok = yt.StartRowIndex(r)
		require.True(t, ok)
		require.Equal(t, int64(1), startRowIndex)

		rowCount, ok := yt.ApproximateRowCount(r)
		require.True(t, ok)
		require.Greater(t, rowCount, int64(0))

		require.NoError(t, r.Err())
	})

	t.Run("ExecTx", func(t *testing.T) {
		t.Run("commit", func(t *testing.T) {
			name := tmpPath()
			err := yt.ExecTx(env.Ctx, env.YT, func(ctx context.Context, tx yt.Tx) error {
				_, err := tx.CreateNode(ctx, name, yt.NodeTable, nil)
				require.NoError(t, err)

				w, err := tx.WriteTable(ctx, name, nil)
				require.NoError(t, err)
				defer func() { _ = w.Commit() }()

				require.NoError(t, w.Write(exampleRow{"foo", 1}))

				return nil
			}, nil)
			require.NoError(t, err)

			var count int
			require.NoError(t, env.YT.GetNode(ctx, name.Attr("row_count"), &count, nil))
			require.Equal(t, 1, count)
		})

		t.Run("abort", func(t *testing.T) {
			name := tmpPath()
			err := yt.ExecTx(env.Ctx, env.YT, func(ctx context.Context, tx yt.Tx) error {
				_, err := tx.CreateNode(ctx, name, yt.NodeTable, nil)
				require.NoError(t, err)

				w, err := tx.WriteTable(ctx, name, nil)
				require.NoError(t, err)
				defer func() { _ = w.Commit() }()

				require.NoError(t, w.Write(exampleRow{"foo", 1}))
				return xerrors.New("some error")
			}, nil)
			require.Error(t, err)

			var count int
			require.Error(t, env.YT.GetNode(ctx, name.Attr("row_count"), &count, nil))
		})
	})
}

func TestHighLevelTableWriter(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	checkTable := func(t *testing.T, path ypath.Path, rowCount int, expectedSchema schema.Schema) {
		t.Helper()

		var realRowCount int
		require.NoError(t, env.YT.GetNode(env.Ctx, path.Attr("row_count"), &realRowCount, nil))
		require.Equal(t, rowCount, realRowCount, "table size differs from number of written rows")

		var realSchema schema.Schema
		require.NoError(t, env.YT.GetNode(env.Ctx, path.Attr("schema"), &realSchema, nil))
		require.Truef(t, expectedSchema.Equal(realSchema), "%v != %v", expectedSchema, realSchema)
	}

	testRollbackWithCreateTransactionFalse := func(t *testing.T, scope yt.CypressClient) {
		t.Helper()

		tmpTableName := tmpPath()

		w, err := yt.WriteTable(env.Ctx, scope, tmpTableName, yt.WithCreateTransaction(false))
		require.NoError(t, err)

		require.NoError(t, w.Write(exampleRow{"lazy_create_table", 1}))
		require.NoError(t, w.Rollback())

		exists, err := scope.NodeExists(env.Ctx, tmpTableName, nil)
		require.NoError(t, err)
		require.True(t, exists, "Table should exist because it created without tx")
	}

	testRollbackWithCreateTransactionTrue := func(t *testing.T, scope yt.CypressClient) {
		t.Helper()

		tmpTableName := tmpPath()

		w, err := yt.WriteTable(env.Ctx, scope, tmpTableName, yt.WithCreateTransaction(true))
		require.NoError(t, err)

		require.NoError(t, w.Write(exampleRow{"lazy_create_table", 1}))
		require.NoError(t, w.Rollback())

		exists, err := scope.NodeExists(env.Ctx, tmpTableName, nil)
		require.NoError(t, err)
		require.False(t, exists, "Table should not exist because it created inside tx")
	}

	testCommitWithCreateTransactionTrue := func(t *testing.T, scope yt.CypressClient) ypath.Path {
		t.Helper()

		tmpTableName := tmpPath()

		w, err := yt.WriteTable(env.Ctx, scope, tmpTableName, yt.WithCreateTransaction(true))
		require.NoError(t, err)

		const testSize = 1024
		for i := 0; i < testSize; i++ {
			require.NoError(t, w.Write(exampleRow{"foo", 1}))
		}

		exists, err := scope.NodeExists(env.Ctx, tmpTableName, nil)
		require.NoError(t, err)
		require.False(t, exists, "Table should not be visible because it created inside tx")

		require.NoError(t, w.Commit())

		exists, err = scope.NodeExists(env.Ctx, tmpTableName, nil)
		require.NoError(t, err)
		require.True(t, exists, "Table should exists after commit")

		return tmpTableName
	}

	t.Run("BigWrite", func(t *testing.T) {
		tmpTableName := tmpPath()

		w, err := yt.WriteTable(env.Ctx, env.YT, tmpTableName, yt.WithBatchSize(100))
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		const testSize = 1024
		for i := 0; i < testSize; i++ {
			require.NoError(t, w.Write(exampleRow{"foo", 1}))
		}

		require.NoError(t, w.Commit())

		checkTable(t, tmpTableName, testSize, schema.MustInfer(&exampleRow{}))
	})

	t.Run("EagerCreate", func(t *testing.T) {
		tmpTableName := tmpPath()

		w, err := yt.WriteTable(env.Ctx, env.YT, tmpTableName,
			yt.WithCreateOptions(yt.WithInferredSchema(&exampleRow{})),
			yt.WithCreateTransaction(false),
		)
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		checkTable(t, tmpTableName, 0, schema.MustInfer(&exampleRow{}))

		const testSize = 1024
		for i := 0; i < testSize; i++ {
			require.NoError(t, w.Write(exampleRow{"foo", 1}))
		}

		require.NoError(t, w.Commit())

		checkTable(t, tmpTableName, testSize, schema.MustInfer(&exampleRow{}))
	})

	t.Run("NestedClientRollbackCreateTransactionFalse", func(t *testing.T) {
		testRollbackWithCreateTransactionFalse(t, env.YT)
	})
	t.Run("NestedTxRollbackCreateTransactionFalse", func(t *testing.T) {
		parentTx, err := env.YT.BeginTx(env.Ctx, nil)
		require.NoError(t, err)
		defer func() { _ = parentTx.Abort() }()

		testRollbackWithCreateTransactionFalse(t, parentTx)

		require.NoError(t, parentTx.Commit())
	})
	t.Run("NestedClientRollbackCreateTransactionTrue", func(t *testing.T) {
		testRollbackWithCreateTransactionTrue(t, env.YT)
	})
	t.Run("NestedTxRollbackCreateTransactionTrue", func(t *testing.T) {
		parentTx, err := env.YT.BeginTx(env.Ctx, nil)
		require.NoError(t, err)
		defer func() { _ = parentTx.Abort() }()

		testRollbackWithCreateTransactionTrue(t, parentTx)

		require.NoError(t, parentTx.Commit())
	})
	t.Run("NestedClientCommitCreateTransactionTrue", func(t *testing.T) {
		path := testCommitWithCreateTransactionTrue(t, env.YT)
		checkTable(t, path, 1024, schema.MustInfer(&exampleRow{}))
	})
	t.Run("NestedTxCommitCreateTransactionTrue", func(t *testing.T) {
		parentTx, err := env.YT.BeginTx(env.Ctx, nil)
		require.NoError(t, err)
		defer func() { _ = parentTx.Abort() }()

		path := testCommitWithCreateTransactionTrue(t, parentTx)

		require.NoError(t, parentTx.Commit())

		checkTable(t, path, 1024, schema.MustInfer(&exampleRow{}))
	})

	t.Run("ExistingTable", func(t *testing.T) {
		tmpTableName := tmpPath()

		_, err := yt.CreateTable(env.Ctx, env.YT, tmpTableName)
		require.NoError(t, err)

		w, err := yt.WriteTable(env.Ctx, env.YT, tmpTableName, yt.WithExistingTable())
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		checkTable(t, tmpTableName, 0, schema.Schema{Strict: ptr.Bool(false)})

		const testSize = 1024
		const partSize = 512
		for i := 0; i < partSize; i++ {
			require.NoError(t, w.Write(exampleRow{"foo", 1}))
		}

		require.NoError(t, w.Commit())

		checkTable(t, tmpTableName, partSize, schema.Schema{Strict: ptr.Bool(false)})

		w, err = yt.WriteTable(env.Ctx, env.YT, tmpTableName, yt.WithExistingTable(), yt.WithAppend())
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		checkTable(t, tmpTableName, partSize, schema.Schema{Strict: ptr.Bool(false)})

		for i := 0; i < partSize; i++ {
			require.NoError(t, w.Write(exampleRow{"foo", 1}))
		}

		require.NoError(t, w.Commit())

		checkTable(t, tmpTableName, testSize, schema.Schema{Strict: ptr.Bool(false)})
	})

	t.Run("AppendToNonExistingTable", func(t *testing.T) {
		tmpTableName := tmpPath()

		w, err := yt.WriteTable(env.Ctx, env.YT, tmpTableName, yt.WithAppend())
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		const testSize = 1024
		for i := 0; i < testSize; i++ {
			require.NoError(t, w.Write(exampleRow{"foo", 1}))
		}

		require.NoError(t, w.Commit())

		checkTable(t, tmpTableName, testSize, schema.MustInfer(&exampleRow{}))
	})

	t.Run("WriteWithNoRetries", func(t *testing.T) {
		tmpTableName := tmpPath()

		w, err := yt.WriteTable(env.Ctx, env.YT, tmpTableName, yt.WithRetries(0))
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		const testSize = 1024
		for i := 0; i < testSize; i++ {
			require.NoError(t, w.Write(exampleRow{"foo", 1}))
		}

		require.NoError(t, w.Commit())

		checkTable(t, tmpTableName, testSize, schema.MustInfer(&exampleRow{}))
	})
}

type testTimeTypes struct {
	D0 schema.Date
	D1 schema.Datetime
	D2 schema.Timestamp
	D3 schema.Interval
	D4 schema.Date32
	D5 schema.Datetime64
	D6 schema.Timestamp64
	D7 schema.Interval64
}

func TestTimeTables(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	rows := []testTimeTypes{
		{D0: 1, D1: 2, D2: 3, D3: 4, D4: -1, D5: -2, D6: -3, D7: -4},
	}

	path := env.TmpPath()
	require.NoError(t, env.UploadSlice(path, rows))

	var outRows []testTimeTypes
	require.NoError(t, env.DownloadSlice(path, &outRows))

	require.Equal(t, rows, outRows)
}

func TestBigRow(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	bigRow := map[string]any{
		"row": make([]byte, 20*(1<<20)),
	}
	config := map[string]any{
		"max_row_weight": 32 * (1 << 20),
	}
	opts := &yt.WriteTableOptions{TableWriter: config}

	t.Run("Error", func(t *testing.T) {
		tmpTableName := tmpPath()

		_, err := yt.CreateTable(env.Ctx, env.YT, tmpTableName)
		require.NoError(t, err)

		w, err := yt.WriteTable(env.Ctx, env.YT, tmpTableName, yt.WithExistingTable())
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		require.NoError(t, w.Write(bigRow))
		require.Error(t, w.Commit())
	})

	t.Run("RawWriterConfig", func(t *testing.T) {
		tmpTableName := tmpPath()

		_, err := yt.CreateTable(env.Ctx, env.YT, tmpTableName)
		require.NoError(t, err)

		w, err := env.YT.WriteTable(env.Ctx, tmpTableName, opts)
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		require.NoError(t, w.Write(bigRow))
		require.NoError(t, w.Commit())
	})

	t.Run("HighLevelConfig", func(t *testing.T) {
		tmpTableName := tmpPath()

		_, err := yt.CreateTable(env.Ctx, env.YT, tmpTableName)
		require.NoError(t, err)

		w, err := yt.WriteTable(env.Ctx, env.YT, tmpTableName,
			yt.WithExistingTable(),
			yt.WithTableWriterConfig(config))
		require.NoError(t, err)
		defer func() { _ = w.Rollback() }()

		require.NoError(t, w.Write(bigRow))
		require.NoError(t, w.Commit())
	})
}
func TestWriteEarlyErrorRetry(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)
	path := env.TmpPath()

	_, err := env.YT.CreateNode(env.Ctx, path, yt.NodeTable, nil)
	require.NoError(t, err)

	done := false
	returnInvalidProxyOnce := func() (string, bool) {
		if !done {
			done = true
			return "hahn.yt.yandex-team.ru:1", true
		} else {
			return "", false
		}
	}

	w, err := env.YT.WriteTable(httpclient.WithHeavyProxyOverride(env.Ctx, returnInvalidProxyOnce), path, nil)
	require.NoError(t, err)
	defer w.Rollback()

	require.NoError(t, w.Write(testRow{Key: "foo", Value: "bar"}))
	require.NoError(t, w.Commit())
}

func TestReadEarlyErrorRetry(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)
	path := env.TmpPath()

	_, err := env.YT.CreateNode(env.Ctx, path, yt.NodeTable, nil)
	require.NoError(t, err)

	done := false
	returnInvalidProxyOnce := func() (string, bool) {
		if !done {
			done = true
			return "hahn.yt.yandex-team.ru:1", true
		} else {
			return "", false
		}
	}

	r, err := env.YT.ReadTable(httpclient.WithHeavyProxyOverride(env.Ctx, returnInvalidProxyOnce), path, nil)
	require.NoError(t, err)
	defer r.Close()

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestReadTableRawResume(t *testing.T) {
	t.Parallel()

	env := yttest.New(t, yttest.WithConfig(yt.Config{CompressionCodec: yt.ClientCodecNone}))

	type keyRow struct {
		Key   int64  `yson:"key,key"`
		Value string `yson:"value"`
	}
	var rows []keyRow
	for i := 0; i < 30; i++ {
		rows = append(rows, keyRow{Key: int64(i / 3), Value: fmt.Sprintf("value-%02d", i)})
	}
	path := env.TmpPath()
	require.NoError(t, env.UploadSlice(path, rows))

	formats := map[string]any{
		"json":          "json",
		"dsv":           "dsv",
		"schemaful_dsv": yson.ValueWithAttrs{Value: "schemaful_dsv", Attrs: map[string]any{"columns": []string{"key", "value"}}},
	}
	ranges := []struct {
		name     string
		ranges   []ypath.Range
		firstRow int64
	}{
		{name: "Table"},
		{name: "RowIndexRange", ranges: []ypath.Range{ypath.Interval(ypath.RowIndex(4), ypath.RowIndex(20))}, firstRow: 4},
		{name: "KeyRange", ranges: []ypath.Range{ypath.Interval(ypath.Key(int64(2)), ypath.Key(int64(7)))}, firstRow: 6},
		{name: "ExactKey", ranges: []ypath.Range{ypath.Exact(ypath.Key(int64(4)))}, firstRow: 12},
	}

	for formatName, format := range formats {
		for _, tc := range ranges {
			t.Run(formatName+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(env.Ctx, time.Minute)
				defer cancel()

				richPath := &ypath.Rich{Path: path, Ranges: tc.ranges}
				options := &yt.ReadTableOptions{Format: format}
				expected := readAllTableRaw(ctx, t, env.YT, richPath, options)

				rowSize := strings.IndexByte(expected, '\n') + 1
				rt := &firstReadTruncatingRoundTripper{n: rowSize + rowSize/2}
				data := readAllTableRaw(httpclient.WithRoundTripper(ctx, rt), t, env.YT, richPath, options)

				require.Equal(t, expected, data)
				require.Len(t, rt.readParams, 2)
				require.Equal(t, tc.firstRow+1, lowerRowIndex(t, rt.readParams[1]))
			})
		}
	}
}

func readAllTableRaw(ctx context.Context, t *testing.T, yc yt.Client, path ypath.YPath, options *yt.ReadTableOptions) string {
	t.Helper()

	r, err := yt.ReadTableRaw(ctx, yc, path, options)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(data)
}

func lowerRowIndex(t *testing.T, readParams string) any {
	t.Helper()

	var params struct {
		Path yson.ValueWithAttrs `yson:"path"`
	}
	require.NoError(t, yson.Unmarshal([]byte(readParams), &params))

	ranges, ok := params.Path.Attrs["ranges"].([]any)
	require.True(t, ok, "path %v has no ranges", params.Path)
	require.Len(t, ranges, 1)
	rng, ok := ranges[0].(map[string]any)
	require.True(t, ok, "range %v is not a map", ranges[0])
	lower, ok := rng["lower_limit"].(map[string]any)
	require.True(t, ok, "range %v has no lower limit", rng)
	return lower["row_index"]
}
