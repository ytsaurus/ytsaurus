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

	"a.yandex-team.ru/library/go/core/log"
	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/yttest"
)

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

func TestTabletClient(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func() (yt.Client, error)
	}{
		{name: "http", makeClient: func() (yt.Client, error) {
			return ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
		{name: "rpc", makeClient: func() (yt.Client, error) {
			return ytrpc.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := tc.makeClient()
			require.NoError(t, err)

			t.Run("Tx", func(t *testing.T) {
				t.Parallel()

				testTable := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, testTable, schema.MustInfer(&testRow{})))
				require.NoError(t, migrate.MountAndWait(ctx, client, testTable))

				keys := []interface{}{
					&testKey{"bar"},
					&testKey{"foo"},
					&testKey{"baz"},
				}

				rows := []interface{}{
					&testRow{"bar", "2"},
					&testRow{"foo", "1"},
				}

				tx, err := client.BeginTabletTx(ctx, nil)
				require.NoError(t, err)

				err = tx.InsertRows(ctx, testTable, rows, nil)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)

				r, err := client.LookupRows(ctx, testTable, keys, nil)
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

				r, err = client.LookupRows(ctx, testTable, keys, &yt.LookupRowsOptions{KeepMissingRows: true})
				require.NoError(t, err)
				checkResult(r, true)

				r, err = client.SelectRows(ctx, fmt.Sprintf("* from [%s]", testTable), nil)
				require.NoError(t, err)
				checkResult(r, false)

				tx, err = client.BeginTabletTx(ctx, nil)
				require.NoError(t, err)

				err = tx.DeleteRows(ctx, testTable, keys, nil)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)

				r, err = client.LookupRows(ctx, testTable, keys, nil)
				require.NoError(t, err)

				require.False(t, r.Next())
				require.NoError(t, r.Err())
			})

			t.Run("TxDuration", func(t *testing.T) {
				t.Parallel()

				testTable := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, testTable, schema.MustInfer(&testRow{})))
				require.NoError(t, migrate.MountAndWait(ctx, client, testTable))

				rows := []interface{}{
					&testRow{"bar", "2"},
					&testRow{"foo", "1"},
				}

				tx, err := client.BeginTabletTx(ctx, nil)
				require.NoError(t, err)

				time.Sleep(time.Second * 20)

				require.NoError(t, tx.InsertRows(ctx, testTable, rows, nil))
				require.NoError(t, tx.Commit())
			})

			t.Run("ExecTabletTx", func(t *testing.T) {
				t.Parallel()

				testTable := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, testTable, schema.MustInfer(&testRow{})))
				require.NoError(t, migrate.MountAndWait(ctx, client, testTable))

				rows := []interface{}{&testRow{"foo", "1"}}
				keys := []interface{}{&testKey{"foo"}}

				err := yt.ExecTabletTx(ctx, client, func(ctx context.Context, tx yt.TabletTx) error {
					return tx.InsertRows(ctx, testTable, rows, nil)
				}, nil)
				require.NoError(t, err)

				r, err := client.LookupRows(ctx, testTable, keys, nil)
				require.NoError(t, err)

				var res testRow
				require.True(t, r.Next())
				require.NoError(t, r.Scan(&res))
				assert.Equal(t, rows[0], &res)

				require.False(t, r.Next())
				require.NoError(t, r.Err())

				var mu sync.Mutex
				mu.Lock()
			})

			t.Run("LookupColumnFilter", func(t *testing.T) {
				t.Parallel()

				testTable := tmpPath().Child("table")
				schema := schema.MustInfer(&testRowWithTwoColumns{})
				require.NoError(t, migrate.Create(ctx, client, testTable, schema))
				require.NoError(t, migrate.MountAndWait(ctx, client, testTable))

				rows := []interface{}{&testRowWithTwoColumns{"foo", "1", "2"}}
				keys := []interface{}{&testKey{"foo"}}

				require.NoError(t, client.InsertRows(ctx, testTable, rows, nil))

				readRow := func(r yt.TableReader) (row testRowWithTwoColumns) {
					defer r.Close()

					require.True(t, r.Next())
					require.NoError(t, r.Scan(&row))

					require.False(t, r.Next())
					require.NoError(t, r.Err())
					return
				}

				r, err := client.LookupRows(ctx, testTable, keys, nil)
				require.NoError(t, err)
				require.Equal(t, readRow(r), testRowWithTwoColumns{"foo", "1", "2"})

				r, err = client.LookupRows(ctx, testTable, keys, &yt.LookupRowsOptions{
					Columns: []string{"table_key", "value0"},
				})
				require.NoError(t, err)
				require.Equal(t, readRow(r), testRowWithTwoColumns{Key: "foo", Value0: "1"})
			})

			t.Run("ReadTimestamp", func(t *testing.T) {
				t.Parallel()

				testTable := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, testTable, schema.MustInfer(&testRow{})))
				require.NoError(t, migrate.MountAndWait(ctx, client, testTable))

				rows := []interface{}{&testRow{"foo", "1"}}
				keys := []interface{}{&testKey{"foo"}}

				require.NoError(t, client.InsertRows(ctx, testTable, rows, nil))

				ts, err := client.GenerateTimestamp(ctx, nil)
				_ = ts
				require.NoError(t, err)

				require.NoError(t, client.DeleteRows(ctx, testTable, keys, nil))

				checkReader := func(r yt.TableReader) {
					require.True(t, r.Next())

					var row testRow
					require.NoError(t, r.Scan(&row))
					require.Equal(t, &row, rows[0])

					require.False(t, r.Next())
					require.NoError(t, r.Err())
				}

				r, err := client.LookupRows(ctx, testTable, keys, &yt.LookupRowsOptions{Timestamp: &ts})
				require.NoError(t, err)
				defer r.Close()
				checkReader(r)

				r, err = client.SelectRows(ctx, fmt.Sprintf("* from [%s]", testTable), &yt.SelectRowsOptions{
					Timestamp: &ts,
				})
				require.NoError(t, err)
				defer r.Close()
				checkReader(r)
			})
		})
	}
}

func TestAbortCommittedTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func(l log.Structured) (yt.Client, error)
	}{
		{name: "http", makeClient: func(l log.Structured) (yt.Client, error) {
			return ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
		}},
		{name: "rpc", makeClient: func(l log.Structured) (yt.Client, error) {
			return ytrpc.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, recorded := observer.New(zapcore.ErrorLevel)
			l := logzap.Logger{L: zap.New(core)}

			client, err := tc.makeClient(l.Structured())
			require.NoError(t, err)

			tx, err := client.BeginTabletTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			err = tx.Abort()
			require.Error(t, err)

			err = tx.InsertRows(ctx, "//tmp", []interface{}{}, nil)
			require.Error(t, err)

			tx, err = client.BeginTabletTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, tx.Abort())

			err = tx.Commit()
			require.Error(t, err)

			err = tx.InsertRows(ctx, "//tmp", []interface{}{}, nil)
			require.Error(t, err)

			require.Empty(t, recorded.All())
		})
	}
}

func TestGenerateTimestamp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func() (yt.Client, error)
	}{
		{name: "http", makeClient: func() (yt.Client, error) {
			return ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
		{name: "rpc", makeClient: func() (yt.Client, error) {
			return ytrpc.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := tc.makeClient()
			require.NoError(t, err)

			ts, err := client.GenerateTimestamp(ctx, nil)
			require.NoError(t, err)
			require.NotZero(t, ts)
		})
	}
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
