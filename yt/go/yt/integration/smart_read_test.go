package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/httpclient"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestSmartRead(t *testing.T) {
	t.Parallel()

	logger, stopLogger := yttest.NewLogger(t)
	defer stopLogger()

	env := yttest.New(t, yttest.WithConfig(yt.Config{
		CompressionCodec: yt.ClientCodecNone,
		Logger:           logger,
	}))

	t.Run("P", func(t *testing.T) {
		t.Run("EmptyTable", func(t *testing.T) {
			t.Parallel()

			path := env.TmpPath()
			_, err := env.YT.CreateNode(env.Ctx, path, yt.NodeTable, nil)
			require.NoError(t, err)

			t.Run("NoTx", func(t *testing.T) {
				r, err := env.YT.ReadTable(env.Ctx, path, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				require.NoError(t, err)
				defer r.Close()

				require.False(t, r.Next())
				require.NoError(t, r.Err())
			})

			t.Run("Tx", func(t *testing.T) {
				tx, err := env.YT.BeginTx(env.Ctx, nil)
				require.NoError(t, err)
				defer tx.Abort()

				r, err := tx.ReadTable(env.Ctx, path, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				require.NoError(t, err)
				defer r.Close()

				require.False(t, r.Next())
				require.NoError(t, r.Err())
			})
		})

		t.Run("NoErrors", func(t *testing.T) {
			t.Parallel()

			rows := []testRow{
				{Key: "1", Value: "a"},
				{Key: "2", Value: "b"},
				{Key: "3", Value: "c"},
			}

			path := env.TmpPath()
			require.NoError(t, env.UploadSlice(path, rows))

			checkRead := func(t *testing.T, doRead func() (yt.TableReader, error)) {
				r, err := doRead()
				require.NoError(t, err)
				defer r.Close()

				var row testRow

				require.True(t, r.Next())
				require.NoError(t, r.Scan(&row))
				require.Equal(t, rows[0], row)

				require.True(t, r.Next())
				require.NoError(t, r.Scan(&row))
				require.Equal(t, rows[1], row)

				require.True(t, r.Next())
				require.NoError(t, r.Scan(&row))
				require.Equal(t, rows[2], row)

				require.False(t, r.Next())
				require.NoError(t, r.Err())
			}

			t.Run("NoTx", func(t *testing.T) {
				checkRead(t, func() (yt.TableReader, error) {
					return env.YT.ReadTable(env.Ctx, path, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				})
			})

			t.Run("Tx", func(t *testing.T) {
				tx, err := env.YT.BeginTx(env.Ctx, nil)
				require.NoError(t, err)
				defer tx.Abort()

				checkRead(t, func() (yt.TableReader, error) {
					return tx.ReadTable(env.Ctx, path, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				})
			})
		})

		t.Run("ErrorInjection", func(t *testing.T) {
			t.Parallel()

			ctx := httpclient.WithRoundTripper(env.Ctx, &readTruncatingRoundTripper{n: 1000})
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()

			var rows []testRow
			for i := 0; i < 1000; i++ {
				rows = append(rows, testRow{Key: fmt.Sprintf("%09d", i), Value: "bar"})
			}

			path := env.TmpPath()
			require.NoError(t, env.UploadSlice(path, rows))

			checkRead := func(t *testing.T, doRead func() (yt.TableReader, error)) {
				r, err := doRead()
				require.NoError(t, err)
				defer r.Close()

				var row testRow
				for i := 0; i < len(rows); i++ {
					require.True(t, r.Next())
					require.NoError(t, r.Scan(&row))
					require.Equal(t, rows[i], row)
				}

				require.False(t, r.Next())
				require.NoError(t, r.Err())
			}

			t.Run("NoTx", func(t *testing.T) {
				checkRead(t, func() (yt.TableReader, error) {
					return env.YT.ReadTable(ctx, path, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				})
			})

			t.Run("Tx", func(t *testing.T) {
				tx, err := env.YT.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Abort()

				checkRead(t, func() (yt.TableReader, error) {
					return tx.ReadTable(ctx, path, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				})
			})
		})

		t.Run("ReadRanges", func(t *testing.T) {
			t.Parallel()

			ctx := httpclient.WithRoundTripper(env.Ctx, &readTruncatingRoundTripper{n: 1000})
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()

			var rows []testRow
			for i := 0; i < 1000; i++ {
				rows = append(rows, testRow{Key: fmt.Sprintf("%09d", i), Value: "bar"})
			}

			path := env.TmpPath()
			require.NoError(t, env.UploadSlice(path, rows))

			checkRead := func(t *testing.T, doRead func() (yt.TableReader, error)) {
				r, err := doRead()
				require.NoError(t, err)
				defer r.Close()

				var row testRow
				for i := 0; i < len(rows); i++ {
					require.True(t, r.Next())
					require.NoError(t, r.Scan(&row))
					require.Equal(t, rows[i], row)
				}
				for i := 0; i < len(rows); i++ {
					require.True(t, r.Next())
					require.NoError(t, r.Scan(&row))
					require.Equal(t, rows[i], row)
				}
				require.False(t, r.Next())
				require.NoError(t, r.Err())
			}

			pathWithRange := &ypath.Rich{
				Path: path,
				Ranges: []ypath.Range{
					{},
					{Upper: &ypath.ReadLimit{RowIndex: ptr.Int64(500)}},
					{Lower: &ypath.ReadLimit{RowIndex: ptr.Int64(500)}},
				},
			}

			t.Run("NoTx", func(t *testing.T) {
				checkRead(t, func() (yt.TableReader, error) {
					return env.YT.ReadTable(ctx, pathWithRange, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				})
			})

			t.Run("Tx", func(t *testing.T) {
				tx, err := env.YT.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Abort()

				checkRead(t, func() (yt.TableReader, error) {
					return tx.ReadTable(ctx, pathWithRange, &yt.ReadTableOptions{Smart: ptr.Bool(true)})
				})
			})
		})
	})
}
