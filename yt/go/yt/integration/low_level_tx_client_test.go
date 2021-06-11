package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/ytlog"
	"github.com/stretchr/testify/require"
)

func TestLowLevelTxClient(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func() (yt.LowLevelTxClient, error)
	}{
		{name: "http", makeClient: func() (yt.LowLevelTxClient, error) {
			return ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
		{name: "rpc", makeClient: func() (yt.LowLevelTxClient, error) {
			return ytrpc.NewLowLevelTxClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := tc.makeClient()
			require.NoError(t, err)

			t.Run("CommitTx", func(t *testing.T) {
				t.Parallel()

				tx, err := client.StartTx(ctx, nil)
				require.NoError(t, err)

				err = client.PingTx(ctx, tx, nil)
				require.NoError(t, err)

				err = client.CommitTx(ctx, tx, nil)
				require.NoError(t, err)

				err = client.PingTx(ctx, tx, nil)
				require.Error(t, err)
				require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
			})

			t.Run("CommitTabletTx", func(t *testing.T) {
				t.Parallel()

				tx, err := client.StartTabletTx(ctx, &yt.StartTabletTxOptions{
					Type:   yt.TxTypeTablet,
					Sticky: true,
				})
				require.NoError(t, err)

				err = client.PingTx(ctx, tx, nil)
				require.NoError(t, err)

				err = client.CommitTx(ctx, tx, nil)
				require.NoError(t, err)

				err = client.PingTx(ctx, tx, nil)
				require.Error(t, err)
				require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
			})

			t.Run("AbortTx", func(t *testing.T) {
				t.Parallel()

				tx, err := client.StartTx(ctx, nil)
				require.NoError(t, err)

				err = client.AbortTx(ctx, tx, nil)
				require.NoError(t, err)

				err = client.PingTx(ctx, tx, nil)
				require.Error(t, err)
				require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
			})

			t.Run("AbortTabletTx", func(t *testing.T) {
				t.Parallel()

				tx, err := client.StartTabletTx(ctx, &yt.StartTabletTxOptions{
					Type:   yt.TxTypeTablet,
					Sticky: true,
				})
				require.NoError(t, err)

				err = client.AbortTx(ctx, tx, nil)
				require.NoError(t, err)

				err = client.PingTx(ctx, tx, nil)
				require.Error(t, err)
				require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchTransaction))
			})
		})
	}
}
