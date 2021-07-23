package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/ytlog"
	"github.com/stretchr/testify/require"
)

type testSchemaRow struct {
	A string `yson:",key"`
	B int
}

type testReshardRow struct {
	A string `yson:"a,key"`
	B uint64 `yson:"b,key"`
	C string `yson:"c,omitempty"`
}

func TestMountClient(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
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

			t.Run("Mount", func(t *testing.T) {
				t.Parallel()

				testSchema := schema.MustInfer(&testSchemaRow{})

				p := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, p, testSchema))

				require.NoError(t, migrate.MountAndWait(ctx, client, p))
				require.NoError(t, migrate.MountAndWait(ctx, client, p))

				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
			})

			t.Run("Remount", func(t *testing.T) {
				t.Parallel()

				testSchema := schema.MustInfer(&testSchemaRow{})

				p := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, p, testSchema))

				require.NoError(t, migrate.MountAndWait(ctx, client, p))
				require.NoError(t, client.RemountTable(ctx, p, nil))
				require.NoError(t, waitTabletState(ctx, client, p, yt.TabletMounted))

				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
			})

			t.Run("Freeze", func(t *testing.T) {
				t.Parallel()

				testSchema := schema.MustInfer(&testSchemaRow{})

				p := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, p, testSchema))

				require.NoError(t, migrate.MountAndWait(ctx, client, p))

				require.NoError(t, migrate.FreezeAndWait(ctx, client, p))
				require.NoError(t, migrate.FreezeAndWait(ctx, client, p))

				require.NoError(t, migrate.UnfreezeAndWait(ctx, client, p))
				require.NoError(t, migrate.UnfreezeAndWait(ctx, client, p))

				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
			})

			t.Run("Reshard", func(t *testing.T) {
				t.Parallel()

				testSchema := schema.MustInfer(&testReshardRow{})

				p := tmpPath().Child("table")
				require.NoError(t, migrate.Create(ctx, client, p, testSchema))

				require.Error(t, client.ReshardTable(ctx, p, &yt.ReshardTableOptions{
					PivotKeys: [][]interface{}{{"a"}},
				}), "first pivot key must match that of the first tablet in the resharded range")

				require.Error(t, client.ReshardTable(ctx, p, &yt.ReshardTableOptions{
					PivotKeys: [][]interface{}{{}, {"b"}, {"a"}},
				}), "pivot keys must be strictly increasing")

				require.Error(t, client.ReshardTable(ctx, p, &yt.ReshardTableOptions{
					PivotKeys: []interface{}{
						[]interface{}{},
						testReshardRow{A: "c", B: 420},
					},
				}), "only slices could be used as pivot keys")

				require.NoError(t, client.ReshardTable(ctx, p, &yt.ReshardTableOptions{
					PivotKeys: []interface{}{
						[]interface{}{},
						[]interface{}{"a"},
						[]interface{}{"b", uint64(42)},
					},
				}))

				require.NoError(t, client.ReshardTable(ctx, p, &yt.ReshardTableOptions{
					TabletCount: ptr.Int(6),
				}))

				require.NoError(t, migrate.MountAndWait(ctx, client, p))
				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
			})
		})
	}
}

func waitTabletState(ctx context.Context, yc yt.Client, path ypath.Path, state string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	return yt.PollMaster(ctx, yc, func() (stop bool, err error) {
		var currentState string
		err = yc.GetNode(ctx, path.Attr("tablet_state"), &currentState, nil)
		if err != nil {
			return
		}

		if currentState == state {
			stop = true
			return
		}

		return
	})
}
