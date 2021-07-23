package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/ytlog"

	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

type testRow struct {
	A string `yson:",key"`
	B int
}

type orderedRow struct {
	A string
	B int
}

func tmpPath() ypath.Path {
	return ypath.Path("//tmp").Child(guid.New().String())
}

func TestMigrate(t *testing.T) {
	env := yttest.New(t)

	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
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

			t.Run("TableMount", func(t *testing.T) {
				t.Parallel()

				p := tmpPath().Child("table")
				testSchema := schema.MustInfer(&testRow{})
				testSchema.Strict = ptr.Bool(true)

				require.NoError(t, migrate.Create(ctx, client, p, testSchema))

				require.NoError(t, migrate.MountAndWait(ctx, client, p))
				require.NoError(t, migrate.MountAndWait(ctx, client, p))

				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
			})

			t.Run("TableFreeze", func(t *testing.T) {
				t.Parallel()

				p := tmpPath().Child("table")
				testSchema := schema.MustInfer(&testRow{})
				testSchema.Strict = ptr.Bool(true)

				require.NoError(t, migrate.Create(ctx, client, p, testSchema))

				require.NoError(t, migrate.MountAndWait(ctx, client, p))

				require.NoError(t, migrate.FreezeAndWait(ctx, client, p))
				require.NoError(t, migrate.FreezeAndWait(ctx, client, p))

				require.NoError(t, migrate.UnfreezeAndWait(ctx, client, p))
				require.NoError(t, migrate.UnfreezeAndWait(ctx, client, p))

				require.NoError(t, migrate.UnmountAndWait(ctx, client, p))
			})

			t.Run("CreateTables", func(t *testing.T) {
				t.Parallel()

				root := tmpPath()
				pathA := root.Child("table_a")
				pathB := root.Child("table_b")

				testSchema := schema.MustInfer(&testRow{})
				testSchema.Strict = ptr.Bool(true)
				updatedSchema := testSchema.Append(schema.Column{
					Name: "C",
					Type: schema.TypeInt64,
				})

				firstSchemas := map[ypath.Path]migrate.Table{
					pathA: {Schema: testSchema},
					pathB: {Schema: testSchema},
				}
				secondSchemas := map[ypath.Path]migrate.Table{
					pathA: {Schema: testSchema},
					pathB: {Schema: updatedSchema},
				}

				getIDs := func() (a, b yt.NodeID) {
					require.NoError(t, client.GetNode(ctx, pathA.Attr("id"), &a, nil))
					require.NoError(t, client.GetNode(ctx, pathB.Attr("id"), &b, nil))
					return
				}

				drop := migrate.OnConflictDrop(ctx, client)

				require.NoError(t, migrate.EnsureTables(ctx, client, firstSchemas, drop))
				checkSchemas(t, env, firstSchemas)
				firstIDA, firstIDB := getIDs()

				require.NoError(t, migrate.EnsureTables(ctx, client, firstSchemas, drop))
				checkSchemas(t, env, firstSchemas)

				secondIDA, secondIDB := getIDs()
				require.Equal(t, firstIDA, secondIDA)
				require.Equal(t, firstIDB, secondIDB)

				require.NoError(t, migrate.UnmountAndWait(ctx, client, pathA))

				require.NoError(t, migrate.EnsureTables(ctx, client, secondSchemas, drop))
				checkSchemas(t, env, secondSchemas)

				secondIDA, secondIDB = getIDs()
				require.Equal(t, firstIDA, secondIDA)
				require.NotEqual(t, firstIDB, secondIDB)
			})

			t.Run("OrderedTables", func(t *testing.T) {
				t.Parallel()

				p := tmpPath().Child("table")

				orderedSchema := schema.MustInfer(&orderedRow{})
				orderedSchema.Strict = ptr.Bool(true)

				schemas := map[ypath.Path]migrate.Table{
					p: {Schema: orderedSchema},
				}

				require.NoError(t, migrate.EnsureTables(ctx, client, schemas, migrate.OnConflictFail))
				require.NoError(t, migrate.EnsureTables(ctx, client, schemas, migrate.OnConflictFail))
			})

			t.Run("DoNotTouch", func(t *testing.T) {
				t.Parallel()

				p := tmpPath().Child("table")

				testSchema := schema.MustInfer(&testRow{})
				testSchema.Strict = ptr.Bool(true)

				schemas := map[ypath.Path]migrate.Table{
					p: {Schema: testSchema},
				}

				checkNoConflict := func(path ypath.Path, _, _ schema.Schema) error {
					t.Fatalf("Migration reported conflict for path %q", path)
					return nil
				}

				require.NoError(t, migrate.EnsureTables(ctx, client, schemas, checkNoConflict))
				require.NoError(t, migrate.EnsureTables(ctx, client, schemas, checkNoConflict))
			})

			t.Run("Alter", func(t *testing.T) {
				t.Parallel()

				p := tmpPath().Child("table")

				testSchema := schema.MustInfer(&testRow{})
				testSchema.Strict = ptr.Bool(true)
				updatedSchema := testSchema.Append(schema.Column{
					Name: "C",
					Type: schema.TypeInt64,
				})

				schemas := map[ypath.Path]migrate.Table{p: {Schema: testSchema}}
				updatedSchemas := map[ypath.Path]migrate.Table{p: {Schema: updatedSchema}}

				alter := migrate.OnConflictTryAlter(ctx, client)

				require.NoError(t, migrate.EnsureTables(ctx, client, schemas, alter))
				checkSchemas(t, env, schemas)

				require.NoError(t, migrate.EnsureTables(ctx, client, updatedSchemas, alter))
				checkSchemas(t, env, updatedSchemas)
			})
		})
	}
}

func checkSchemas(t *testing.T, env *yttest.Env, schemas map[ypath.Path]migrate.Table) {
	for path, expected := range schemas {
		var actual schema.Schema
		require.NoError(t, env.YT.GetNode(env.Ctx, path.Attr("schema"), &actual, nil))
		require.Equal(t, expected.Schema.Normalize().WithUniqueKeys(), actual.Normalize())
	}
}
