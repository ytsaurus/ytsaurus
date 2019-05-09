package integration

import (
	"testing"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"

	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

type testRow struct {
	A string `yson:",key"`
	B int
}

var (
	testSchema    = schema.MustInfer(&testRow{}).WithUniqueKeys()
	updatedSchema = testSchema.Append(schema.Column{
		Name: "C",
		Type: schema.TypeInt64,
	})
)

func init() {
	strict := true
	testSchema.Strict = &strict
	updatedSchema.Strict = &strict
}

func TestTableMount(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	path := env.TmpPath().Child("table")
	require.NoError(t, migrate.Create(env.Ctx, env.YT, path, testSchema))

	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, path))
	require.NoError(t, migrate.MountAndWait(env.Ctx, env.YT, path))

	require.NoError(t, migrate.UnmountAndWait(env.Ctx, env.YT, path))
	require.NoError(t, migrate.UnmountAndWait(env.Ctx, env.YT, path))
}

func TestCreateTables(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	root := env.TmpPath()
	pathA := root.Child("table_a")
	pathB := root.Child("table_b")

	firstSchemas := map[ypath.Path]schema.Schema{
		pathA: testSchema,
		pathB: testSchema,
	}
	secondSchemas := map[ypath.Path]schema.Schema{
		pathA: testSchema,
		pathB: updatedSchema,
	}

	getIDs := func() (a, b yt.NodeID) {
		require.NoError(t, env.YT.GetNode(env.Ctx, pathA.Attr("id"), &a, nil))
		require.NoError(t, env.YT.GetNode(env.Ctx, pathB.Attr("id"), &b, nil))
		return
	}

	checkSchemas := func(schemas map[ypath.Path]schema.Schema) {
		for path, expected := range schemas {
			var actual schema.Schema
			require.NoError(t, env.YT.GetNode(env.Ctx, path.Attr("schema"), &actual, nil))
			require.Equal(t, expected, actual)
		}
	}

	drop := migrate.OnConflictDrop(env.Ctx, env.YT)

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, firstSchemas, drop))
	checkSchemas(firstSchemas)
	firstIDA, firstIDB := getIDs()

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, firstSchemas, drop))
	checkSchemas(firstSchemas)

	secondIDA, secondIDB := getIDs()
	require.Equal(t, firstIDA, secondIDA)
	require.Equal(t, firstIDB, secondIDB)

	require.NoError(t, migrate.UnmountAndWait(env.Ctx, env.YT, pathA))

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, secondSchemas, drop))
	checkSchemas(secondSchemas)

	secondIDA, secondIDB = getIDs()
	require.Equal(t, firstIDA, secondIDA)
	require.NotEqual(t, firstIDB, secondIDB)
}
