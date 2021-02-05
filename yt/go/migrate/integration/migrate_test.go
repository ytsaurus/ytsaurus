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

type orderedRow struct {
	A string
	B int
}

var (
	testSchema    = schema.MustInfer(&testRow{})
	updatedSchema = testSchema.Append(schema.Column{
		Name: "C",
		Type: schema.TypeInt64,
	})
	orderedSchema = schema.MustInfer(&orderedRow{})
)

func init() {
	strict := true

	testSchema.Strict = &strict
	updatedSchema.Strict = &strict
	orderedSchema.Strict = &strict
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

func checkSchemas(t *testing.T, env *yttest.Env, schemas map[ypath.Path]migrate.Table) {
	for path, expected := range schemas {
		var actual schema.Schema
		require.NoError(t, env.YT.GetNode(env.Ctx, path.Attr("schema"), &actual, nil))
		require.Equal(t, expected.Schema.WithUniqueKeys(), actual)
	}
}

func TestCreateTables(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	root := env.TmpPath()
	pathA := root.Child("table_a")
	pathB := root.Child("table_b")

	firstSchemas := map[ypath.Path]migrate.Table{
		pathA: {Schema: testSchema},
		pathB: {Schema: testSchema},
	}
	secondSchemas := map[ypath.Path]migrate.Table{
		pathA: {Schema: testSchema},
		pathB: {Schema: updatedSchema},
	}

	getIDs := func() (a, b yt.NodeID) {
		require.NoError(t, env.YT.GetNode(env.Ctx, pathA.Attr("id"), &a, nil))
		require.NoError(t, env.YT.GetNode(env.Ctx, pathB.Attr("id"), &b, nil))
		return
	}

	drop := migrate.OnConflictDrop(env.Ctx, env.YT)

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, firstSchemas, drop))
	checkSchemas(t, env, firstSchemas)
	firstIDA, firstIDB := getIDs()

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, firstSchemas, drop))
	checkSchemas(t, env, firstSchemas)

	secondIDA, secondIDB := getIDs()
	require.Equal(t, firstIDA, secondIDA)
	require.Equal(t, firstIDB, secondIDB)

	require.NoError(t, migrate.UnmountAndWait(env.Ctx, env.YT, pathA))

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, secondSchemas, drop))
	checkSchemas(t, env, secondSchemas)

	secondIDA, secondIDB = getIDs()
	require.Equal(t, firstIDA, secondIDA)
	require.NotEqual(t, firstIDB, secondIDB)
}

func TestOrderedTables(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	root := env.TmpPath()
	tablePath := root.Child("table")

	schemas := map[ypath.Path]migrate.Table{
		tablePath: {Schema: orderedSchema},
	}

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, schemas, migrate.OnConflictFail))
	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, schemas, migrate.OnConflictFail))
}

func TestMigrateDoNotTouch(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	root := env.TmpPath()
	schemas := map[ypath.Path]migrate.Table{
		root.Child("table"): {Schema: testSchema},
	}

	checkNoConflict := func(path ypath.Path, _, _ schema.Schema) error {
		t.Fatalf("Migration reported conflict for path %q", path)
		return nil
	}

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, schemas, checkNoConflict))
	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, schemas, checkNoConflict))
}

func TestMigrateAlter(t *testing.T) {
	env, cancel := yttest.NewEnv(t)
	defer cancel()

	root := env.TmpPath()
	schemas := map[ypath.Path]migrate.Table{root.Child("table"): {Schema: testSchema}}
	updatedSchema := map[ypath.Path]migrate.Table{root.Child("table"): {Schema: updatedSchema}}

	alter := migrate.OnConflictTryAlter(env.Ctx, env.YT)

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, schemas, alter))
	checkSchemas(t, env, schemas)

	require.NoError(t, migrate.EnsureTables(env.Ctx, env.YT, updatedSchema, alter))
	checkSchemas(t, env, updatedSchema)
}
