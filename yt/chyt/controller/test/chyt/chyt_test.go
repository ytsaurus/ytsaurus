package chyt

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
)

func TestSimple(t *testing.T) {
	chytEnv, teardownCb := newCHYTEnv(t)
	defer teardownCb(t)

	alias := helpers.GenerateAlias()
	stopCb := runCHYTClique(chytEnv, alias, nil)
	defer stopCb()

	result := makeQuery(chytEnv, "select 1", alias)
	var data []map[string]int
	require.NoError(t, json.Unmarshal(result, &data))
	require.True(t, reflect.DeepEqual(data, []map[string]int{
		map[string]int{"1": 1},
	}))
}

func TestClusterConnectionUpdate(t *testing.T) {
	chytEnv, teardownCb := newCHYTEnv(t)
	defer teardownCb(t)

	alias := helpers.GenerateAlias()
	stopCb := runCHYTClique(chytEnv, alias, nil)
	defer stopCb()

	ctlClient := chytEnv.ctlClient
	briefInfo := ctlClient.GetBriefInfo(alias)
	require.Equal(t, 1, briefInfo.IncarnationIndex)

	err := chytEnv.ytEnv.YT.SetNode(
		chytEnv.ytEnv.Ctx,
		ypath.Path("//sys/@cluster_connection/chyt"),
		"something",
		nil)
	require.NoError(t, err)

	helpers.Wait(t, func() bool {
		briefInfo = ctlClient.GetBriefInfo(alias)
		return briefInfo.IncarnationIndex == 2
	})

	err = chytEnv.ytEnv.YT.SetNode(
		chytEnv.ytEnv.Ctx,
		ypath.Path("//sys/@cluster_connection/not_tracked_field"),
		"something",
		nil)
	require.NoError(t, err)

	time.Sleep(time.Second)

	briefInfo = ctlClient.GetBriefInfo(alias)
	require.Equal(t, 2, briefInfo.IncarnationIndex)
}

func TestSQLUDFStorage(t *testing.T) {
	chytEnv, teardownCb := newCHYTEnv(t)
	defer teardownCb(t)

	alias := helpers.GenerateAlias()
	stopCb := runCHYTClique(chytEnv, alias, nil)
	defer stopCb()

	udfStoragePath := ypath.Path("//sys/strawberry/chyt").Child(alias).Child("user_defined_sql_functions")
	ok, err := chytEnv.ytEnv.YT.NodeExists(chytEnv.ytEnv.Ctx, udfStoragePath, nil)
	require.NoError(t, err)
	require.True(t, ok)

	makeQuery(chytEnv, "create function linear_equation as (x, k, b) -> k*x + b", alias)

	functionPath := udfStoragePath.Child("linear_equation")
	ok, err = chytEnv.ytEnv.YT.NodeExists(chytEnv.ytEnv.Ctx, functionPath, nil)
	require.NoError(t, err)
	require.True(t, ok)
}
