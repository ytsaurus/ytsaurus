package integration

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/xerrors"
	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestGetInSyncReplicas(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	tablePath := env.TmpPath()
	_, err := createReplicatedTable(env, tablePath)
	require.NoError(t, err)

	replicaPath1 := env.TmpPath()
	replicaID1, err := createTableReplica(env, tablePath, replicaPath1)
	require.NoError(t, err)

	replicaPath2 := env.TmpPath()
	replicaID2, err := createTableReplica(env, tablePath, replicaPath2)
	require.NoError(t, err)

	_, err = createReplicaTable(env, replicaPath1, replicaID1)
	require.NoError(t, err)
	_, err = createReplicaTable(env, replicaPath2, replicaID2)
	require.NoError(t, err)

	require.NoError(t, ensureTableReplicaEnabled(env, replicaID1))

	require.Equal(t, "enabled", getTableReplicaState(env, replicaID1))
	require.Equal(t, "disabled", getTableReplicaState(env, replicaID2))

	keys := []interface{}{
		&testKey{"foo"},
		&testKey{"bar"},
	}

	rows := []interface{}{
		&testRow{"foo", "1"},
		&testRow{"bar", "2"},
	}

	require.NoError(t, env.YT.InsertRows(env.Ctx, tablePath, rows, &yt.InsertRowsOptions{
		RequireSyncReplica: ptr.Bool(false),
	}))

	ts, err := env.YT.GenerateTimestamp(env.Ctx, nil)
	require.NoError(t, err)

	ids, err := env.YT.GetInSyncReplicas(env.Ctx, tablePath, ts, nil, nil)
	require.NoError(t, err)
	require.Equal(t, NodeIDSlice{replicaID1, replicaID2}.MakeSet(), NodeIDSlice(ids).MakeSet())

	require.True(t, WaitFor(time.Second*60, func() bool {
		ids, err = env.YT.GetInSyncReplicas(env.Ctx, tablePath, ts, keys, nil)
		require.NoError(t, err)
		return reflect.DeepEqual([]yt.NodeID{replicaID1}, ids)
	}), "replicas sync timed out")
}

type NodeIDSet map[yt.NodeID]struct{}

type NodeIDSlice []yt.NodeID

func (s NodeIDSlice) MakeSet() NodeIDSet {
	ret := make(NodeIDSet)
	for _, id := range s {
		ret[id] = struct{}{}
	}
	return ret
}

func createReplicatedTable(env *yttest.Env, path ypath.Path) (id yt.NodeID, err error) {
	id, err = env.YT.CreateNode(env.Ctx, path, yt.NodeReplicatedTable, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"schema":                     schema.MustInfer(&testRow{}),
			"enable_replication_logging": true,
			"dynamic":                    true,
		},
	})
	if err != nil {
		return
	}
	err = migrate.MountAndWait(env.Ctx, env.YT, path)
	return
}

func createTableReplica(env *yttest.Env, tablePath, replicaPath ypath.Path) (yt.NodeID, error) {
	return env.YT.CreateObject(env.Ctx, yt.NodeTableReplica, &yt.CreateObjectOptions{
		Attributes: map[string]interface{}{
			"cluster_name": os.Getenv("YT_ID"),
			"table_path":   tablePath,
			"replica_path": replicaPath,
		},
	})
}

func createReplicaTable(env *yttest.Env, path ypath.Path, replicaID yt.NodeID) (id yt.NodeID, err error) {
	id, err = env.YT.CreateNode(env.Ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"schema":              schema.MustInfer(&testRow{}),
			"dynamic":             true,
			"upstream_replica_id": replicaID,
		},
	})
	if err != nil {
		return
	}
	err = migrate.MountAndWait(env.Ctx, env.YT, path)
	return
}

func ensureTableReplicaEnabled(env *yttest.Env, id yt.NodeID) error {
	if err := env.YT.AlterTableReplica(env.Ctx, id, &yt.AlterTableReplicaOptions{
		Enabled: ptr.Bool(true),
	}); err != nil {
		return err
	}
	return waitTableReplicaState(env, id, "enabled")
}

func waitTableReplicaState(env *yttest.Env, id yt.NodeID, target string) error {
	if !WaitFor(time.Second*60, func() bool {
		return getTableReplicaState(env, id) == target
	}) {
		return xerrors.Errorf("table replica state wait timed out")
	}
	return nil
}

func getTableReplicaState(env *yttest.Env, id yt.NodeID) string {
	var state string
	if err := env.YT.GetNode(env.Ctx, id.YPath().Attr("state"), &state, nil); err != nil {
		return ""
	}
	return state
}

func WaitFor(timeout time.Duration, f func() bool) bool {
	end := time.Now().Add(timeout)

	const maxSleep = time.Second
	sleepFor := time.Millisecond
	for {
		if f() {
			return true
		}

		if time.Now().After(end) {
			return false
		}

		time.Sleep(sleepFor)
		if sleepFor < maxSleep {
			sleepFor *= 2
		}
	}
}
