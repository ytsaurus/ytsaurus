package integration

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestReplicatedTables(t *testing.T) {
	suite := NewSuite(t)

	RunClientTests(t, []ClientTest{
		{Name: "GetInSyncReplicas", Test: suite.TestGetInSyncReplicas},
	})
}

func (s *Suite) TestGetInSyncReplicas(t *testing.T, yc yt.Client) {
	t.Parallel()

	tablePath := tmpPath()
	_, err := createReplicatedTable(s.Ctx, yc, tablePath)
	require.NoError(t, err)

	replicaPath1 := tmpPath()
	replicaID1, err := createTableReplica(s.Ctx, yc, tablePath, replicaPath1)
	require.NoError(t, err)

	replicaPath2 := tmpPath()
	replicaID2, err := createTableReplica(s.Ctx, yc, tablePath, replicaPath2)
	require.NoError(t, err)

	_, err = createReplicaTable(s.Ctx, yc, replicaPath1, replicaID1)
	require.NoError(t, err)
	_, err = createReplicaTable(s.Ctx, yc, replicaPath2, replicaID2)
	require.NoError(t, err)

	require.NoError(t, ensureTableReplicaEnabled(s.Ctx, yc, replicaID1))

	require.Equal(t, "enabled", getTableReplicaState(s.Ctx, yc, replicaID1))
	require.Equal(t, "disabled", getTableReplicaState(s.Ctx, yc, replicaID2))

	keys := []any{
		&testKey{"foo"},
		&testKey{"bar"},
	}

	rows := []any{
		&testRow{"foo", "1"},
		&testRow{"bar", "2"},
	}

	require.NoError(t, yc.InsertRows(s.Ctx, tablePath, rows, &yt.InsertRowsOptions{
		RequireSyncReplica: ptr.Bool(false),
	}))

	ts, err := yc.GenerateTimestamp(s.Ctx, nil)
	require.NoError(t, err)

	ids, err := yc.GetInSyncReplicas(s.Ctx, tablePath, ts, nil, nil)
	require.NoError(t, err)
	require.Equal(t, NodeIDSlice{replicaID1, replicaID2}.MakeSet(), NodeIDSlice(ids).MakeSet())

	require.True(t, WaitFor(time.Second*60, func() bool {
		ids, err = yc.GetInSyncReplicas(s.Ctx, tablePath, ts, keys, nil)
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

func createReplicatedTable(ctx context.Context, yc yt.Client, path ypath.Path) (id yt.NodeID, err error) {
	id, err = yc.CreateNode(ctx, path, yt.NodeReplicatedTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"schema":                     schema.MustInfer(&testRow{}),
			"enable_replication_logging": true,
			"dynamic":                    true,
		},
	})
	if err != nil {
		return
	}
	err = migrate.MountAndWait(ctx, yc, path)
	return
}

func createTableReplica(ctx context.Context, yc yt.Client, tablePath, replicaPath ypath.Path) (yt.NodeID, error) {
	return yc.CreateObject(ctx, yt.NodeTableReplica, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"cluster_name": os.Getenv("YT_ID"),
			"table_path":   tablePath,
			"replica_path": replicaPath,
		},
	})
}

func createReplicaTable(ctx context.Context, yc yt.Client, path ypath.Path, replicaID yt.NodeID) (id yt.NodeID, err error) {
	id, err = yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"schema":              schema.MustInfer(&testRow{}),
			"dynamic":             true,
			"upstream_replica_id": replicaID,
		},
	})
	if err != nil {
		return
	}
	err = migrate.MountAndWait(ctx, yc, path)
	return
}

func ensureTableReplicaEnabled(ctx context.Context, yc yt.Client, id yt.NodeID) error {
	if err := yc.AlterTableReplica(ctx, id, &yt.AlterTableReplicaOptions{
		Enabled: ptr.Bool(true),
	}); err != nil {
		return err
	}
	return waitTableReplicaState(ctx, yc, id, "enabled")
}

func waitTableReplicaState(ctx context.Context, yc yt.Client, id yt.NodeID, target string) error {
	if !WaitFor(time.Second*60, func() bool {
		return getTableReplicaState(ctx, yc, id) == target
	}) {
		return xerrors.Errorf("table replica state wait timed out")
	}
	return nil
}

func getTableReplicaState(ctx context.Context, yc yt.Client, id yt.NodeID) string {
	var state string
	if err := yc.GetNode(ctx, id.YPath().Attr("state"), &state, nil); err != nil {
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
