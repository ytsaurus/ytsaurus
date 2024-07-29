package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestTableBackupClient(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "BasicBackup", Test: suite.TestBasicBackup, SkipRPC: true},
	})
}

func (s *Suite) TestBasicBackup(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	var clusterConnection yson.RawValue
	require.NoError(t, yc.GetNode(ctx, ypath.Path("//sys/@cluster_connection"), &clusterConnection, nil))
	require.NoError(t, yc.SetNode(ctx, ypath.Path("//sys/clusters"), map[string]yson.RawValue{"self": clusterConnection}, nil))
	require.NoError(t, yc.SetNode(ctx, ypath.Path("//sys/@config/tablet_manager/enable_backups"), true, nil))

	src := tmpPath().Child("table")
	bak := ypath.Path(src + ".bak")
	res := ypath.Path(src + ".res")

	_, err := yc.CreateNode(ctx, src, yt.NodeTable, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]any{
			"dynamic":                   true,
			"schema":                    schema.MustInfer(&testRow{}),
			"enable_dynamic_store_read": true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, migrate.MountAndWait(ctx, yc, src))

	manifest := yt.BackupManifest{
		Clusters: map[string][]yt.TableBackupManifest{
			"self": {
				{SourcePath: src, DestinationPath: bak},
			},
		},
	}
	require.NoError(t, yc.CreateTableBackup(ctx, manifest, nil))

	ok, err := yc.NodeExists(ctx, bak, nil)
	require.NoError(t, err)
	require.True(t, ok)

	manifest = yt.BackupManifest{
		Clusters: map[string][]yt.TableBackupManifest{
			"self": {
				{SourcePath: bak, DestinationPath: res},
			},
		},
	}
	require.NoError(t, yc.RestoreTableBackup(ctx, manifest, &yt.RestoreTableBackupOptions{
		Mount: true,
	}))
	ok, err = yc.NodeExists(ctx, bak, nil)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, waitTabletState(ctx, yc, res, yt.TabletMounted))
}
