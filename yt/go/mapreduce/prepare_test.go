package mapreduce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type recordingGetNodeClient struct {
	yt.Client

	paths       []ypath.YPath
	clusterName string
	err         error
}

func (c *recordingGetNodeClient) GetNode(
	_ context.Context,
	path ypath.YPath,
	result any,
	_ *yt.GetNodeOptions,
) error {
	c.paths = append(c.paths, path)
	if path.YPath() == ypath.Path("//sys/@cluster_name") {
		*result.(*string) = c.clusterName
		return nil
	}
	return c.err
}

func TestGetPathCluster(t *testing.T) {
	for _, tc := range []struct {
		name        string
		path        ypath.YPath
		wantCluster string
		wantOK      bool
	}{
		{name: "plain path", path: ypath.Path("//tmp/input")},
		{name: "rich path", path: ypath.NewRich("//tmp/input")},
		{name: "rich path with cluster", path: ypath.NewRich("//tmp/input").SetCluster("other"), wantCluster: "other", wantOK: true},
		{name: "encoded path with cluster", path: ypath.Path(`<cluster="other">//tmp/input`), wantCluster: "other", wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster, ok := getPathCluster(tc.path)
			require.Equal(t, tc.wantCluster, cluster)
			require.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestGetInputTableSchemasSkipsRemotePathsAndFetchesClusterNameOnce(t *testing.T) {
	yc := &recordingGetNodeClient{
		clusterName: "local",
		err:         errors.New("input GetNode must not be called"),
	}
	p := &prepare{
		mr:  &client{yc: yc},
		ctx: context.Background(),
		spec: &spec.Spec{InputTablePaths: []ypath.YPath{
			ypath.NewRich("//tmp/input-1").SetCluster("other"),
			ypath.NewRich("//tmp/input-2").SetCluster("other"),
		}},
	}

	schemas, err := p.getInputTableSchemas()
	require.NoError(t, err)
	require.Equal(t, []*schema.Schema{nil, nil}, schemas)
	require.Equal(t, []ypath.YPath{ypath.Path("//sys/@cluster_name")}, yc.paths)
}

func TestPrepareInputValidationSkipsRemotePaths(t *testing.T) {
	yc := &recordingGetNodeClient{
		clusterName: "local",
		err:         errors.New("input GetNode must not be called"),
	}
	mr := New(yc).(*client)
	p := &prepare{
		mr:  mr,
		ctx: context.Background(),
		spec: &spec.Spec{
			Type: yt.OperationSort,
			InputTablePaths: []ypath.YPath{
				ypath.NewRich("//tmp/input").SetCluster("other"),
			},
		},
	}

	err := p.prepare(nil)
	require.NoError(t, err)
	require.Equal(t, []ypath.YPath{ypath.Path("//sys/@cluster_name")}, yc.paths)
}

func TestPrepareInputValidationChecksLocalPaths(t *testing.T) {
	getNodeErr := errors.New("stop after recording GetNode")
	yc := &recordingGetNodeClient{err: getNodeErr}
	mr := New(yc).(*client)
	p := &prepare{
		mr:  mr,
		ctx: context.Background(),
		spec: &spec.Spec{
			Type:            yt.OperationSort,
			InputTablePaths: []ypath.YPath{ypath.Path("//tmp/input")},
		},
	}

	err := p.prepare(nil)
	require.ErrorIs(t, err, getNodeErr)
	require.Equal(t, []ypath.YPath{ypath.Path("//tmp/input/@")}, yc.paths)
}

func TestPrepareInputValidationChecksExplicitlyLocalClusterPaths(t *testing.T) {
	getNodeErr := errors.New("stop after recording input GetNode")
	yc := &recordingGetNodeClient{clusterName: "local", err: getNodeErr}
	mr := New(yc).(*client)
	p := &prepare{
		mr:  mr,
		ctx: context.Background(),
		spec: &spec.Spec{
			Type: yt.OperationSort,
			InputTablePaths: []ypath.YPath{
				ypath.NewRich("//tmp/input").SetCluster("local"),
			},
		},
	}

	err := p.prepare(nil)
	require.ErrorIs(t, err, getNodeErr)
	require.Equal(t, []ypath.YPath{
		ypath.Path("//sys/@cluster_name"),
		ypath.Path("//tmp/input/@"),
	}, yc.paths)
}
