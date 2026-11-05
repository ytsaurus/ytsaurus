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

	paths []ypath.YPath
	err   error
}

func (c *recordingGetNodeClient) GetNode(
	_ context.Context,
	path ypath.YPath,
	_ any,
	_ *yt.GetNodeOptions,
) error {
	c.paths = append(c.paths, path)
	return c.err
}

func TestIsClusterQualifiedPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		path ypath.YPath
		want bool
	}{
		{name: "plain path", path: ypath.Path("//tmp/input")},
		{name: "rich path", path: ypath.NewRich("//tmp/input")},
		{name: "rich path with cluster", path: ypath.NewRich("//tmp/input").SetCluster("other"), want: true},
		{name: "encoded path with cluster", path: ypath.Path(`<cluster="other">//tmp/input`), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isClusterQualifiedPath(tc.path))
		})
	}
}

func TestGetInputTableSchemasSkipsClusterQualifiedPaths(t *testing.T) {
	yc := &recordingGetNodeClient{err: errors.New("GetNode must not be called")}
	p := &prepare{
		mr:  &client{yc: yc},
		ctx: context.Background(),
		spec: &spec.Spec{InputTablePaths: []ypath.YPath{
			ypath.NewRich("//tmp/input").SetCluster("other"),
		}},
	}

	schemas, err := p.getInputTableSchemas()
	require.NoError(t, err)
	require.Equal(t, []*schema.Schema{nil}, schemas)
	require.Empty(t, yc.paths)
}

func TestPrepareInputValidationSkipsClusterQualifiedPaths(t *testing.T) {
	yc := &recordingGetNodeClient{err: errors.New("GetNode must not be called")}
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
	require.Empty(t, yc.paths)
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
