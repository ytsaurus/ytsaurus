package mapreduce

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/mapreduce/spec"
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

func requireRichAttrsPath(t *testing.T, path ypath.YPath) {
	t.Helper()

	rich, ok := path.(*ypath.Rich)
	require.True(t, ok)
	require.Equal(t, ypath.Path("//tmp/input/@"), rich.Path)
	require.Equal(t, "other", rich.Cluster)
}

func TestGetInputTableSchemasPreservesRichPathAttributes(t *testing.T) {
	getNodeErr := errors.New("stop after recording GetNode")
	yc := &recordingGetNodeClient{err: getNodeErr}
	p := &prepare{
		mr:  &client{yc: yc},
		ctx: context.Background(),
		spec: &spec.Spec{InputTablePaths: []ypath.YPath{
			ypath.NewRich("//tmp/input").SetCluster("other"),
		}},
	}

	_, err := p.getInputTableSchemas()
	require.ErrorIs(t, err, getNodeErr)
	require.Len(t, yc.paths, 1)
	requireRichAttrsPath(t, yc.paths[0])
}

func TestPrepareInputValidationPreservesRichPathAttributes(t *testing.T) {
	getNodeErr := errors.New("stop after recording GetNode")
	yc := &recordingGetNodeClient{err: getNodeErr}
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
	require.ErrorIs(t, err, getNodeErr)
	require.Len(t, yc.paths, 1)
	requireRichAttrsPath(t, yc.paths[0])
}
