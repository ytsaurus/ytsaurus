package dq

import (
	"context"
	"strconv"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type runtimePaths struct {
	StderrTable ypath.Path
	CoreTable   ypath.Path
}

func (c *Controller) prepareBlobTable(ctx context.Context, kind string, root ypath.Path, incarnationIndex int, expirationTimeout uint64) (ypath.Path, error) {
	path := root.Child(kind).Child(strconv.Itoa(incarnationIndex))
	_, err := c.ytc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Force:     true,
		Recursive: true,
		Attributes: map[string]any{
			"expiration_timeout": expirationTimeout,
		},
	})
	return path, err
}

func (c *Controller) prepareRuntime(ctx context.Context, spec runtimeDataSpec, alias string, incarnationIndex int) (runtimePaths, error) {
	root := ypath.Path(spec.RuntimeDataPath).Child(alias)
	var paths runtimePaths

	stderrTable, err := c.prepareBlobTable(ctx, "stderrs", root, incarnationIndex, spec.RuntimeDataExpirationTimeout)
	if err != nil {
		return paths, err
	}
	paths.StderrTable = stderrTable

	coreTable, err := c.prepareBlobTable(ctx, "cores", root, incarnationIndex, spec.RuntimeDataExpirationTimeout)
	if err != nil {
		return paths, err
	}
	paths.CoreTable = coreTable

	return paths, nil
}
