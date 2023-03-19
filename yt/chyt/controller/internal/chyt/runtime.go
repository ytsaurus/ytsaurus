package chyt

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

func (c *Controller) prepareBlobTable(ctx context.Context, kind string, root ypath.Path, incarnationIndex int) (path ypath.Path, err error) {
	path = root.Child(kind).Child(strconv.Itoa(incarnationIndex))
	_, err = c.ytc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{Force: true, Recursive: true})
	return
}

func (c *Controller) prepareRuntime(ctx context.Context, root ypath.Path, alias string, incarnationIndex int) (paths runtimePaths, err error) {
	paths.StderrTable, err = c.prepareBlobTable(ctx, "stderrs", root, incarnationIndex)
	if err != nil {
		return
	}
	paths.CoreTable, err = c.prepareBlobTable(ctx, "cores", root, incarnationIndex)
	if err != nil {
		return
	}
	return
}
