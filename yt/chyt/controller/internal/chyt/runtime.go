package chyt

import (
	"context"
	"strconv"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

var defaultRuntimeRoot ypath.Path = "//sys/clickhouse/kolkhoz"

type runtimePaths struct {
	StderrTable ypath.Path
	CoreTable   ypath.Path
}

func (c *Controller) prepareBlobTable(kind string, root ypath.Path, incarnationIndex int) (path ypath.Path, err error) {
	path = root.Child(kind).Child(strconv.Itoa(incarnationIndex))
	_, err = c.ytc.CreateNode(context.TODO(), path, yt.NodeTable, &yt.CreateNodeOptions{Force: true, Recursive: true})
	return
}

func (c *Controller) prepareRuntime(root ypath.Path, alias string, incarnationIndex int) (paths runtimePaths, err error) {
	paths.StderrTable, err = c.prepareBlobTable("stderrs", root, incarnationIndex)
	if err != nil {
		return
	}
	paths.CoreTable, err = c.prepareBlobTable("cores", root, incarnationIndex)
	if err != nil {
		return
	}
	return
}
