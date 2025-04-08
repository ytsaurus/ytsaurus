package chyt

import (
	"context"
	"fmt"
	"strconv"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type runtimePaths struct {
	StderrTable ypath.Path
	CoreTable   ypath.Path
}

func (c *Controller) prepareBlobTable(ctx context.Context, kind string, root ypath.Path, incarnationIndex int, expirationTimeout uint64) (path ypath.Path, err error) {
	path = root.Child(kind).Child(strconv.Itoa(incarnationIndex))
	_, err = c.ytc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Force:     true,
		Recursive: true,
		Attributes: map[string]any{
			"expiration_timeout": expirationTimeout,
		},
	})
	return
}

func (c *Controller) prepareRuntime(ctx context.Context, spec runtimeDataSpec, alias string, incarnationIndex int) (paths runtimePaths, err error) {
	root := spec.RuntimeDataPath.Child(alias)
	paths.StderrTable, err = c.prepareBlobTable(ctx, "stderrs", root, incarnationIndex, spec.RuntimeDataExpirationTimeout)
	if err != nil {
		return
	}
	paths.CoreTable, err = c.prepareBlobTable(ctx, "cores", root, incarnationIndex, spec.RuntimeDataExpirationTimeout)
	if err != nil {
		return
	}
	return
}

func (c *Controller) createDirIfNotExists(ctx context.Context, dir ypath.Path, opaque bool) error {
	_, err := c.ytc.CreateNode(ctx, dir, yt.NodeMap,
		&yt.CreateNodeOptions{
			IgnoreExisting: true,
			Attributes: map[string]any{
				"opaque": opaque,
			},
		})
	return err
}

func (c *Controller) prepareCypressDirectories(ctx context.Context, alias string) error {
	if err := c.createDirIfNotExists(ctx, c.artifactDir(alias), true /*opaque*/); err != nil {
		return fmt.Errorf("error creating artifact dir: %v", err)
	}

	if err := c.createDirIfNotExists(ctx, c.systemLogTableRootDir(alias), false /*opaque*/); err != nil {
		return fmt.Errorf("error creating system log table root dir: %v", err)
	}

	if err := c.createDirIfNotExists(ctx, c.sqlUDFDir(alias), true /*opaque*/); err != nil {
		return fmt.Errorf("error creating sql udf dir: %v", err)
	}

	return nil
}
