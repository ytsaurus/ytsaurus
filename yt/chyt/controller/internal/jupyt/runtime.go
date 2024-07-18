package jupyt

import (
	"context"
	"fmt"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

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

	return nil
}
