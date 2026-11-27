package dq

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const serviceConfigFile = "dq_service_process.yson"

func (c *Controller) artifactDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("artifacts")
}

func (c *Controller) ensureArtifactDir(ctx context.Context, alias string) error {
	_, err := c.ytc.CreateNode(ctx, c.artifactDir(alias), yt.NodeMap, &yt.CreateNodeOptions{
		IgnoreExisting: true,
		Recursive:      true,
	})
	return err
}

func (c *Controller) uploadTextArtifact(ctx context.Context, alias string, filename string, content string) (ypath.Rich, error) {
	path := c.artifactDir(alias).Child(filename)
	_, err := c.ytc.CreateNode(ctx, path, yt.NodeFile, &yt.CreateNodeOptions{IgnoreExisting: true})
	if err != nil {
		return ypath.Rich{}, err
	}
	w, err := c.ytc.WriteFile(ctx, path, nil)
	if err != nil {
		return ypath.Rich{}, err
	}
	if _, err = w.Write([]byte(content)); err != nil {
		return ypath.Rich{}, err
	}
	if err = w.Close(); err != nil {
		return ypath.Rich{}, err
	}
	return ypath.Rich{Path: path, FileName: filename}, nil
}
