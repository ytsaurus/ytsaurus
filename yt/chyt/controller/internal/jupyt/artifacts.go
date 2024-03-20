package jupyt

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	VenvDirectory             = ypath.Path("//sys/bin/jupyter-venv")
	TrampolineBinaryDirectory = ypath.Path("//sys/bin/jupyter-trampoline")
)

func (c *Controller) resolveSymlink(ctx context.Context, path ypath.Path) (target ypath.Path, err error) {
	var nodeType yt.NodeType
	err = c.ytc.GetNode(ctx, path.SuppressSymlink().Attr("type"), &nodeType, nil)
	if err != nil {
		return
	}
	if nodeType != yt.NodeLink {
		target = path
		return
	}
	c.l.Debug("artifact defined by symlink, resolving it", log.String("path", target.String()))
	err = c.ytc.GetNode(ctx, path.Attr("path"), &target, nil)
	c.l.Debug("symlink resolved", log.String("target_path", target.String()))
	return
}

type artifact struct {
	name string
	path ypath.Path
}

func (c *Controller) buildArtifact(ctx context.Context, artifact artifact) (path ypath.Rich, err error) {
	path.FileName = artifact.name
	path.Path, err = c.resolveSymlink(ctx, artifact.path)
	return
}

func (c *Controller) appendArtifacts(ctx context.Context, speclet *Speclet, filePaths *[]ypath.Rich, description *map[string]any) (err error) {
	artifacts := []artifact{
		{"jupyter-trampoline.sh", TrampolineBinaryDirectory.Child(speclet.TrampolinePathOrDefault())},
	}
	if speclet.VenvPath != nil {
		artifacts = append(artifacts, artifact{"venv.tgz", VenvDirectory.Child(*speclet.VenvPath)})
	}

	var artifactDescription = map[string]yson.RawValue{}

	for _, artifact := range artifacts {
		var path ypath.Rich
		path, err = c.buildArtifact(ctx, artifact)
		if err != nil {
			return
		}
		*filePaths = append(*filePaths, path)
		err = appendArtifactDescription(ctx, &artifactDescription, c.ytc, artifact.name, path.Path)
		if err != nil {
			return
		}
	}

	(*description)["artifacts"] = artifactDescription

	return
}