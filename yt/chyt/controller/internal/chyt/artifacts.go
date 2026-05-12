package chyt

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	CHYTBinaryDirectory       = ypath.Path("//sys/bin/ytserver-clickhouse")
	TrampolineBinaryDirectory = ypath.Path("//sys/bin/clickhouse-trampoline")
)

type chytBinaryInfo struct {
	NodeId          string      `yson:"id,attr"`
	Version         string      `yson:"version,attr"`
	ContentRevision yt.Revision `yson:"content_revision,attr"`
}

func (c *Controller) getChytBinaryInfo(ctx context.Context, path ypath.Path) (info chytBinaryInfo, err error) {
	options := yt.GetNodeOptions{Attributes: []string{"id", "version", "content_revision"}}
	err = c.ytc.GetNode(ctx, path, &info, &options)
	if err != nil {
		return
	}
	return
}

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

func (c *Controller) resolveChytVersion(ctx context.Context, path ypath.Path, chytVersion *chytOpletInfo) {
	chytVersion.CHYTRunningVersionPath = path.String()
	info, err := c.getChytBinaryInfo(ctx, path)
	if err != nil {
		chytVersion.CHYTRunningVersion = "unknown"
		chytVersion.BinaryNodeId = nil
		chytVersion.BinaryRevision = nil
		return
	}

	chytVersion.CHYTRunningVersion = info.Version
	chytVersion.BinaryNodeId = &info.NodeId
	chytVersion.BinaryRevision = &info.ContentRevision
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

func (c *Controller) appendOpArtifacts(ctx context.Context, speclet *Speclet, filePaths *[]ypath.Rich, description *map[string]any, chytVersion *chytOpletInfo) (err error) {
	artifacts := []artifact{
		{"ytserver-clickhouse", CHYTBinaryDirectory.Child(speclet.CHYTVersionOrDefault())},
		{"clickhouse-trampoline", TrampolineBinaryDirectory.Child(speclet.TrampolineVersionOrDefault())},
	}

	if speclet.EnableGeodataOrDefault(c.config.EnableGeodataOrDefault()) {
		artifacts = append(artifacts, artifact{"geodata.tgz", speclet.GeodataPathOrDefault()})
	}

	var artifactDescription = map[string]yson.RawValue{}

	for _, artifact := range artifacts {
		var path ypath.Rich
		path, err = c.buildArtifact(ctx, artifact)
		if err != nil {
			return
		}
		if artifact.name == "ytserver-clickhouse" {
			c.resolveChytVersion(ctx, path.Path, chytVersion)
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
