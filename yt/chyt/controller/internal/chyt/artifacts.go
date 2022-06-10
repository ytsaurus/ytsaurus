package chyt

import (
	"context"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

const (
	CHYTBinaryDirectory       = "//sys/bin/ytserver-clickhouse"
	LogTailerBinaryDirectory  = "//sys/bin/ytserver-log-tailer"
	TrampolineBinaryDirectory = "//sys/bin/clickhouse-trampoline"
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

type simpleArtifact struct {
	name string
	path ypath.Path
}

type versionedArtifact struct {
	name           string
	directory      ypath.Path
	version        *string
	defaultVersion string
}

func (a versionedArtifact) toSimpleArtifact() simpleArtifact {
	version := a.defaultVersion
	if a.version != nil {
		version = *a.version
	}
	return simpleArtifact{a.name, a.directory.Child(version)}
}

func (c *Controller) buildArtifact(ctx context.Context, artifact simpleArtifact) (path ypath.Rich, err error) {
	path.FileName = artifact.name
	path.Path, err = c.resolveSymlink(ctx, artifact.path)
	return
}

func (c *Controller) appendArtifacts(ctx context.Context, speclet *Speclet, filePaths *[]ypath.Rich, description *map[string]interface{}) (err error) {
	versionedArtifacts := []versionedArtifact{
		{"ytserver-clickhouse", CHYTBinaryDirectory, speclet.CHYTVersion, DefaultCHYTVersion},
		{"ytserver-log-tailer", LogTailerBinaryDirectory, speclet.LogTailerVersion, DefaultLogTailerVersion},
		{"clickhouse-trampoline", TrampolineBinaryDirectory, speclet.TrampolineVersion, DefaultTrampolineVersion},
	}

	artifacts := []simpleArtifact{}

	for _, artifact := range versionedArtifacts {
		artifacts = append(artifacts, artifact.toSimpleArtifact())
	}

	enableGeoData := DefaultEnableGeoData
	if speclet.EnableGeoData != nil {
		enableGeoData = true
	}

	if enableGeoData {
		geodataPath := DefaultGeoDataPath
		if speclet.GeoDataPath != nil {
			geodataPath = *speclet.GeoDataPath
		}
		artifacts = append(artifacts, simpleArtifact{"geodata.gz", geodataPath})
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
