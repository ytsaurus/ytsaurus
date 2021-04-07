package chyt

import (
	"context"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

func (c *Controller) resolveSymlink(path ypath.Path) (target ypath.Path, err error) {
	var nodeType yt.NodeType
	err = c.ytc.GetNode(context.TODO(), path.SuppressSymlink().Attr("type"), &nodeType, nil)
	if err != nil {
		return
	}
	if nodeType != yt.NodeLink {
		target = path
		return
	}
	c.l.Debug("artifact defined by symlink, resolving it", log.String("path", target.String()))
	err = c.ytc.GetNode(context.TODO(), path.Attr("path"), &target, nil)
	c.l.Debug("symlink resolved", log.String("target_path", target.String()))
	return
}

func (c *Controller) buildArtifact(key string, spec *ArtifactSpec, defaultPath ypath.Path) (path ypath.Rich, err error) {
	path.FileName = key
	if spec == nil || spec.Path == nil {
		path.Path, err = c.resolveSymlink(defaultPath)
	} else {
		path.Path, err = c.resolveSymlink(*spec.Path)
	}
	return
}

type artifact struct {
	name        string
	spec        *ArtifactSpec
	defaultPath ypath.Path
}

func (c *Controller) appendArtifacts(speclet *Speclet, filePaths *[]ypath.Rich, description *map[string]interface{}) (err error) {
	artifacts := []artifact{
		{"ytserver-clickhouse", speclet.YTServerClickHouse, "//sys/bin/ytserver-clickhouse/ytserver-clickhouse"},
		{"ytserver-log-tailer", speclet.YTServerLogTailer, "//sys/bin/ytserver-log-tailer/ytserver-log-tailer"},
		{"clickhouse-trampoline", speclet.ClickHouseTrampoline, "//sys/bin/clickhouse-trampoline/clickhouse-trampoline"},
		{"geodata.tgz", speclet.GeoData, "//sys/clickhouse/geodata/geodata.tgz"},
	}

	var artifactDescription = map[string]yson.RawValue{}

	for _, artifact := range artifacts {
		var path ypath.Rich
		path, err = c.buildArtifact(artifact.name, artifact.spec, artifact.defaultPath)
		if err != nil {
			return
		}
		*filePaths = append(*filePaths, path)
		err = appendArtifactDescription(&artifactDescription, c.ytc, artifact.name, path.Path)
		if err != nil {
			return
		}
	}

	(*description)["artifacts"] = artifactDescription

	return
}
