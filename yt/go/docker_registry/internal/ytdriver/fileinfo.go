package yt

import (
	"time"

	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

type statAttrs struct {
	Type                 yt.NodeType `yson:"type"`
	ModificationTime     yson.Time   `yson:"modification_time"`
	UncompressedDataSize int64       `yson:"uncompressed_data_size"`
}

type fileInfo struct {
	path string
	stat statAttrs
}

func (fi fileInfo) Path() string {
	return fi.path
}

func (fi fileInfo) Size() int64 {
	return fi.stat.UncompressedDataSize
}

func (fi fileInfo) ModTime() time.Time {
	return time.Time(fi.stat.ModificationTime)
}

func (fi fileInfo) IsDir() bool {
	isDir := false
	if fi.stat.Type == "map_node" {
		isDir = true
	}
	return isDir
}
