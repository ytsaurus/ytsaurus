package api

import (
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type APIConfig struct {
	Family  string
	Stage   string
	Root    ypath.Path
	BaseACL []yt.ACE
}

type HTTPAPIConfig struct {
	APIConfig

	Clusters    []string
	Token       string
	DisableAuth bool
}

type HTTPServerConfig struct {
	HTTPAPIConfig

	Endpoint string
}
