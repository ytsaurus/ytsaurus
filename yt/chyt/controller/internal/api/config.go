package api

import (
	"a.yandex-team.ru/yt/go/ypath"
)

type APIConfig struct {
	Family string
	Stage  string
	Root   ypath.Path
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
