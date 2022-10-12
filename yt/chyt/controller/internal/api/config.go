package api

import (
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type APIConfig struct {
	Family        string
	Stage         string
	Root          ypath.Path
	BaseACL       []yt.ACE
	RobotUsername string
}

type HTTPAPIConfig struct {
	APIConfig

	Clusters    []string
	Token       string
	DisableAuth bool
	Endpoint    string
}
