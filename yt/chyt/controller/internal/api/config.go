package api

import (
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

type APIConfig struct {
	ControllerFactory strawberry.ControllerFactory
	ControllerConfig  yson.RawValue
	AgentInfo         strawberry.AgentInfo
	BaseACL           []yt.ACE
	RobotUsername     string
}

type HTTPAPIConfig struct {
	APIConfig

	Clusters    []string
	Token       string
	DisableAuth bool
	Endpoint    string
}
