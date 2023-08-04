package api

import (
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/yt"
)

type APIConfig struct {
	// ControllerFactories contains mapping from controller families to controller factories.
	ControllerFactories map[string]strawberry.ControllerFactory
	// ControllerMappings contains rules of mapping host to a particular controller.
	// See https://github.com/go-chi/hostrouter/blob/master/README.md for key examples.
	ControllerMappings map[string]string
	AgentInfo          strawberry.AgentInfo
	BaseACL            []yt.ACE
	RobotUsername      string
	ValidatePoolAccess *bool
}

const (
	DefaultValidatePool = true
)

func (c *APIConfig) ValidatePoolAccessOrDefault() bool {
	if c.ValidatePoolAccess != nil {
		return *c.ValidatePoolAccess
	}
	return DefaultValidatePool
}

func (c *APIConfig) ControllerMappingsOrDefault() map[string]string {
	if len(c.ControllerMappings) == 0 {
		return map[string]string{"*": "chyt"}
	}
	return c.ControllerMappings
}

type HTTPAPIConfig struct {
	BaseAPIConfig APIConfig

	ClusterInfos []strawberry.AgentInfo
	Token        string
	DisableAuth  bool
	Endpoint     string
}
