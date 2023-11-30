package app

import (
	"time"

	"go.ytsaurus.tech/yt/chyt/controller/internal/agent"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

// Config is taken from yson config file. It contains both strawberry-specific options
// and controller-specific options; at this point they are opaque and passed as raw YSON.
type Config struct {
	// Token of the user for coordination, operation and state management.
	// If not present, it is taken from STRAWBERRY_TOKEN env var.
	Token string `yson:"token"`

	// CoordinationProxy defines coordination cluster if it is needed; e.g. locke.
	CoordinationProxy *string `yson:"coordination_proxy"`

	// CoordinationPath is the path for a lock at the coordination cluster.
	CoordinationPath ypath.Path `yson:"coordination_path"`

	// LocationProxies defines operating clusters; e.g. hume or localhost:4243.
	LocationProxies []string `yson:"location_proxies"`

	// Strawberry contains strawberry-specific configuration.
	Strawberry agent.Config `yson:"strawberry"`

	// Controllers contains a mapping from controller family to opaque controller configs.
	Controllers map[string]yson.RawValue `yson:"controllers"`

	// TODO(max42): deprecate option below.

	// Controller is a legacy way for defining CHYT controller config.
	Controller yson.RawValue `yson:"controller"`

	HTTPAPIEndpoint        *string `yson:"http_api_endpoint"`
	HTTPMonitoringEndpoint *string `yson:"http_monitoring_endpoint"`
	// HTTPControllerMappings contains rules of mapping a host to a controller family.
	// See https://github.com/go-chi/hostrouter/blob/master/README.md for key examples.
	HTTPControllerMappings map[string]string `yson:"http_controller_mappings"`
	// HTTPLocationAliases contains aliases for location proxies,
	// under which the location is accessible through http api.
	HTTPLocationAliases map[string][]string `yson:"http_location_aliases"`

	// HealthStatusExpirationPeriod defines when agent health status becomes outdated.
	HealthStatusExpirationPeriod *time.Duration `yson:"health_status_expiration_period"`

	BaseACL []yt.ACE `yson:"base_acl"`

	DisableAPIAuth bool `yson:"disable_api_auth"`
}

const (
	DefaultHealthStatusExpirationPeriod = time.Duration(time.Minute)
	DefaultHTTPAPIEndpoint              = ":80"
	DefaultHTTPMonitoringEndpoint       = ":2223"
)

func (c *Config) HealthStatusExpirationPeriodOrDefault() time.Duration {
	if c.HealthStatusExpirationPeriod != nil {
		return *c.HealthStatusExpirationPeriod
	}
	return DefaultHealthStatusExpirationPeriod
}

func (c *Config) HTTPAPIEndpointOrDefault() string {
	if c.HTTPAPIEndpoint != nil {
		return *c.HTTPAPIEndpoint
	}
	return DefaultHTTPAPIEndpoint
}

func (c *Config) HTTPMonitoringEndpointOrDefault() string {
	if c.HTTPMonitoringEndpoint != nil {
		return *c.HTTPMonitoringEndpoint
	}
	return DefaultHTTPMonitoringEndpoint
}
