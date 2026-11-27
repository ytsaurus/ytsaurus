package dq

import (
	"go.ytsaurus.tech/yt/go/ypath"
)

type Config struct {
	DefaultBinaryCypressPath *ypath.Path `yson:"default_binary_cypress_path"`
	DefaultYTPrefix          *ypath.Path `yson:"default_yt_prefix"`
	DefaultYTUser            *string     `yson:"default_yt_user"`

	EnableDiscoveryHealthCheck *bool `yson:"enable_discovery_health_check"`

	DefaultRuntimeDataPath *ypath.Path `yson:"default_runtime_data_path"`
	EnableRuntimeData      *bool       `yson:"enable_runtime_data"`

	DefaultSpeclet *Speclet `yson:"default_speclet"`
}

const (
	DefaultEnableDiscoveryHealthCheck = true
	DefaultEnableRuntimeData          = true
)

func (c *Config) EnableDiscoveryHealthCheckOrDefault() bool {
	if c.EnableDiscoveryHealthCheck != nil {
		return *c.EnableDiscoveryHealthCheck
	}
	return DefaultEnableDiscoveryHealthCheck
}

func (c *Config) EnableRuntimeDataOrDefault() bool {
	if c.EnableRuntimeData != nil {
		return *c.EnableRuntimeData
	}
	return DefaultEnableRuntimeData
}

func (c *Config) DefaultRuntimeDataPathOrDefault(root ypath.Path) ypath.Path {
	if c.DefaultRuntimeDataPath != nil {
		return *c.DefaultRuntimeDataPath
	}
	return root
}

func (c *Config) DefaultBinaryCypressPathOrDefault() ypath.Path {
	if c.DefaultBinaryCypressPath != nil {
		return *c.DefaultBinaryCypressPath
	}
	return ypath.Path("//home/yql/dq_v2/bin/dq_service_process")
}

func (c *Config) DefaultYTPrefixOrDefault() ypath.Path {
	if c.DefaultYTPrefix != nil {
		return *c.DefaultYTPrefix
	}
	return ypath.Path("//home/yql/dq_v2")
}

func (c *Config) DefaultYTUserOrDefault() string {
	if c.DefaultYTUser != nil {
		return *c.DefaultYTUser
	}
	return "yql_agent"
}
