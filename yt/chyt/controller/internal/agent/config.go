package agent

import (
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
)

// Config contains strawberry-specific configuration.
type Config struct {
	// Root points to root directory with operation states.
	Root ypath.Path `yson:"root"`

	// PassPeriod defines how often agent performs its passes.
	PassPeriod *yson.Duration `yson:"pass_period"`
	// RevisionCollectPeriod defines how often agent collects Cypress node revisions via batch ListNode.
	RevisionCollectPeriod *yson.Duration `yson:"revision_collect_period"`

	// Stage of the controller, e.g. production, prestable, etc.
	Stage string `yson:"stage"`

	// RobotUsername is the name of the robot from which all the operations are started.
	// It is used to check permission to the pool during seting "pool" option.
	RobotUsername string `yson:"robot_username"`

	// PassWorkerNumber is the number of workers used to process oplets.
	PassWorkerNumber *int `yson:"pass_worker_number"`

	// DefaultNetworkProject is the default network project used for oplets.
	DefaultNetworkProject *string `yson:"default_network_project"`

	// ClusterURLTemplate is a template executed via text/template library to get cluster URL.
	// Cluster URL is used for generating "fancy" links for debug purposes only.
	// If template parsing or execution fails, panic is called.
	// Available template parameters: Proxy.
	// E.g. "https://example.com/{{.Proxy}}"
	ClusterURLTemplate string `yson:"cluster_url_template"`
}

const (
	DefaultPassPeriod            = yson.Duration(5000)
	DefaultRevisionCollectPeriod = yson.Duration(5000)
	DefaultPassWorkerNumber      = 1
)

func (c *Config) PassPeriodOrDefault() yson.Duration {
	if c.PassPeriod != nil {
		return *c.PassPeriod
	}
	return DefaultPassPeriod
}

func (c *Config) RevisionCollectPeriodOrDefault() yson.Duration {
	if c.RevisionCollectPeriod != nil {
		return *c.RevisionCollectPeriod
	}
	return DefaultRevisionCollectPeriod
}

func (c *Config) PassWorkerNumberOrDefault() int {
	if c.PassWorkerNumber != nil {
		return *c.PassWorkerNumber
	}
	return DefaultPassWorkerNumber
}
