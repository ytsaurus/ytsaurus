package agent

import (
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
)

// Config contains strawberry-specific configuration.
type Config struct {
	// Root points to root directory with operation states.
	Root ypath.Path `yson:"root"`

	// PassPeriod defines how often agent performs its passes.
	PassPeriod *yson.Duration `yson:"pass_period"`
	// CollectOperationsPeriod defines how often agent collects running operations.
	CollectOperationsPeriod *yson.Duration `yson:"collect_operations_period"`
	// RevisionCollectPeriod defines how often agent collects Cypress node revisions via batch ListNode.
	RevisionCollectPeriod *yson.Duration `yson:"revision_collect_period"`

	// MinOpletPassTimeout is a minimum timeout value to limit the execution time of Oplet.Pass().
	MinOpletPassTimeout *yson.Duration `yson:"min_oplet_pass_timeout"`
	// OpletPassTimeoutFactor is a PassPeriod factor to limit the Oplet.Pass() execution time.
	// Especially the phases of restart operation.
	OpletPassTimeoutFactor *float64 `yson:"oplet_pass_timeout_factor"`

	// HealthCheckerToleranceFactor is a maximum period factor before a health checker
	// considers the state as `expired`. E.g. if the pass period is 5s and the factor is 2.0,
	// the health checker will consider the state valid for 10s.
	HealthCheckerToleranceFactor *float64 `yson:"health_checker_tolerance_factor"`

	// Stage of the controller, e.g. production, prestable, etc.
	Stage string `yson:"stage"`

	// RobotUsername is the name of the robot from which all the operations are started.
	// It is used to check permission to the pool during setting "pool" option.
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

	// AssignAdministerToCreator determines whether the operation creator
	// should be granted the `administer` right to the corresponding ACO.
	AssignAdministerToCreator *bool `yson:"assign_administer_to_creator"`

	// ScaleWorkerNumber is the number of workers used to scale oplets.
	ScaleWorkerNumber *int `yson:"scale_worker_number"`
	// ScalePeriod defines how often agent runs oplet scaling task.
	ScalePeriod *yson.Duration `yson:"scale_period"`
}

const (
	DefaultPassPeriod                   = yson.Duration(5 * time.Second)
	DefaultCollectOperationsPeriod      = yson.Duration(10 * time.Second)
	DefaultRevisionCollectPeriod        = yson.Duration(5 * time.Second)
	DefaultMinOpletPassTimeout          = yson.Duration(10 * time.Second)
	DefaultOpletPassTimeoutFactor       = 1.0
	DefaultHealthCheckerToleranceFactor = 2.0
	DefaultPassWorkerNumber             = 1
	DefaultAssignAdministerToCreator    = true
	DefaultScaleWorkerNumber            = 1
	DefaultScalePeriod                  = yson.Duration(60 * time.Second)
)

func (c *Config) PassPeriodOrDefault() yson.Duration {
	if c.PassPeriod != nil {
		return *c.PassPeriod
	}
	return DefaultPassPeriod
}

func (c *Config) CollectOperationsPeriodOrDefault() yson.Duration {
	if c.CollectOperationsPeriod != nil {
		return *c.CollectOperationsPeriod
	}
	return DefaultCollectOperationsPeriod
}

func (c *Config) RevisionCollectPeriodOrDefault() yson.Duration {
	if c.RevisionCollectPeriod != nil {
		return *c.RevisionCollectPeriod
	}
	return DefaultRevisionCollectPeriod
}

func (c *Config) MinOpletPassTimeoutOrDefault() yson.Duration {
	if c.MinOpletPassTimeout != nil {
		return *c.MinOpletPassTimeout
	}
	return DefaultMinOpletPassTimeout
}

func (c *Config) OpletPassTimeoutFactorOrDefault() float64 {
	if c.OpletPassTimeoutFactor != nil {
		return *c.OpletPassTimeoutFactor
	}
	return DefaultOpletPassTimeoutFactor
}

func (c *Config) HealthCheckerToleranceFactorOrDefault() float64 {
	if c.HealthCheckerToleranceFactor != nil {
		return *c.HealthCheckerToleranceFactor
	}
	return DefaultHealthCheckerToleranceFactor
}

func (c *Config) PassWorkerNumberOrDefault() int {
	if c.PassWorkerNumber != nil {
		return *c.PassWorkerNumber
	}
	return DefaultPassWorkerNumber
}

func (c *Config) AssignAdministerToCreatorOrDefault() bool {
	if c.AssignAdministerToCreator != nil {
		return *c.AssignAdministerToCreator
	}
	return DefaultAssignAdministerToCreator
}

func (c *Config) ScaleWorkerNumberOrDefault() int {
	if c.ScaleWorkerNumber != nil {
		return *c.ScaleWorkerNumber
	}
	return DefaultScaleWorkerNumber
}

func (c *Config) ScalePeriodOrDefault() yson.Duration {
	if c.ScalePeriod != nil {
		return *c.ScalePeriod
	}
	return DefaultScalePeriod
}

func applyOverride[T any](base **T, override *T) {
	if override != nil {
		*base = override
	}
}

func (c *Config) ApplyOverrides(o ConfigOverrides) Config {
	var overridedCfg Config
	if c != nil {
		overridedCfg = *c
	}

	applyOverride(&overridedCfg.PassPeriod, o.PassPeriod)
	applyOverride(&overridedCfg.CollectOperationsPeriod, o.CollectOperationsPeriod)
	applyOverride(&overridedCfg.RevisionCollectPeriod, o.RevisionCollectPeriod)
	applyOverride(&overridedCfg.MinOpletPassTimeout, o.MinOpletPassTimeout)
	applyOverride(&overridedCfg.OpletPassTimeoutFactor, o.OpletPassTimeoutFactor)
	applyOverride(&overridedCfg.HealthCheckerToleranceFactor, o.HealthCheckerToleranceFactor)

	return overridedCfg
}

// ConfigOverrides contains location-specific overrides of Config fields.
// For information about field semantics, see Config struct.
type ConfigOverrides struct {
	PassPeriod *yson.Duration `yson:"pass_period"`

	CollectOperationsPeriod *yson.Duration `yson:"collect_operations_period"`

	RevisionCollectPeriod *yson.Duration `yson:"revision_collect_period"`

	MinOpletPassTimeout *yson.Duration `yson:"min_oplet_pass_timeout"`

	OpletPassTimeoutFactor *float64 `yson:"oplet_pass_timeout_factor"`

	HealthCheckerToleranceFactor *float64 `yson:"health_checker_tolerance_factor"`
}
