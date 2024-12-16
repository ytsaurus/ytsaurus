package jupyt

import (
	"time"

	"go.ytsaurus.tech/yt/go/yson"
)

type Speclet struct {
	CPU    *uint64 `yson:"cpu"`
	Memory *uint64 `yson:"memory"`
	GPU    *uint64 `yson:"gpu"`

	JupyterDockerImage   string            `yson:"jupyter_docker_image"`
	IdleTimeout          *yson.Duration    `yson:"idle_timeout" requires_restart:"false"`
	EnableIdleSuspension bool              `yson:"enable_idle_suspension" requires_restart:"false"`
	EnvVars              map[string]string `yson:"env_vars"`
}

const (
	gib                = 1024 * 1024 * 1024
	DefaultCPU         = 2
	DefaultMemory      = 8 * gib
	DefaultGPU         = 0
	DefaultIdleTimeout = 24 * time.Hour
)

func (speclet *Speclet) CPUOrDefault() uint64 {
	if speclet.CPU != nil {
		return *speclet.CPU
	}
	return DefaultCPU
}

func (speclet *Speclet) MemoryOrDefault() uint64 {
	if speclet.Memory != nil {
		return *speclet.Memory
	}
	return DefaultMemory
}

func (speclet *Speclet) GPUOrDefault() uint64 {
	if speclet.GPU != nil {
		return *speclet.GPU
	}
	return DefaultGPU
}

func (speclet *Speclet) IdleTimeoutOrDefault() time.Duration {
	if speclet.IdleTimeout != nil {
		return time.Duration(*speclet.IdleTimeout)
	}
	return DefaultIdleTimeout
}
