package jupyt

import (
	"go.ytsaurus.tech/yt/go/yson"
	"time"
)

type Speclet struct {
	CPU    *uint64 `yson:"cpu"`
	Memory *uint64 `yson:"memory"`

	JupyterDockerImage   string         `yson:"jupyter_docker_image"`
	IdleTimeout          *yson.Duration `yson:"idle_timeout" requires_restart:"false"`
	EnableIdleSuspension bool           `yson:"enable_idle_suspension" requires_restart:"false"`
}

const (
	gib                = 1024 * 1024 * 1024
	DefaultCPU         = 2
	DefaultMemory      = 8 * gib
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

func (speclet *Speclet) IdleTimeoutOrDefault() time.Duration {
	if speclet.IdleTimeout != nil {
		return time.Duration(*speclet.IdleTimeout)
	}
	return DefaultIdleTimeout
}
