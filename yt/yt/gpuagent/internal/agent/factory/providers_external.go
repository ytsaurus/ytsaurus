package factory

import (
	"fmt"

	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/yt/gpuagent/internal/agent"
	"go.ytsaurus.tech/yt/yt/gpuagent/internal/agent/nv"
)

type ProviderType string

const (
	ProviderTypeNVIDIA ProviderType = "nvidia"
)

const ProviderTypeUsage = `GPU provider backend: "nvidia" (NVML)`

const ProviderTypeDefault = string(ProviderTypeNVIDIA)

func ValidateProviderType(p ProviderType) error {
	if p != ProviderTypeNVIDIA {
		return fmt.Errorf("invalid gpu-provider %q: must be %q", p, ProviderTypeNVIDIA)
	}
	return nil
}

func NewGPUProvider(providerType ProviderType, debug bool, l *logzap.Logger) (agent.GPUProvider, error) {
	if debug {
		return agent.NewDummyGPUProvider(l)
	}
	return nv.New(l)
}
