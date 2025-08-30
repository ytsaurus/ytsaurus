package agent

import (
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
)

type DummyGPUProvider interface {
	GPUProvider
}

func NewDummyGPUProvider(l *logzap.Logger) (GPUProvider, error) {
	return &dummyGPUProvider{l: l}, nil
}

type dummyGPUProvider struct {
	l *logzap.Logger
}

func (p *dummyGPUProvider) Init() error {
	p.l.Info("Dummy GPU provider initialized")
	return nil
}

func (p *dummyGPUProvider) Shutdown() {
	p.l.Info("Dummy GPU provider shutdown")
}

func (p *dummyGPUProvider) GetGPUDevices() ([]GPUInfo, error) {
	p.l.Info("Dummy GPU provider get GPU devices")
	return make([]GPUInfo, 0), nil
}
