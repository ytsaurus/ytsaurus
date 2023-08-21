package agent

import (
	"sync"
	"time"

	"go.ytsaurus.tech/yt/chyt/controller/internal/monitoring"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type HealthState struct {
	status monitoring.HealthStatus
	m      sync.Mutex
}

func NewHealthState() *HealthState {
	return &HealthState{
		status: monitoring.HealthStatus{
			ModificationTime: time.Now(),
			Err:              yterrors.Err("health status is not set"),
		},
		m: sync.Mutex{},
	}
}

func (s *HealthState) Store(err error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.status.ModificationTime = time.Now()
	s.status.Err = err
}

func (s *HealthState) Load() monitoring.HealthStatus {
	s.m.Lock()
	defer s.m.Unlock()
	return s.status
}
