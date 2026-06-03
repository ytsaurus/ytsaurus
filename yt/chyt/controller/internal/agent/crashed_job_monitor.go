package agent

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.ytsaurus.tech/yt/go/yt"
)

type crashedJobEvent struct {
	jobID          yt.JobID
	expirationTime time.Time
}

type crashedJobMonitor struct {
	eventExpirationTimeout time.Duration

	mu       sync.Mutex
	opEvents map[yt.OperationID][]crashedJobEvent
}

func newCrashedJobMonitor(expirationTimeout time.Duration) *crashedJobMonitor {
	return &crashedJobMonitor{
		eventExpirationTimeout: expirationTimeout,
		opEvents:               make(map[yt.OperationID][]crashedJobEvent),
	}
}

func (m *crashedJobMonitor) registerCrashedJobs(ops []OperationStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()

	expirationTime := time.Now().Add(m.eventExpirationTimeout)
	for _, op := range ops {
		opID := op.ID
		for _, jobID := range op.CrashedJobs {
			m.opEvents[opID] = append(m.opEvents[opID], crashedJobEvent{jobID, expirationTime})
		}
	}
}

func (m *crashedJobMonitor) getCoreAlert() error {
	if m == nil {
		return errors.New("core monitor is not initialized")
	}

	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	cores := make(map[yt.OperationID][]yt.JobID)
	toDelete := make([]yt.OperationID, 0)
	for opID, events := range m.opEvents {
		newLen := 0
		for idx := 0; idx < len(events); idx++ {
			if events[idx].expirationTime.Before(now) {
				continue
			}
			cores[opID] = append(cores[opID], events[idx].jobID)
			events[newLen] = events[idx]
			newLen++
		}
		m.opEvents[opID] = m.opEvents[opID][:newLen]
		if newLen == 0 {
			toDelete = append(toDelete, opID)
		}
	}

	for _, id := range toDelete {
		delete(m.opEvents, id)
	}

	if len(cores) == 0 {
		return nil
	}

	parts := make([]string, 0, len(cores))
	for opID, jobIDs := range cores {
		jobStrs := make([]string, len(jobIDs))
		for i, j := range jobIDs {
			jobStrs[i] = j.String()
		}
		parts = append(parts, fmt.Sprintf("op %v: crashed jobs [%v]", opID, strings.Join(jobStrs, ", ")))
	}

	return errors.New(strings.Join(parts, "; "))
}
