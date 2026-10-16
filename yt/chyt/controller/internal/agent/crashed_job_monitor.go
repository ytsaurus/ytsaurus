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
	opletAlias     string
	expirationTime time.Time
}

type crashedJobBatchEntry struct {
	operationID yt.OperationID
	opletAlias  string
	jobIDs      []yt.JobID
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

func (m *crashedJobMonitor) registerCrashedJobs(batch []crashedJobBatchEntry) map[string]int {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	expirationTime := now.Add(m.eventExpirationTimeout)
	for _, entry := range batch {
		for _, jobID := range entry.jobIDs {
			m.opEvents[entry.operationID] = append(m.opEvents[entry.operationID], crashedJobEvent{
				jobID:          jobID,
				opletAlias:     entry.opletAlias,
				expirationTime: expirationTime,
			})
		}
	}

	counts := make(map[string]int)
	for _, events := range m.getActiveEventsLocked(now) {
		for _, event := range events {
			if event.opletAlias != "" {
				counts[event.opletAlias]++
			}
		}
	}
	return counts
}

func (m *crashedJobMonitor) getActiveEvents() map[yt.OperationID][]crashedJobEvent {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getActiveEventsLocked(now)
}

func (m *crashedJobMonitor) getActiveEventsLocked(now time.Time) map[yt.OperationID][]crashedJobEvent {
	activeEvents := make(map[yt.OperationID][]crashedJobEvent)
	toDelete := make([]yt.OperationID, 0)
	for opID, events := range m.opEvents {
		newLen := 0
		for idx := 0; idx < len(events); idx++ {
			if events[idx].expirationTime.Before(now) {
				continue
			}
			activeEvents[opID] = append(activeEvents[opID], events[idx])
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

	return activeEvents
}

func (m *crashedJobMonitor) getCoreAlert() error {
	if m == nil {
		return errors.New("core monitor is not initialized")
	}

	activeEvents := m.getActiveEvents()
	if len(activeEvents) == 0 {
		return nil
	}

	parts := make([]string, 0, len(activeEvents))
	for opID, events := range activeEvents {
		jobStrs := make([]string, len(events))
		for i, event := range events {
			jobStrs[i] = event.jobID.String()
		}
		parts = append(parts, fmt.Sprintf("op %v: crashed jobs [%v]", opID, strings.Join(jobStrs, ", ")))
	}

	return errors.New(strings.Join(parts, "; "))
}
