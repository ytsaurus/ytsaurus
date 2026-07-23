package agent

import (
	"time"

	"go.ytsaurus.tech/library/go/core/metrics"
)

// AgentMetrics is a typed wrapper over a metrics registry exposing generic
// per-agent sensors.
type AgentMetrics struct {
	opletCount       metrics.IntGauge
	failedOpletCount metrics.IntGauge
	lastPassDuration metrics.Gauge
	passErrorCount   metrics.Counter
}

func NewAgentMetrics(r metrics.Registry) *AgentMetrics {
	if r == nil {
		return nil
	}
	m := &AgentMetrics{
		opletCount:       r.IntGauge("oplet_count"),
		failedOpletCount: r.IntGauge("failed_oplet_count"),
		lastPassDuration: r.Gauge("last_pass_duration_seconds"),
		passErrorCount:   r.Counter("pass_error_count"),
	}
	m.opletCount.Set(0)
	m.failedOpletCount.Set(0)
	m.lastPassDuration.Set(0)
	return m
}

func (m *AgentMetrics) SetOpletCount(count int) {
	if m == nil {
		return
	}
	m.opletCount.Set(int64(count))
}

func (m *AgentMetrics) SetFailedOpletCount(count int) {
	if m == nil {
		return
	}
	m.failedOpletCount.Set(int64(count))
}

func (m *AgentMetrics) RecordPassDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.lastPassDuration.Set(d.Seconds())
}

func (m *AgentMetrics) RecordPassError() {
	if m == nil {
		return
	}
	m.passErrorCount.Inc()
}

// Reset zeroes the leader-scoped count sensors. It is called when the agent
// stops (e.g. leadership is lost) so that a former leader stops reporting its
// last known oplet counts instead of leaving them frozen at stale values.
// pass_error_count is intentionally left untouched: it is a monotonic counter
// whose rate naturally drops to zero once the agent stops running passes.
func (m *AgentMetrics) Reset() {
	m.SetOpletCount(0)
	m.SetFailedOpletCount(0)
}
