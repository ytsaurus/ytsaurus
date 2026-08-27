package agent

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
)

// AgentMetrics is a typed wrapper over a metrics registry exposing generic
// per-agent sensors.
type AgentMetrics struct {
	opletCount       metrics.IntGauge
	brokenOpletCount metrics.IntGauge
	failedOpletCount metrics.IntGauge

	lastPassDuration  metrics.Gauge
	opletPassDuration metrics.Timer
	passErrorCount    metrics.Counter

	controllerRegistry      metrics.Registry
	controllerMetricVectors map[string]*controllerMetricVector
}

type controllerOpletMetricSet struct {
	alias   string
	metrics []strawberry.Metric
}

type controllerMetricVector struct {
	gauge       metrics.GaugeVec
	labels      []string
	tagsByAlias map[string]map[string]string
}

func NewAgentMetrics(r metrics.Registry, config *MetricsConfig) *AgentMetrics {
	if r == nil || config == nil {
		return nil
	}
	m := &AgentMetrics{
		opletCount:              r.IntGauge("oplet_count"),
		brokenOpletCount:        r.IntGauge("broken_oplet_count"),
		failedOpletCount:        r.IntGauge("failed_oplet_count"),
		lastPassDuration:        r.Gauge("last_pass_duration_seconds"),
		opletPassDuration:       r.DurationHistogram("oplet_pass_duration_seconds", config.OpletPassDurationHistogram.buckets()),
		passErrorCount:          r.Counter("pass_error_count"),
		controllerRegistry:      r.WithPrefix("controller"),
		controllerMetricVectors: make(map[string]*controllerMetricVector),
	}
	m.opletCount.Set(0)
	m.brokenOpletCount.Set(0)
	m.failedOpletCount.Set(0)
	m.lastPassDuration.Set(0)
	return m
}

func (m *AgentMetrics) SetControllerMetrics(controllerMetrics []controllerOpletMetricSet) error {
	if m == nil {
		return nil
	}

	type metricUpdate struct {
		name  string
		alias string
		tags  map[string]string
		value float64
	}

	var updates []metricUpdate
	labelsByMetric := make(map[string][]string)
	invalidMetricNames := make(map[string]struct{})
	var validationErrors []error
	for _, metricSet := range controllerMetrics {
		for _, metric := range metricSet.metrics {
			tags := metric.Tags
			if tags == nil {
				tags = make(map[string]string, 1)
			}
			tags["alias"] = metricSet.alias
			labels := metricLabels(tags)

			expectedLabels, ok := labelsByMetric[metric.Name]
			if !ok {
				if vector, exists := m.controllerMetricVectors[metric.Name]; exists {
					expectedLabels = vector.labels
				} else {
					expectedLabels = labels
				}
				labelsByMetric[metric.Name] = expectedLabels
			}
			// GaugeVec.With panics on a label schema mismatch, so validate the complete update first.
			if !slices.Equal(labels, expectedLabels) {
				if _, ok := invalidMetricNames[metric.Name]; !ok {
					invalidMetricNames[metric.Name] = struct{}{}
					validationErrors = append(validationErrors,
						fmt.Errorf("controller metric %q has inconsistent labels for oplet %q", metric.Name, metricSet.alias))
				}
			}

			updates = append(updates, metricUpdate{
				name:  metric.Name,
				alias: metricSet.alias,
				tags:  tags,
				value: metric.Value,
			})
		}
	}

	currentTagsByMetric := make(map[string]map[string]map[string]string)

	// Publish current samples before zeroing stale tag sets so scrapes do not see a reset/rebuild gap.
	for _, update := range updates {
		if _, invalid := invalidMetricNames[update.name]; invalid {
			continue
		}

		vector, ok := m.controllerMetricVectors[update.name]
		if !ok {
			vector = &controllerMetricVector{
				gauge:  m.controllerRegistry.GaugeVec(update.name, labelsByMetric[update.name]),
				labels: labelsByMetric[update.name],
			}
			m.controllerMetricVectors[update.name] = vector
		}
		vector.gauge.With(update.tags).Set(update.value)

		tagsByAlias, ok := currentTagsByMetric[update.name]
		if !ok {
			tagsByAlias = make(map[string]map[string]string)
			currentTagsByMetric[update.name] = tagsByAlias
		}
		tagsByAlias[update.alias] = update.tags
	}

	for name, vector := range m.controllerMetricVectors {
		if _, invalid := invalidMetricNames[name]; invalid {
			continue
		}

		currentTags := currentTagsByMetric[name]
		for alias, previousTags := range vector.tagsByAlias {
			if tags, ok := currentTags[alias]; !ok || !maps.Equal(tags, previousTags) {
				vector.gauge.With(previousTags).Set(0)
			}
		}
		vector.tagsByAlias = currentTags
	}

	return errors.Join(validationErrors...)
}

func metricLabels(tags map[string]string) []string {
	labels := make([]string, 0, len(tags))
	for label := range tags {
		labels = append(labels, label)
	}
	slices.Sort(labels)
	return labels
}

func (m *AgentMetrics) SetOpletCount(count int) {
	if m == nil {
		return
	}
	m.opletCount.Set(int64(count))
}

func (m *AgentMetrics) SetBrokenOpletCount(count int) {
	if m == nil {
		return
	}
	m.brokenOpletCount.Set(int64(count))
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

func (m *AgentMetrics) RecordOpletPassDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.opletPassDuration.RecordDuration(d)
}

func (m *AgentMetrics) RecordPassError() {
	if m == nil {
		return
	}
	m.passErrorCount.Inc()
}

// Reset clears leader-scoped gauges when the agent stops (e.g. leadership is
// lost) so that a former leader does not report stale values.
// pass_error_count and oplet_pass_duration_seconds are intentionally left
// untouched: they are cumulative sensors whose rates naturally drop to zero
// once the agent stops running passes.
func (m *AgentMetrics) Reset() {
	if m == nil {
		return
	}

	m.SetOpletCount(0)
	m.SetBrokenOpletCount(0)
	m.SetFailedOpletCount(0)
	for _, vector := range m.controllerMetricVectors {
		vector.gauge.Reset()
	}
	m.controllerMetricVectors = make(map[string]*controllerMetricVector)
}
