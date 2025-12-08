package otel

import (
	"fmt"
	"slices"
	"sync"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/metrics/internal/pkg/registryutil"
	"go.ytsaurus.tech/library/go/core/metrics/nop"
)

// metricsVector is a base implementation of vector of metrics of any supported type.
type metricsVector struct {
	metricsMu sync.RWMutex
	metrics   map[uint64]Metric
	labels    []string
	newMetric func(map[string]string) Metric
	logger    log.Logger
}

func (v *metricsVector) with(tags map[string]string) (Metric, error) {
	hv, err := registryutil.VectorHash(tags, v.labels)
	if err != nil {
		return nil, fmt.Errorf("cannot construct vector hash: %w", err)
	}

	v.metricsMu.RLock()
	metric, ok := v.metrics[hv]
	v.metricsMu.RUnlock()
	if ok {
		return metric, nil
	}

	v.metricsMu.Lock()
	defer v.metricsMu.Unlock()

	metric, ok = v.metrics[hv]
	if !ok {
		metric = v.newMetric(tags)
		v.metrics[hv] = metric
	}

	return metric, nil
}

// reset deletes all metrics in this vector.
func (v *metricsVector) reset() {
	v.metricsMu.Lock()
	defer v.metricsMu.Unlock()

	clear(v.metrics)
}

var _ metrics.CounterVec = (*CounterVec)(nil)

// CounterVec stores counters and
// implements metrics.CounterVec interface.
type CounterVec struct {
	vec *metricsVector
}

// CounterVec creates a new counters vector with given metric name and
// partitioned by the given label names.
func (r *Registry) CounterVec(name string, labels []string) metrics.CounterVec {
	return &CounterVec{
		vec: &metricsVector{
			labels:  slices.Clone(labels),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).
					Counter(name).(*Counter)
			},
			logger: r.logger,
		},
	}
}

// With creates new or returns existing counter with given tags from vector.
func (v *CounterVec) With(tags map[string]string) metrics.Counter {
	m, err := v.vec.with(tags)
	if err != nil {
		v.vec.logger.Warn("cannot extract counter from vector", log.Any("tags", tags), log.Error(err))
		return nop.Counter{}
	}
	return m.(*Counter)
}

// Reset deletes all metrics in this vector.
func (v *CounterVec) Reset() {
	v.vec.reset()
}

// GaugeVec stores gauges and
// implements metrics.GaugeVec interface.
type GaugeVec struct {
	vec *metricsVector
}

// GaugeVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) GaugeVec(name string, labels []string) metrics.GaugeVec {
	return &GaugeVec{
		vec: &metricsVector{
			labels:  slices.Clone(labels),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).Gauge(name).(*Gauge)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
func (v *GaugeVec) With(tags map[string]string) metrics.Gauge {
	m, err := v.vec.with(tags)
	if err != nil {
		v.vec.logger.Warn("cannot extract gauge from vector", log.Any("tags", tags), log.Error(err))
		return nop.Gauge{}
	}
	return m.(*Gauge)
}

// Reset deletes all metrics in this vector.
func (v *GaugeVec) Reset() {
	v.vec.reset()
}

// IntGaugeVec stores gauges and
// implements metrics.IntGaugeVec interface.
type IntGaugeVec struct {
	vec *metricsVector
}

// IntGaugeVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) IntGaugeVec(name string, labels []string) metrics.IntGaugeVec {
	return &IntGaugeVec{
		vec: &metricsVector{
			labels:  slices.Clone(labels),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).IntGauge(name).(*IntGauge)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
func (v *IntGaugeVec) With(tags map[string]string) metrics.IntGauge {
	m, err := v.vec.with(tags)
	if err != nil {
		v.vec.logger.Warn("cannot extract int gauge from vector", log.Any("tags", tags), log.Error(err))
		return nop.IntGauge{}
	}
	return m.(*IntGauge)
}

// Reset deletes all metrics in this vector.
func (v *IntGaugeVec) Reset() {
	v.vec.reset()
}

// TimerVec stores gauges and
// implements metrics.TimerVec interface.
type TimerVec struct {
	vec *metricsVector
}

// TimerVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) TimerVec(name string, labels []string) metrics.TimerVec {
	return &TimerVec{
		vec: &metricsVector{
			labels:  slices.Clone(labels),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).Timer(name).(*Timer)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
func (v *TimerVec) With(tags map[string]string) metrics.Timer {
	m, err := v.vec.with(tags)
	if err != nil {
		v.vec.logger.Warn("cannot extract timer from vector", log.Any("tags", tags), log.Error(err))
		return nop.Timer{}
	}
	return m.(*Timer)
}

// Reset deletes all metrics in this vector.
func (v *TimerVec) Reset() {
	v.vec.reset()
}

// HistogramVec stores gauges and
// implements metrics.HistogramVec interface.
type HistogramVec struct {
	vec *metricsVector
}

// HistogramVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) HistogramVec(name string, buckets metrics.Buckets, labels []string) metrics.HistogramVec {
	return &HistogramVec{
		vec: &metricsVector{
			labels:  slices.Clone(labels),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).Histogram(name, buckets).(*Histogram)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
func (v *HistogramVec) With(tags map[string]string) metrics.Histogram {
	m, err := v.vec.with(tags)
	if err != nil {
		v.vec.logger.Warn("cannot extract histogram from vector", log.Any("tags", tags), log.Error(err))
		return nop.Histogram{}
	}
	return m.(*Histogram)
}

// Reset deletes all metrics in this vector.
func (v *HistogramVec) Reset() {
	v.vec.reset()
}

// DurationHistogramVec stores gauges and
// implements metrics.DurationHistogramVec interface.
type DurationHistogramVec struct {
	vec *metricsVector
}

// DurationHistogramVec creates a new gauges vector with given metric name and
// partitioned by the given label names.
func (r *Registry) DurationHistogramVec(name string, buckets metrics.DurationBuckets, labels []string) metrics.TimerVec {
	return &DurationHistogramVec{
		vec: &metricsVector{
			labels:  slices.Clone(labels),
			metrics: make(map[uint64]Metric),
			newMetric: func(tags map[string]string) Metric {
				return r.WithTags(tags).DurationHistogram(name, buckets).(*Histogram)
			},
		},
	}
}

// With creates new or returns existing gauge with given tags from vector.
func (v *DurationHistogramVec) With(tags map[string]string) metrics.Timer {
	m, err := v.vec.with(tags)
	if err != nil {
		v.vec.logger.Warn("cannot extract duration histogram from vector", log.Any("tags", tags), log.Error(err))
		return nop.Timer{}
	}
	return m.(*Histogram)
}

// Reset deletes all metrics in this vector.
func (v *DurationHistogramVec) Reset() {
	v.vec.reset()
}
