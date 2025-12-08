package otel

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"go.ytsaurus.tech/library/go/core/log"
	noplog "go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/metrics/internal/pkg/metricsutil"
	"go.ytsaurus.tech/library/go/core/metrics/internal/pkg/registryutil"
	"go.ytsaurus.tech/library/go/core/metrics/nop"
)

var _ metrics.Registry = (*Registry)(nil)

type Registry struct {
	mu *sync.RWMutex

	provider metric.MeterProvider
	meter    metric.Meter
	logger   log.Logger

	name          string
	separator     string
	tags          map[string]string
	subregistries map[string]*Registry
}

func NewRegistry(name string, opts ...RegistryOpt) metrics.Registry {
	r := &Registry{
		mu: new(sync.RWMutex),

		provider: otel.GetMeterProvider(),
		logger:   new(noplog.Logger),

		name:          name,
		separator:     ".",
		subregistries: make(map[string]*Registry),
	}

	for _, opt := range opts {
		opt(r)
	}

	r.meter = r.provider.Meter(name)

	return r
}

func (r *Registry) WithTags(tags map[string]string) metrics.Registry {
	return r.newSubregistry(r.name, registryutil.MergeTags(r.tags, tags))
}

func (r *Registry) WithPrefix(prefix string) metrics.Registry {
	nname := registryutil.BuildFQName(r.separator, r.name, prefix)
	return r.newSubregistry(nname, r.tags)
}

func (r *Registry) ComposeName(parts ...string) string {
	return registryutil.BuildFQName(r.separator, parts...)
}

func (r *Registry) Counter(name string) metrics.Counter {
	counter, err := r.meter.Int64Counter(name)
	if err != nil {
		r.logger.Warn("cannot create counter", log.String("name", name), log.Error(err))
		return nop.Counter{}
	}
	return &Counter{
		counter: counter,
		tags:    tagsToAttributes(r.tags),
	}
}

func (r *Registry) FuncCounter(name string, fn func() int64) metrics.FuncCounter {
	attrs := tagsToAttributes(r.tags)

	counter, err := r.meter.Int64ObservableCounter(name,
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(fn(), metric.WithAttributeSet(attrs))
			return nil
		}),
	)
	if err != nil {
		r.logger.Warn("cannot create func counter", log.String("name", name), log.Error(err))
		return nop.FuncCounter{}
	}

	return &FuncCounter{
		counter: counter,
		fn:      fn,
	}
}

func (r *Registry) Gauge(name string) metrics.Gauge {
	gauge, err := r.meter.Float64Gauge(name)
	if err != nil {
		r.logger.Warn("cannot create gauge", log.String("name", name), log.Error(err))
		return nop.Gauge{}
	}
	return &Gauge{
		gauge: gauge,
		tags:  tagsToAttributes(r.tags),
	}
}

func (r *Registry) FuncGauge(name string, fn func() float64) metrics.FuncGauge {
	attrs := tagsToAttributes(r.tags)

	gauge, err := r.meter.Float64ObservableGauge(name,
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(fn(), metric.WithAttributeSet(attrs))
			return nil
		}),
	)
	if err != nil {
		r.logger.Warn("cannot create func gauge", log.String("name", name), log.Error(err))
		return nop.FuncGauge{}
	}

	return &FuncGauge{
		gauge: gauge,
		fn:    fn,
	}
}

func (r *Registry) IntGauge(name string) metrics.IntGauge {
	gauge, err := r.meter.Int64Gauge(name)
	if err != nil {
		r.logger.Warn("cannot create int gauge", log.String("name", name), log.Error(err))
		return nop.IntGauge{}
	}
	return &IntGauge{
		gauge: gauge,
		tags:  tagsToAttributes(r.tags),
	}
}

func (r *Registry) FuncIntGauge(name string, fn func() int64) metrics.FuncIntGauge {
	attrs := tagsToAttributes(r.tags)

	gauge, err := r.meter.Int64ObservableGauge(name,
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(fn(), metric.WithAttributeSet(attrs))
			return nil
		}),
	)
	if err != nil {
		r.logger.Warn("cannot create func int gauge", log.String("name", name), log.Error(err))
		return nop.FuncIntGauge{}
	}

	return &FuncIntGauge{
		gauge: gauge,
		fn:    fn,
	}
}

func (r *Registry) Histogram(name string, buckets metrics.Buckets) metrics.Histogram {
	bounds := metricsutil.BucketsBounds(buckets)
	hist, err := r.meter.Float64Histogram(name, metric.WithExplicitBucketBoundaries(bounds...))
	if err != nil {
		r.logger.Warn("cannot create histogram", log.String("name", name), log.Float64s("bounds", bounds), log.Error(err))
		return nop.Histogram{}
	}
	return &Histogram{
		hist: hist,
		tags: tagsToAttributes(r.tags),
	}
}

func (r *Registry) Timer(name string) metrics.Timer {
	gauge, err := r.meter.Int64Gauge(name)
	if err != nil {
		r.logger.Warn("cannot create timer", log.String("name", name), log.Error(err))
		return nop.Timer{}
	}
	return &Timer{
		gauge: gauge,
		tags:  tagsToAttributes(r.tags),
	}
}

func (r *Registry) DurationHistogram(name string, buckets metrics.DurationBuckets) metrics.Timer {
	bounds := metricsutil.DurationBucketsBounds(buckets)
	hist, err := r.meter.Float64Histogram(name, metric.WithExplicitBucketBoundaries(bounds...))
	if err != nil {
		r.logger.Warn("cannot create duration histogram", log.String("name", name), log.Float64s("bounds", bounds), log.Error(err))
		return nop.Histogram{}
	}
	return &Histogram{
		hist: hist,
		tags: tagsToAttributes(r.tags),
	}
}

func (r *Registry) newSubregistry(name string, tags map[string]string) *Registry {
	registryKey := registryutil.BuildRegistryKey(name, tags)

	r.mu.RLock()
	existing, ok := r.subregistries[registryKey]
	r.mu.RUnlock()
	if ok {
		return existing
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// recheck
	if existing, ok := r.subregistries[registryKey]; ok {
		return existing
	}

	subregistry := &Registry{
		mu: r.mu,

		provider: r.provider,
		meter:    r.provider.Meter(name),
		logger:   r.logger,

		name:          name,
		separator:     r.separator,
		tags:          tags,
		subregistries: r.subregistries,
	}

	r.subregistries[registryKey] = subregistry
	return subregistry
}
