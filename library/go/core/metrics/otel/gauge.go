package otel

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.ytsaurus.tech/library/go/core/metrics"
)

var _ metrics.Gauge = (*Gauge)(nil)
var _ Metric = (*Gauge)(nil)

type Gauge struct {
	mu    sync.Mutex
	value float64
	gauge metric.Float64Gauge
	tags  attribute.Set
}

func (g *Gauge) Add(val float64) {
	g.mu.Lock()
	g.value += val
	val = g.value
	g.mu.Unlock()

	g.gauge.Record(context.Background(), val, metric.WithAttributeSet(g.tags))
}

func (g *Gauge) Set(val float64) {
	g.mu.Lock()
	g.value = val
	g.mu.Unlock()

	g.gauge.Record(context.Background(), val, metric.WithAttributeSet(g.tags))
}

func (g *Gauge) otelMetric() {}

var _ metrics.FuncGauge = (*FuncGauge)(nil)
var _ Metric = (*FuncGauge)(nil)

type FuncGauge struct {
	gauge metric.Float64ObservableGauge
	fn    func() float64
}

func (g *FuncGauge) Function() func() float64 {
	return g.fn
}

func (g *FuncGauge) otelMetric() {}

var _ metrics.IntGauge = (*IntGauge)(nil)
var _ Metric = (*IntGauge)(nil)

type IntGauge struct {
	gauge metric.Int64Gauge
	value int64
	tags  attribute.Set
}

func (g *IntGauge) Add(val int64) {
	val = atomic.AddInt64(&g.value, val)
	g.gauge.Record(context.Background(), val, metric.WithAttributeSet(g.tags))
}

func (g *IntGauge) Set(val int64) {
	atomic.StoreInt64(&g.value, val)
	g.gauge.Record(context.Background(), val, metric.WithAttributeSet(g.tags))
}

func (g *IntGauge) otelMetric() {}

var _ metrics.FuncIntGauge = (*FuncIntGauge)(nil)
var _ Metric = (*FuncIntGauge)(nil)

type FuncIntGauge struct {
	gauge metric.Int64ObservableGauge
	fn    func() int64
}

func (g *FuncIntGauge) Function() func() int64 {
	return g.fn
}

func (g *FuncIntGauge) otelMetric() {}
