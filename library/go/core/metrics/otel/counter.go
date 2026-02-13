package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.ytsaurus.tech/library/go/core/metrics"
)

var _ metrics.Counter = (*Counter)(nil)
var _ Metric = (*Counter)(nil)

type Counter struct {
	counter metric.Int64Counter
	tags    attribute.Set
}

func (c *Counter) Add(val int64) {
	c.counter.Add(context.Background(), val, metric.WithAttributeSet(c.tags))
}

func (c *Counter) Inc() {
	c.Add(1)
}

func (c *Counter) otelMetric() {}

var _ metrics.FuncCounter = (*FuncCounter)(nil)
var _ Metric = (*FuncCounter)(nil)

type FuncCounter struct {
	counter metric.Int64ObservableCounter
	fn      func() int64
}

func (c *FuncCounter) Function() func() int64 {
	return c.fn
}

func (c *FuncCounter) otelMetric() {}
