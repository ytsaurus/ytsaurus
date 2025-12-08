package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.ytsaurus.tech/library/go/core/metrics"
)

var _ metrics.Timer = (*Timer)(nil)
var _ Metric = (*Timer)(nil)

type Timer struct {
	gauge metric.Int64Gauge
	tags  attribute.Set
}

func (t *Timer) RecordDuration(val time.Duration) {
	t.gauge.Record(context.Background(), int64(val), metric.WithAttributeSet(t.tags))
}

func (t *Timer) otelMetric() {}
