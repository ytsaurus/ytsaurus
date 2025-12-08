package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Metric interface {
	otelMetric()
}

type float64Adder interface {
	Add(context.Context, float64, ...metric.AddOption)
}

type int64Adder interface {
	Add(context.Context, int64, ...metric.AddOption)
}

func tagsToAttributes(tags map[string]string) attribute.Set {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attribute.NewSet(attrs...)
}
