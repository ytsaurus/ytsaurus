package otel

import (
	"go.opentelemetry.io/otel/metric"

	"go.ytsaurus.tech/library/go/core/log"
)

type RegistryOpt func(*Registry)

func WithMeterProvider(mp metric.MeterProvider) RegistryOpt {
	return func(r *Registry) {
		r.provider = mp
	}
}

func WithSeparator(sep string) RegistryOpt {
	return func(r *Registry) {
		r.separator = sep
	}
}

func WithLogger(l log.Logger) RegistryOpt {
	return func(r *Registry) {
		r.logger = l
	}
}
