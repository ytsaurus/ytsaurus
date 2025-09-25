//go:build !internal
// +build !internal

package app

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/metrics/prometheus"
)

type MetricsRegistry struct {
	registry *prometheus.Registry
}

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		registry: prometheus.NewRegistry(prometheus.NewRegistryOpts()),
	}
}

func (r *MetricsRegistry) WithTags(tags map[string]string) metrics.Registry {
	return r.registry.WithTags(tags)
}

func (r *MetricsRegistry) WithPrefix(prefix string) metrics.Registry {
	return r.registry.WithPrefix(prefix)
}

type handler struct {
	registry metrics.MetricsStreamer
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = h.registry.Stream(r.Context(), w)
}

func (r *MetricsRegistry) HandleMetrics(router *chi.Mux) {
	router.Handle("/prometheus", handler{registry: r.registry})
}
