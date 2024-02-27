// Package httpmetrics measures important metrics of HTTP service.
//
// By default, collects aggregate metrics.
package httpmetrics

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/metrics/solomon"
)

type (
	MiddlewareOption func(*metricsMiddleware)

	metricsMiddleware struct {
		r metrics.Registry

		endpointKey func(req *http.Request) string

		durationBuckets metrics.DurationBuckets

		httpcodes []int

		solomonRated bool

		defaultEndpoint endpointMetrics
		endpoints       sync.Map
	}

	endpointMetrics struct {
		registerOnce sync.Once

		m *metricsMiddleware

		requestCount     metrics.Counter
		requestDuration  metrics.Timer
		panicsCount      metrics.Counter
		inflightRequests metrics.Gauge

		httpCodes map[string]metrics.Counter
	}
)

func httpCodeSpecificTag(code int) string {
	return strconv.FormatInt(int64(code), 10)
}

func httpCodeDefaultTag(code int) string {
	switch code / 100 {
	case 1:
		return "1XX"
	case 2:
		return "2XX"
	case 3:
		return "3XX"
	case 4:
		return "4XX"
	default:
		return "5XX"
	}
}

func (m *metricsMiddleware) httpCodeTag(code int) string {
	for _, interestingCode := range m.httpcodes {
		if interestingCode == code {
			return httpCodeSpecificTag(code)
		}
	}
	return httpCodeDefaultTag(code)
}

func (m *metricsMiddleware) register(r metrics.Registry, endpoint *endpointMetrics, key string) {
	endpoint.registerOnce.Do(func() {
		if key != "" {
			r = r.WithTags(map[string]string{"endpoint": key})
		}

		endpoint.requestCount = r.Counter("request_count")
		endpoint.requestDuration = r.DurationHistogram("request_duration", m.durationBuckets)
		endpoint.panicsCount = r.Counter("panics_count")
		endpoint.inflightRequests = r.Gauge("inflight_requests")
		if m.solomonRated {
			solomon.Rated(endpoint.requestCount)
			solomon.Rated(endpoint.requestDuration)
			solomon.Rated(endpoint.panicsCount)
		}
		endpoint.httpCodes = map[string]metrics.Counter{}

		registerCounter := func(tag string) {
			endpoint.httpCodes[tag] = r.WithTags(map[string]string{"http_code": tag}).Counter("http_code_count")
			if m.solomonRated {
				solomon.Rated(endpoint.httpCodes[tag])
			}
		}

		for _, code := range m.httpcodes {
			registerCounter(httpCodeSpecificTag(code))
		}

		for i := 1; i <= 5; i++ {
			registerCounter(httpCodeDefaultTag(i * 100))
		}
	})
}

func (e *endpointMetrics) finishRequest(startTime time.Time, w middleware.WrapResponseWriter) {
	e.requestDuration.RecordDuration(time.Since(startTime))
	e.inflightRequests.Add(-1)

	e.httpCodes[e.m.httpCodeTag(w.Status())].Inc()

	if err := recover(); err != nil {
		e.panicsCount.Inc()
		panic(err)
	}
}

// New creates middleware that reports various profiling metrics.
//
// By default, total request rate, error rate and request timing are collected.
func New(registry metrics.Registry, opts ...MiddlewareOption) func(next http.Handler) http.Handler {
	m := &metricsMiddleware{
		r:               registry,
		durationBuckets: metrics.NewDurationBuckets(DefaultDurationBuckets()...),
		endpointKey:     func(req *http.Request) string { return "" },
	}
	m.defaultEndpoint.m = m

	for _, opt := range opts {
		opt(m)
	}

	return m.wrap
}

func (m *metricsMiddleware) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := m.endpointKey(r)
		endpoint := m.endpoint(key)

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		endpoint.requestCount.Inc()
		endpoint.inflightRequests.Add(1)

		startTime := time.Now()
		defer endpoint.finishRequest(startTime, ww)

		next.ServeHTTP(ww, r)
	})
}

func (m *metricsMiddleware) endpoint(key string) *endpointMetrics {
	endpoint := &m.defaultEndpoint
	if key != "" {
		value, _ := m.endpoints.LoadOrStore(key, &endpointMetrics{m: m})
		endpoint = value.(*endpointMetrics)
	}

	m.register(m.r, endpoint, key)

	return endpoint
}
