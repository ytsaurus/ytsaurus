package httpmetrics

import (
	"net/http"
	"time"

	"go.ytsaurus.tech/library/go/core/metrics"
)

// WithPathEndpoint configures middleware to thread each separate HTTP path as separate endpoint.
//
// DO NOT use this option, when request URL contains resource identifiers, e.g. /task/1234.
func WithPathEndpoint() func(*metricsMiddleware) {
	return func(m *metricsMiddleware) {
		m.endpointKey = func(req *http.Request) string {
			return req.URL.Path
		}
	}
}

// WithEndpointKey specifies custom function for determining endpoint from request.
//
// EndpointKey must return string from small fixed set.
func WithEndpointKey(key func(req *http.Request) string) func(*metricsMiddleware) {
	return func(m *metricsMiddleware) {
		m.endpointKey = key
	}
}

// WithHTTPCodes specifies list of interesting HTTP codes.
//
// HTTP codes, not listed in this set, will be reported as part of general category 1XX, 2XX, 3XX, etc...
func WithHTTPCodes(codes ...int) func(*metricsMiddleware) {
	return func(m *metricsMiddleware) {
		m.httpcodes = codes
	}
}

func DefaultDurationBuckets() []time.Duration {
	return []time.Duration{
		100 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		10 * time.Second,
	}
}

// WithDurationBuckets specifies buckets to be used for request duration histogram.
func WithDurationBuckets(buckets metrics.DurationBuckets) func(*metricsMiddleware) {
	return func(m *metricsMiddleware) {
		m.durationBuckets = buckets
	}
}

func WithSolomonRated() func(*metricsMiddleware) {
	return func(m *metricsMiddleware) {
		m.solomonRated = true
	}
}

// WithPreinitializedEndpoints pre-initializes endpoint metrics set for provided keys.
func WithPreinitializedEndpoints(keys ...string) func(*metricsMiddleware) {
	return func(m *metricsMiddleware) {
		for _, key := range keys {
			m.endpoint(key)
		}
	}
}
