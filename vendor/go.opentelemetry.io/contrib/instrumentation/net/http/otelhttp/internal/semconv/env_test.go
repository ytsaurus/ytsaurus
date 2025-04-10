// Code created by gotmpl. DO NOT MODIFY.
// source: internal/shared/semconv/env_test.go.tmpl

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package semconv

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestHTTPServerDoesNotPanic(t *testing.T) {
	testCases := []struct {
		name   string
		server HTTPServer
	}{
		{
			name:   "empty",
			server: HTTPServer{},
		},
		{
			name:   "nil meter",
			server: NewHTTPServer(nil),
		},
		{
			name:   "with Meter",
			server: NewHTTPServer(noop.Meter{}),
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				req, err := http.NewRequest("GET", "http://example.com", nil)
				require.NoError(t, err)

				_ = tt.server.RequestTraceAttrs("stuff", req)
				_ = tt.server.ResponseTraceAttrs(ResponseTelemetry{StatusCode: 200})
				tt.server.RecordMetrics(context.Background(), ServerMetricData{
					ServerName: "stuff",
					MetricAttributes: MetricAttributes{
						Req: req,
					},
				})
			})
		})
	}
}

func TestServerNetworkTransportAttr(t *testing.T) {
	for _, tt := range []struct {
		name     string
		optinVal string
		network  string

		wantAttributes []attribute.KeyValue
	}{
		{
			name:    "without any opt-in",
			network: "tcp",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.transport", "ip_tcp"),
			},
		},
		{
			name:     "without an old optin",
			optinVal: "old",
			network:  "tcp",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.transport", "ip_tcp"),
			},
		},
		{
			name:     "without a dup optin",
			optinVal: "http/dup",
			network:  "tcp",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.transport", "ip_tcp"),
				attribute.String("network.transport", "tcp"),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(OTelSemConvStabilityOptIn, tt.optinVal)
			s := NewHTTPServer(nil)

			assert.Equal(t, tt.wantAttributes, s.NetworkTransportAttr(tt.network))
		})
	}
}

func TestHTTPClientDoesNotPanic(t *testing.T) {
	testCases := []struct {
		name   string
		client HTTPClient
	}{
		{
			name:   "empty",
			client: HTTPClient{},
		},
		{
			name:   "nil meter",
			client: NewHTTPClient(nil),
		},
		{
			name:   "with Meter",
			client: NewHTTPClient(noop.Meter{}),
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				req, err := http.NewRequest("GET", "http://example.com", nil)
				require.NoError(t, err)

				_ = tt.client.RequestTraceAttrs(req)
				_ = tt.client.ResponseTraceAttrs(&http.Response{StatusCode: 200})

				opts := tt.client.MetricOptions(MetricAttributes{
					Req:        req,
					StatusCode: 200,
				})
				tt.client.RecordResponseSize(context.Background(), 40, opts)
				tt.client.RecordMetrics(context.Background(), MetricData{
					RequestSize: 20,
					ElapsedTime: 1,
				}, opts)
			})
		})
	}
}

func TestHTTPClientTraceAttributes(t *testing.T) {
	for _, tt := range []struct {
		name     string
		optinVal string

		wantAttributes []attribute.KeyValue
	}{
		{
			name: "with no optin set",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.host.name", "example.com"),
			},
		},
		{
			name:     "with optin set to old only",
			optinVal: "old",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.host.name", "example.com"),
			},
		},
		{
			name:     "with optin set to duplicate",
			optinVal: "http/dup",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.host.name", "example.com"),
				attribute.String("server.address", "example.com"),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(OTelSemConvStabilityOptIn, tt.optinVal)

			c := NewHTTPClient(nil)
			a := c.TraceAttributes("example.com")
			assert.Equal(t, tt.wantAttributes, a)
		})
	}
}

func TestClientTraceAttributes(t *testing.T) {
	for _, tt := range []struct {
		name     string
		optinVal string
		host     string

		wantAttributes []attribute.KeyValue
	}{
		{
			name: "without any opt-in",
			host: "example.com",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.host.name", "example.com"),
			},
		},
		{
			name:     "without an old optin",
			optinVal: "old",
			host:     "example.com",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.host.name", "example.com"),
			},
		},
		{
			name:     "without a dup optin",
			optinVal: "http/dup",
			host:     "example.com",

			wantAttributes: []attribute.KeyValue{
				attribute.String("net.host.name", "example.com"),
				attribute.String("server.address", "example.com"),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(OTelSemConvStabilityOptIn, tt.optinVal)
			s := NewHTTPClient(nil)

			assert.Equal(t, tt.wantAttributes, s.TraceAttributes(tt.host))
		})
	}
}

func BenchmarkRecordMetrics(b *testing.B) {
	benchmarks := []struct {
		name   string
		server HTTPServer
	}{
		{
			name:   "empty",
			server: HTTPServer{},
		},
		{
			name:   "nil meter",
			server: NewHTTPServer(nil),
		},
		{
			name:   "with Meter",
			server: NewHTTPServer(noop.Meter{}),
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			req, _ := http.NewRequest("GET", "http://example.com", nil)
			_ = bm.server.RequestTraceAttrs("stuff", req)
			_ = bm.server.ResponseTraceAttrs(ResponseTelemetry{StatusCode: 200})
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bm.server.RecordMetrics(ctx, ServerMetricData{
					ServerName: bm.name,
					MetricAttributes: MetricAttributes{
						Req: req,
					},
				})
			}
		})
	}
}
