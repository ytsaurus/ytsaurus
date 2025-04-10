// Code created by gotmpl. DO NOT MODIFY.
// source: internal/shared/semconv/httpconv_test.go.tmpl

// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package semconv

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

func TestCurrentHttpServer_MetricAttributes(t *testing.T) {
	defaultRequest, err := http.NewRequest("GET", "http://example.com/path?query=test", nil)
	require.NoError(t, err)

	tests := []struct {
		name                 string
		server               string
		req                  *http.Request
		statusCode           int
		additionalAttributes []attribute.KeyValue
		wantFunc             func(t *testing.T, attrs []attribute.KeyValue)
	}{
		{
			name:                 "routine testing",
			server:               "",
			req:                  defaultRequest,
			statusCode:           200,
			additionalAttributes: []attribute.KeyValue{attribute.String("test", "test")},
			wantFunc: func(t *testing.T, attrs []attribute.KeyValue) {
				require.Len(t, attrs, 7)
				assert.ElementsMatch(t, []attribute.KeyValue{
					attribute.String("http.request.method", "GET"),
					attribute.String("url.scheme", "http"),
					attribute.String("server.address", "example.com"),
					attribute.String("network.protocol.name", "http"),
					attribute.String("network.protocol.version", "1.1"),
					attribute.Int64("http.response.status_code", 200),
					attribute.String("test", "test"),
				}, attrs)
			},
		},
		{
			name:                 "use server address",
			server:               "example.com:9999",
			req:                  defaultRequest,
			statusCode:           200,
			additionalAttributes: nil,
			wantFunc: func(t *testing.T, attrs []attribute.KeyValue) {
				require.Len(t, attrs, 7)
				assert.ElementsMatch(t, []attribute.KeyValue{
					attribute.String("http.request.method", "GET"),
					attribute.String("url.scheme", "http"),
					attribute.String("server.address", "example.com"),
					attribute.Int("server.port", 9999),
					attribute.String("network.protocol.name", "http"),
					attribute.String("network.protocol.version", "1.1"),
					attribute.Int64("http.response.status_code", 200),
				}, attrs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CurrentHTTPServer{}.MetricAttributes(tt.server, tt.req, tt.statusCode, tt.additionalAttributes)
			tt.wantFunc(t, got)
		})
	}
}

func TestNewMethod(t *testing.T) {
	testCases := []struct {
		method   string
		n        int
		want     attribute.KeyValue
		wantOrig attribute.KeyValue
	}{
		{
			method: http.MethodPost,
			n:      1,
			want:   attribute.String("http.request.method", "POST"),
		},
		{
			method:   "Put",
			n:        2,
			want:     attribute.String("http.request.method", "PUT"),
			wantOrig: attribute.String("http.request.method_original", "Put"),
		},
		{
			method:   "Unknown",
			n:        2,
			want:     attribute.String("http.request.method", "GET"),
			wantOrig: attribute.String("http.request.method_original", "Unknown"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.method, func(t *testing.T) {
			got, gotOrig := CurrentHTTPServer{}.method(tt.method)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOrig, gotOrig)
		})
	}
}

func TestHTTPClientStatus(t *testing.T) {
	tests := []struct {
		code int
		stat codes.Code
		msg  bool
	}{
		{0, codes.Error, true},
		{http.StatusContinue, codes.Unset, false},
		{http.StatusSwitchingProtocols, codes.Unset, false},
		{http.StatusProcessing, codes.Unset, false},
		{http.StatusEarlyHints, codes.Unset, false},
		{http.StatusOK, codes.Unset, false},
		{http.StatusCreated, codes.Unset, false},
		{http.StatusAccepted, codes.Unset, false},
		{http.StatusNonAuthoritativeInfo, codes.Unset, false},
		{http.StatusNoContent, codes.Unset, false},
		{http.StatusResetContent, codes.Unset, false},
		{http.StatusPartialContent, codes.Unset, false},
		{http.StatusMultiStatus, codes.Unset, false},
		{http.StatusAlreadyReported, codes.Unset, false},
		{http.StatusIMUsed, codes.Unset, false},
		{http.StatusMultipleChoices, codes.Unset, false},
		{http.StatusMovedPermanently, codes.Unset, false},
		{http.StatusFound, codes.Unset, false},
		{http.StatusSeeOther, codes.Unset, false},
		{http.StatusNotModified, codes.Unset, false},
		{http.StatusUseProxy, codes.Unset, false},
		{306, codes.Unset, false},
		{http.StatusTemporaryRedirect, codes.Unset, false},
		{http.StatusPermanentRedirect, codes.Unset, false},
		{http.StatusBadRequest, codes.Error, false},
		{http.StatusUnauthorized, codes.Error, false},
		{http.StatusPaymentRequired, codes.Error, false},
		{http.StatusForbidden, codes.Error, false},
		{http.StatusNotFound, codes.Error, false},
		{http.StatusMethodNotAllowed, codes.Error, false},
		{http.StatusNotAcceptable, codes.Error, false},
		{http.StatusProxyAuthRequired, codes.Error, false},
		{http.StatusRequestTimeout, codes.Error, false},
		{http.StatusConflict, codes.Error, false},
		{http.StatusGone, codes.Error, false},
		{http.StatusLengthRequired, codes.Error, false},
		{http.StatusPreconditionFailed, codes.Error, false},
		{http.StatusRequestEntityTooLarge, codes.Error, false},
		{http.StatusRequestURITooLong, codes.Error, false},
		{http.StatusUnsupportedMediaType, codes.Error, false},
		{http.StatusRequestedRangeNotSatisfiable, codes.Error, false},
		{http.StatusExpectationFailed, codes.Error, false},
		{http.StatusTeapot, codes.Error, false},
		{http.StatusMisdirectedRequest, codes.Error, false},
		{http.StatusUnprocessableEntity, codes.Error, false},
		{http.StatusLocked, codes.Error, false},
		{http.StatusFailedDependency, codes.Error, false},
		{http.StatusTooEarly, codes.Error, false},
		{http.StatusUpgradeRequired, codes.Error, false},
		{http.StatusPreconditionRequired, codes.Error, false},
		{http.StatusTooManyRequests, codes.Error, false},
		{http.StatusRequestHeaderFieldsTooLarge, codes.Error, false},
		{http.StatusUnavailableForLegalReasons, codes.Error, false},
		{499, codes.Error, false},
		{http.StatusInternalServerError, codes.Error, false},
		{http.StatusNotImplemented, codes.Error, false},
		{http.StatusBadGateway, codes.Error, false},
		{http.StatusServiceUnavailable, codes.Error, false},
		{http.StatusGatewayTimeout, codes.Error, false},
		{http.StatusHTTPVersionNotSupported, codes.Error, false},
		{http.StatusVariantAlsoNegotiates, codes.Error, false},
		{http.StatusInsufficientStorage, codes.Error, false},
		{http.StatusLoopDetected, codes.Error, false},
		{http.StatusNotExtended, codes.Error, false},
		{http.StatusNetworkAuthenticationRequired, codes.Error, false},
		{600, codes.Error, true},
	}

	for _, test := range tests {
		t.Run(strconv.Itoa(test.code), func(t *testing.T) {
			c, msg := HTTPClient{}.Status(test.code)
			assert.Equal(t, test.stat, c)
			if test.msg && msg == "" {
				t.Errorf("expected non-empty message for %d", test.code)
			} else if !test.msg && msg != "" {
				t.Errorf("expected empty message for %d, got: %s", test.code, msg)
			}
		})
	}
}

func TestCurrentHttpClient_MetricAttributes(t *testing.T) {
	defaultRequest, err := http.NewRequest("GET", "http://example.com/path?query=test", nil)
	require.NoError(t, err)

	tests := []struct {
		name                 string
		server               string
		req                  *http.Request
		statusCode           int
		additionalAttributes []attribute.KeyValue
		wantFunc             func(t *testing.T, attrs []attribute.KeyValue)
	}{
		{
			name:                 "routine testing",
			req:                  defaultRequest,
			statusCode:           200,
			additionalAttributes: []attribute.KeyValue{attribute.String("test", "test")},
			wantFunc: func(t *testing.T, attrs []attribute.KeyValue) {
				require.Len(t, attrs, 7)
				assert.ElementsMatch(t, []attribute.KeyValue{
					attribute.String("http.request.method", "GET"),
					attribute.String("server.address", "example.com"),
					attribute.String("url.scheme", "http"),
					attribute.String("network.protocol.name", "http"),
					attribute.String("network.protocol.version", "1.1"),
					attribute.Int64("http.response.status_code", 200),
					attribute.String("test", "test"),
				}, attrs)
			},
		},
		{
			name:                 "use server address",
			req:                  defaultRequest,
			statusCode:           200,
			additionalAttributes: nil,
			wantFunc: func(t *testing.T, attrs []attribute.KeyValue) {
				require.Len(t, attrs, 6)
				assert.ElementsMatch(t, []attribute.KeyValue{
					attribute.String("http.request.method", "GET"),
					attribute.String("server.address", "example.com"),
					attribute.String("url.scheme", "http"),
					attribute.String("network.protocol.name", "http"),
					attribute.String("network.protocol.version", "1.1"),
					attribute.Int64("http.response.status_code", 200),
				}, attrs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CurrentHTTPClient{}.MetricAttributes(tt.req, tt.statusCode, tt.additionalAttributes)
			tt.wantFunc(t, got)
		})
	}
}
