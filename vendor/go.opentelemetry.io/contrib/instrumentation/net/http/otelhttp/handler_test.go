// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelhttp_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func TestHandler(t *testing.T) {
	testCases := []struct {
		name               string
		handler            func(*testing.T) http.Handler
		requestBody        io.Reader
		expectedStatusCode int
	}{
		{
			name: "implements flusher",
			handler: func(t *testing.T) http.Handler {
				return otelhttp.NewHandler(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						assert.Implements(t, (*http.Flusher)(nil), w)
					}), "test_handler",
				)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "succeeds",
			handler: func(t *testing.T) http.Handler {
				return otelhttp.NewHandler(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						assert.NotNil(t, r.Body)

						b, err := io.ReadAll(r.Body)
						assert.NoError(t, err)
						assert.Equal(t, "hello world", string(b))
					}), "test_handler",
				)
			},
			requestBody:        strings.NewReader("hello world"),
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "succeeds with a nil body",
			handler: func(t *testing.T) http.Handler {
				return otelhttp.NewHandler(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						assert.Nil(t, r.Body)
					}), "test_handler",
				)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "succeeds with an http.NoBody",
			handler: func(t *testing.T) http.Handler {
				return otelhttp.NewHandler(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						assert.Equal(t, http.NoBody, r.Body)
					}), "test_handler",
				)
			},
			requestBody:        http.NoBody,
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "http://localhost/", tc.requestBody)
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			tc.handler(t).ServeHTTP(rr, r)
			assert.Equal(t, tc.expectedStatusCode, rr.Result().StatusCode)
		})
	}
}
