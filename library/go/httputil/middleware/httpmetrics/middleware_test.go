package httpmetrics

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.ytsaurus.tech/library/go/core/metrics/solomon"
)

func fakeHandler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	})
}

func panicHandler(_ http.ResponseWriter, _ *http.Request) {
	panic("hello")
}

func metricsToJSON(r *solomon.Registry, t *testing.T) string {
	var buf bytes.Buffer
	_, err := r.StreamJSON(context.Background(), &buf)
	assert.NoError(t, err, "Unexpected error on streaming metrics to JSON")
	return buf.String()
}

func TestMiddleware(t *testing.T) {
	r := solomon.NewRegistry(solomon.NewRegistryOpts())

	middleware := New(r, WithPathEndpoint(), WithHTTPCodes(404))

	w := httptest.NewRecorder()

	middleware(fakeHandler(200)).ServeHTTP(w, httptest.NewRequest("GET", "/items", nil))
	middleware(fakeHandler(404)).ServeHTTP(w, httptest.NewRequest("POST", "/users", nil))

	func() {
		defer func() { _ = recover() }()

		middleware(http.HandlerFunc(panicHandler)).ServeHTTP(w, httptest.NewRequest("POST", "/panic", nil))
	}()
}

func TestMiddleware_DefaultOptions(t *testing.T) {
	r := solomon.NewRegistry(solomon.NewRegistryOpts())

	middleware := New(r)

	w := httptest.NewRecorder()

	middleware(fakeHandler(200)).ServeHTTP(w, httptest.NewRequest("GET", "/items", nil))
}

func TestMiddleware_httpCodeDefaultTag(t *testing.T) {
	r := solomon.NewRegistry(solomon.NewRegistryOpts())

	middleware := New(r, WithHTTPCodes(400))

	w := httptest.NewRecorder()

	middleware(fakeHandler(405)).ServeHTTP(w, httptest.NewRequest("HEAD", "/items", nil))
}

func TestMiddleware_WithPreinitializedEndpoints(t *testing.T) {
	r := solomon.NewRegistry(solomon.NewRegistryOpts())

	key := "/goods"
	middleware := New(r, WithPreinitializedEndpoints(key))

	w := httptest.NewRecorder()

	middleware(fakeHandler(200)).
		ServeHTTP(w, httptest.NewRequest("HEAD", "/items", nil))

	metricsJSON := metricsToJSON(r, t)
	contains := strings.Contains(metricsJSON, fmt.Sprintf(`"endpoint":"%s"`, key))
	if !contains {
		t.Errorf(
			"Expect metrics to contain initialized %q endpoint subset of metrics, got:\n%s",
			key, metricsJSON,
		)
	}
}

func TestMiddleware_WithSkipFunc(t *testing.T) {
	r := solomon.NewRegistry(solomon.NewRegistryOpts())

	skippingMiddleware := New(r, WithPathEndpoint(), WithSkipFunc(func(req *http.Request) bool { return req.URL.Path == "/items" }))

	handler := fakeHandler(200)

	skippingMiddleware(handler).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/items", nil))
	skippingMiddleware(handler).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/price", nil))

	metricsJSON := metricsToJSON(r, t)
	assert.NotContains(t, metricsJSON, "/items", "Registry does not contain metrics for /items endpoint")
	assert.Contains(t, metricsJSON, "/price", "Registry contain unexpected metrics for /price endpoint")

	plainMiddleware := New(r, WithPathEndpoint())

	plainMiddleware(handler).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/items", nil))
	metricsJSON = metricsToJSON(r, t)
	assert.Contains(t, metricsJSON, "/items", "Registry does not contain metrics for /items endpoint")
}
