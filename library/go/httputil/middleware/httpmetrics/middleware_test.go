package httpmetrics

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

	var buff bytes.Buffer
	_, err := r.StreamJSON(context.Background(), &buff)
	if err != nil {
		t.Errorf("Unexpected error on stream json: %v", err)
		t.FailNow()
	}
	contains := strings.Contains(buff.String(), fmt.Sprintf(`"endpoint":"%s"`, key))
	if !contains {
		t.Errorf(
			"Expect metrics to contain initialized %q endpoint subset of metrics, got:\n%s",
			key, buff.String(),
		)
	}
}
