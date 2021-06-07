package httpclient

import (
	"context"
	"net/http"
)

type roundtrippedKey struct{}

func WithRoundTripper(ctx context.Context, rt http.RoundTripper) context.Context {
	return context.WithValue(ctx, roundtrippedKey{}, rt)
}

func GetRoundTripper(ctx context.Context) (http.RoundTripper, bool) {
	v := ctx.Value(roundtrippedKey{})
	if v == nil {
		return nil, false
	}

	return v.(http.RoundTripper), true
}
