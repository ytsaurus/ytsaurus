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

type HeavyProxyOverride func() (string, bool)

type heavyProxyOverrideKey struct{}

func WithHeavyProxyOverride(ctx context.Context, override HeavyProxyOverride) context.Context {
	return context.WithValue(ctx, heavyProxyOverrideKey{}, override)
}

func GetHeavyProxyOverride(ctx context.Context) (string, bool) {
	v := ctx.Value(heavyProxyOverrideKey{})
	if v == nil {
		return "", false
	}

	return v.(HeavyProxyOverride)()
}
