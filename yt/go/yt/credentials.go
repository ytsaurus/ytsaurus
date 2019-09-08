package yt

import (
	"context"
	"net/http"
)

type Credentials interface {
	Set(r *http.Request)
}

type TokenCredentials struct {
	Token string
}

func (c *TokenCredentials) Set(r *http.Request) {
	r.Header.Add("Authorization", "OAuth "+c.Token)
}

type credentials struct{}

var credentialsKey credentials

func ContextCredentials(ctx context.Context) Credentials {
	if v := ctx.Value(&credentialsKey); v != nil {
		return v.(Credentials)
	}

	return nil
}

// WithCredentials allows overriding client credentials on per-call basis.
func WithCredentials(ctx context.Context, credentials Credentials) context.Context {
	return context.WithValue(ctx, &credentialsKey, credentials)
}
