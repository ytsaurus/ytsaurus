package auth

// TODO(dakovalkov): This file contains copy-pasted code from ythttputil package.
// We need to avoid dependency on ythttputil, because it depends on unwanted
// third party packages (e.g. blackblox).
// The right way is to split ythttputil package into smaller ones.

import (
	"context"
	"net/http"
	"strings"

	"go.ytsaurus.tech/yt/go/yterrors"
)

type requester struct{}

var requesterKey requester

func WithRequester(ctx context.Context, requester string) context.Context {
	return context.WithValue(ctx, &requesterKey, requester)
}

func ContextRequester(ctx context.Context) (requester string, ok bool) {
	if v := ctx.Value(&requesterKey); v != nil {
		requester = v.(string)
		ok = requester != ""
	}
	return
}

func GetTokenFromHeader(r *http.Request) (string, error) {
	const oauthPrefix = "OAuth "

	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		return "", yterrors.Err("client is missing credentials")
	}

	if !strings.HasPrefix(authorization, oauthPrefix) {
		return "", yterrors.Err("invalid authorization header format")
	}

	token := strings.TrimSpace(authorization[len(oauthPrefix):])
	return token, nil
}
