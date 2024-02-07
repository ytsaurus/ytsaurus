package auth

// TODO(dakovalkov): This file contains copy-pasted code from ythttputil package.
// We need to avoid dependency on ythttputil, because it depends on unwanted
// third party packages (e.g. blackbox).
// The right way is to split ythttputil package into smaller ones.

import (
	"context"
	"net/http"
	"strings"

	"go.ytsaurus.tech/yt/go/yt"
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

func GetCredentials(r *http.Request) (yt.Credentials, error) {
	if r.Header.Get("Authorization") != "" {
		token, err := GetTokenFromHeader(r)
		if err != nil {
			return nil, err
		}
		return &yt.TokenCredentials{Token: token}, nil
	}

	if cookie, err := r.Cookie(yt.YTCypressCookie); err == nil {
		credentials := &yt.CookieCredentials{
			Cookie:    cookie,
			CSRFToken: r.Header.Get(yt.XCSRFToken),
		}
		return credentials, nil
	}

	if r.Header.Get(yt.XYaUserTicket) != "" {
		return &yt.UserTicketCredentials{Ticket: r.Header.Get(yt.XYaUserTicket)}, nil
	}

	if r.Header.Get(yt.XYaServiceTicket) != "" {
		return &yt.ServiceTicketCredentials{Ticket: r.Header.Get(yt.XYaServiceTicket)}, nil
	}

	return nil, yterrors.Err("client is missing credentials")
}
