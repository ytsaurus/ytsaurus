//go:build !internal
// +build !internal

package common

import (
	"errors"
	"net/http"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/proto/core/rpc"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

type User struct {
	Login string
}

type cookieCredentials struct {
	cookie    *http.Cookie
	csrfToken string
}

func (c cookieCredentials) Set(r *http.Request) {
	r.AddCookie(c.cookie)
	r.Header.Set("X-Csrf-Token", c.csrfToken)
}

func (c cookieCredentials) SetExtension(req *rpc.TRequestHeader) {
	_ = proto.SetExtension(
		req,
		rpc.E_TCredentialsExt_CredentialsExt,
		&rpc.TCredentialsExt{SessionId: &c.cookie.Value},
	)
}

func GetUserByAuthCookie(r *http.Request, l *zap.Logger, authCookieName string, authProxy string) (*User, *string, error) {

	cookie, err := r.Cookie(authCookieName)
	if err != nil || cookie == nil {
		return nil, nil, errors.New("Authentication failed [getting cookie]: " + err.Error())
	}

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:       authProxy,
		Logger:      l,
		Credentials: yt.CookieCredentials{},
	})
	if err != nil {
		return nil, nil, errors.New("Authentication failed [create yt client]: " + err.Error())
	}

	ctx := r.Context()
	user, err := yc.WhoAmI(ctx, nil)
	if err != nil {
		return nil, nil, errors.New("Authentication failed [whoami]: " + err.Error())
	}
	return &User{Login: user.Login}, &cookie.Value, nil
}

func GetUser(r *http.Request, l *zap.Logger, authCookieName string, authProxy string) (*User, *string, error) {
	if authCookieName != "" && authProxy != "" {
		return GetUserByAuthCookie(r, l, authCookieName, authProxy)
	}
	return nil, nil, errors.New("unsupported auth method")
}
