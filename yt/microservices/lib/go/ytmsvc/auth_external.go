//go:build !internal
// +build !internal

package ytmsvc

import (
	"net/http"

	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

type User struct {
	Login string
}

func GetUserByAuthCookie(r *http.Request, l *zap.Logger, authCookieName string, authProxy string) (*User, *string, error) {

	cookie, err := r.Cookie(authCookieName)
	if err != nil {
		return nil, nil, err
	}
	if cookie == nil {
		return nil, nil, xerrors.New("cookie is empty")
	}

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  authProxy,
		Logger: l,
		Credentials: &yt.CookieCredentials{
			Cookie: cookie,
		},
	})
	if err != nil {
		return nil, nil, err
	}

	ctx := r.Context()
	user, err := yc.WhoAmI(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	return &User{Login: user.Login}, &cookie.Value, nil
}

func GetUser(r *http.Request, l *zap.Logger, authCookieName string, authProxy string) (*User, *string, error) {
	if authCookieName != "" && authProxy != "" {
		return GetUserByAuthCookie(r, l, authCookieName, authProxy)
	}
	return nil, nil, xerrors.New("unsupported auth method")
}
