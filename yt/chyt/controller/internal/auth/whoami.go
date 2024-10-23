package auth

import (
	"context"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func ContainsUnauthorized(err error) bool {
	return yterrors.ContainsErrorCode(err, yterrors.CodeInvalidCredentials) ||
		yterrors.ContainsErrorCode(err, yterrors.CodeAuthenticationError)
}

func WhoAmI(ctx context.Context, proxy string, credentials yt.Credentials) (username string, err error) {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:       proxy,
		Credentials: credentials,
	})
	if err != nil {
		return "", err
	}

	res, err := yc.WhoAmI(ctx, nil)
	if err != nil {
		return "", err
	}
	return res.Login, nil
}
