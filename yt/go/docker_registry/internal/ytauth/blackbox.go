package yt

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"a.yandex-team.ru/library/go/yandex/blackbox"
	"a.yandex-team.ru/library/go/yandex/blackbox/httpbb"
)

func authenticateUserViaBlackBox(ctx context.Context, userToken string) (string, error) {
	bb, err := httpbb.NewIntranet()
	if err != nil {
		return "", err
	}

	tokenReq := blackbox.OAuthRequest{
		Attributes: []blackbox.UserAttribute{blackbox.UserAttributeAccountNormalizedLogin},
		OAuthToken: userToken,
		UserIP:     "127.0.0.1",
	}

	rsp, err := bb.OAuth(ctx, tokenReq)
	if err != nil {
		return "", err
	}

	return rsp.User.Login, nil
}

func getUserToken(s string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}

	parts := strings.SplitN(string(data), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("can't decode basic auth header")
	}
	return parts[1], nil
}
