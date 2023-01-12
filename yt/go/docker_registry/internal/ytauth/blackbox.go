package yt

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"a.yandex-team.ru/library/go/yandex/blackbox"
	"a.yandex-team.ru/library/go/yandex/blackbox/httpbb"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

const (
	QloudTVMTokenEnvName = "QLOUD_TVM_TOKEN"
	TVMAppAlias          = "yt-docker-registry"
)

func getBlackBoxClient() (*httpbb.Client, error) {
	opts := make([]httpbb.Option, 1)
	if _, ok := os.LookupEnv(QloudTVMTokenEnvName); ok {
		tvmClient, err := tvmtool.NewQloudClient(tvmtool.WithSrc(TVMAppAlias))
		if err != nil {
			return nil, fmt.Errorf("failed to create tvm client: %w", err)
		}
		opts = append(opts, httpbb.WithTVM(tvmClient))
	}
	return httpbb.NewIntranet(opts...)
}

func authenticateUserViaBlackBox(ctx context.Context, userToken string) (string, error) {
	bb, err := getBlackBoxClient()
	if err != nil {
		return "", fmt.Errorf("failed to create blackbox client %w", err)
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
