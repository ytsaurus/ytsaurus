package yt

import (
	"os"
	"strings"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/core/log"
)

type Config struct {
	Proxy string
	Token string

	Logger log.Logger
}

func NormalizeProxyURL(proxy string) string {
	const prefix = "http://"
	const suffix = ".yt.yandex.net"

	if !strings.Contains(proxy, ".") && !strings.Contains(proxy, ":") && !strings.Contains(proxy, "localhost") {
		proxy += suffix
	}

	if !strings.HasPrefix(proxy, prefix) {
		proxy = prefix + proxy
	}

	return proxy
}

func ClusterFromEnv() (*Config, error) {
	proxy := os.Getenv("YT_PROXY")
	if proxy == "" {
		return nil, xerrors.New("YT_PROXY environment variable is not set")
	}

	token := os.Getenv("YT_TOKEN")
	return &Config{Proxy: proxy, Token: token}, nil

}
