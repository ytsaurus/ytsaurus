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

type ClusterURL struct {
	URL              string
	DisableDiscovery bool
}

func NormalizeProxyURL(proxy string) ClusterURL {
	const prefix = "http://"
	const suffix = ".yt.yandex.net"

	var url ClusterURL
	if !strings.Contains(proxy, ".") && !strings.Contains(proxy, ":") && !strings.Contains(proxy, "localhost") {
		proxy += suffix
	}

	if strings.ContainsAny(proxy, "0123456789") {
		url.DisableDiscovery = true
	}

	if !strings.HasPrefix(proxy, prefix) {
		proxy = prefix + proxy
	}

	url.URL = proxy
	return url
}

func ClusterFromEnv() (*Config, error) {
	proxy := os.Getenv("YT_PROXY")
	if proxy == "" {
		return nil, xerrors.New("YT_PROXY environment variable is not set")
	}

	token := os.Getenv("YT_TOKEN")
	return &Config{Proxy: proxy, Token: token}, nil
}
