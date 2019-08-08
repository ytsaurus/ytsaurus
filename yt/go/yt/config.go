package yt

import (
	"os"
	"strings"
	"time"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/core/log"
)

const DefaultTxTimeout = 15 * time.Second

type Config struct {
	Proxy string
	Token string

	Logger log.Structured
}

func NewConfigFromEnv() (*Config, error) {
	var c Config
	c.Proxy = os.Getenv("YT_PROXY")
	if c.Proxy == "" {
		return nil, xerrors.New("YT_PROXY environment variable is not set")
	}

	c.Token = os.Getenv("YT_TOKEN")
	return &c, nil
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
