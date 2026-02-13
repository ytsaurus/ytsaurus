package chyt

import (
	"errors"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytdiscovery"
)

func newDiscoveryClient(cfg yt.DiscoveryConfig) (yt.DiscoveryClient, error) {
	if len(cfg.DiscoveryServers) > 0 {
		return ytdiscovery.NewStatic(&cfg)
	}

	return nil, errors.New("discovery config is not initialized")
}
