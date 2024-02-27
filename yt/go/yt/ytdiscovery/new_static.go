package ytdiscovery

import (
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/discoveryclient"
)

func checkNotInsideJob(c *yt.DiscoveryConfig) error {
	if c.AllowRequestsFromJob {
		return nil
	}

	if mapreduce.InsideJob() {
		return xerrors.New("requests to cluster from inside job are forbidden")
	}

	return nil
}

// NewStatic creates discovery client from config with static discovery servers list.
func NewStatic(c *yt.DiscoveryConfig) (yt.DiscoveryClient, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	if len(c.DiscoveryServers) == 0 {
		return nil, xerrors.New("discovery servers can not be empty")
	}

	return discoveryclient.New(c, yt.NewStaticDiscoverer(c))
}
