package discovery

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func CreateClient(ctx context.Context, ytc yt.Client) (yt.DiscoveryClient, error) {
	var discoveryConnection struct {
		Addresses *[]string `yson:"addresses"`
		Endpoints *struct {
			EndpointSetID string   `yson:"endpoint_set_id"`
			Clusters      []string `yson:"clusters"`
		} `yson:"endpoints"`
	}

	err := ytc.GetNode(ctx, ypath.Path("//sys/@cluster_connection/discovery_connection"), &discoveryConnection, nil)
	if err != nil {
		return nil, err
	}

	var cfg yt.DiscoveryConfig
	if discoveryConnection.Addresses != nil {
		cfg.DiscoveryServers = *discoveryConnection.Addresses
	}
	if discoveryConnection.Endpoints != nil {
		cfg.EndpointSet = discoveryConnection.Endpoints.EndpointSetID
		cfg.YPClusters = discoveryConnection.Endpoints.Clusters
	}

	return newClient(cfg)
}
