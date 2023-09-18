package discoveryclient

import (
	"context"
)

func (c *client) pickDiscoveryServer(ctx context.Context) (string, error) {
	proxy, err := c.proxySet.PickRandom(ctx)
	if err != nil {
		return "", err
	}

	return proxy, nil
}
