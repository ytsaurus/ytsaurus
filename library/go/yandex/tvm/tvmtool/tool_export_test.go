package tvmtool

import "go.ytsaurus.tech/library/go/yandex/tvm/tvmtool/internal/cache"

const (
	CacheTTL    = cacheTTL
	CacheMaxTTL = cacheMaxTTL
)

func (c *Client) BaseURI() string {
	return c.baseURI
}

func (c *Client) AuthToken() string {
	return c.authToken
}

func (c *Client) Cache() *cache.Cache {
	return c.cache
}
