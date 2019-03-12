// package ythttp provides YT client over HTTP protocol.
package ythttp

import (
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal/httpclient"
)

func NewClient(c *yt.Config) (yt.Client, error) {
	return httpclient.NewHTTPClient(c)
}

// NewClientFromEnv creates YT client configured from environment variables.
//
//   YT_PROXY - required variable specifying cluster address.
//   YT_TOKEN - optional variable specifying token.
func NewClientFromEnv() (yt.Client, error) {
	c, err := yt.ClusterFromEnv()
	if err != nil {
		return nil, err
	}

	return httpclient.NewHTTPClient(c)
}
