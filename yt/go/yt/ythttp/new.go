// package ythttp provides YT client over HTTP protocol.
package ythttp

import (
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal/httpclient"
)

func NewClient(c *yt.Config) (yt.Client, error) {
	return httpclient.NewHTTPClient(c)
}
