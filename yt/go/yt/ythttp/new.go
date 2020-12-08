// package ythttp provides YT client over HTTP protocol.
package ythttp

import (
	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal/httpclient"
)

func checkNotInsideJob() error {
	if mapreduce.RequestsFromJobAllowed() {
		return nil
	}

	if mapreduce.InsideJob() {
		return xerrors.New("requests to cluster from jobs are forbidden")
	}

	return nil
}

// NewClient creates new client from config.
func NewClient(c *yt.Config) (yt.Client, error) {
	if err := checkNotInsideJob(); err != nil {
		return nil, err
	}

	return httpclient.NewHTTPClient(c)
}
