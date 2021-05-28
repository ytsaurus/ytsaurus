package ytrpc

import (
	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal/rpcclient"
)

func checkNotInsideJob(c *yt.Config) error {
	if c.AllowRequestsFromJob {
		return nil
	}

	if mapreduce.InsideJob() {
		return xerrors.New("requests to cluster from inside job are forbidden")
	}

	return nil
}

// NewCypressClient creates new cypress client from config.
func NewCypressClient(c *yt.Config) (yt.CypressClient, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewCypressClient(c)
}
