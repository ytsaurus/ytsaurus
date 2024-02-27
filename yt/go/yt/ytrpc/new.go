package ytrpc

import (
	"testing"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/rpcclient"
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

// NewClient creates new client from config.
//
// Note! Table and File clients have stub implementations.
// If you need one of those use http client instead.
func NewClient(c *yt.Config) (yt.Client, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewClient(c)
}

// NewTestClient creates new client from config to be used in integration tests.
//
// Note! Table and File clients have stub implementations.
// If you need one of those use http client instead.
func NewTestClient(t testing.TB, c *yt.Config) (yt.Client, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewTestClient(t, c)
}

// NewCypressClient creates new cypress client from config.
func NewCypressClient(c *yt.Config) (yt.CypressClient, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewClient(c)
}

// NewLowLevelTxClient creates new stateless transaction client from config.
//
// Clients should rarely use it directly.
func NewLowLevelTxClient(c *yt.Config) (yt.LowLevelTxClient, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewClient(c)
}

// NewAdminClient creates new admin client from config.
func NewAdminClient(c *yt.Config) (yt.AdminClient, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewClient(c)
}

// NewLowLevelSchedulerClient creates new stateless scheduler client from config.
//
// Clients should rarely use it directly.
//
// Note! RPC streaming call GetJobStderr is not implemented yet.
func NewLowLevelSchedulerClient(c *yt.Config) (yt.LowLevelSchedulerClient, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewClient(c)
}
