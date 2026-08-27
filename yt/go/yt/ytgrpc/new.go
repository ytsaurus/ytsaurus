package ytgrpc

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

// NewClient creates new gRPC RPC-proxy client from config.
//
// Note! Table and File clients have stub implementations.
// If you need one of those use http client instead.
func NewClient(c *yt.Config) (yt.Client, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewGRPCClient(c)
}

// NewTestClient creates new gRPC client from config to be used in integration tests.
//
// Note! Table and File clients have stub implementations.
// If you need one of those use http client instead.
func NewTestClient(t testing.TB, c *yt.Config) (yt.Client, error) {
	if err := checkNotInsideJob(c); err != nil {
		return nil, err
	}

	return rpcclient.NewGRPCTestClient(t, c)
}
