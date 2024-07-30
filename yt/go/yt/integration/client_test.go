package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
	"go.ytsaurus.tech/yt/go/yttest"
)

type Suite struct {
	*yttest.Env
}

func NewSuite(t *testing.T) *Suite {
	return &Suite{Env: yttest.New(t)}
}

type ClientTest struct {
	Name    string
	Test    func(ctx context.Context, t *testing.T, yc yt.Client)
	SkipRPC bool
}

func (s *Suite) RunClientTests(t *testing.T, tests []ClientTest) {
	httpClient := NewHTTPClient(t, s.L)
	rpcClient := NewRPCClient(t, s.L)

	for _, tc := range []struct {
		name   string
		client yt.Client
	}{
		{name: "http", client: httpClient},
		{name: "rpc", client: rpcClient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, test := range tests {
				if tc.name == "rpc" && test.SkipRPC {
					t.Skip("rpc test is skipped")
				} else {
					t.Run(test.Name, func(t *testing.T) {
						ctx := ctxlog.WithFields(s.Ctx, log.String("subtest_name", t.Name()))
						test.Test(ctx, t, tc.client)
					})
				}
			}
		})
	}
}

func NewHTTPClient(t *testing.T, l log.Structured) yt.Client {
	t.Helper()

	yc, err := ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
	require.NoError(t, err)

	return yc
}

func NewRPCClient(t *testing.T, l log.Structured) yt.Client {
	t.Helper()

	yc, err := ytrpc.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: l})
	require.NoError(t, err)

	return yc
}
