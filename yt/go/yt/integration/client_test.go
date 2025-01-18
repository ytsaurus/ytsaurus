package integration

import (
	"context"
	"testing"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/clienttest"
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
	httpClient := clienttest.NewHTTPClient(t, s.L)
	rpcClient := clienttest.NewRPCClient(t, s.L)

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
