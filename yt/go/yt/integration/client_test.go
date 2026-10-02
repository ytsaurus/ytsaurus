package integration

import (
	"context"
	"os"
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
	Name     string
	Test     func(ctx context.Context, t *testing.T, yc yt.Client)
	SkipRPC  bool
	SkipHTTP bool
	SkipGRPC bool
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
			s.runNamedClientTests(t, tc.name, tc.client, tests)
		})
	}

	t.Run("grpc", func(t *testing.T) {
		if os.Getenv("YT_TEST_GRPC") == "" {
			t.Skip("gRPC proxy tests are opt-in; set YT_TEST_GRPC=1 to enable")
		}
		grpcClient := clienttest.NewGRPCClient(t, s.L)
		s.runNamedClientTests(t, "grpc", grpcClient, tests)
	})
}

func (s *Suite) runNamedClientTests(t *testing.T, name string, client yt.Client, tests []ClientTest) {
	for _, test := range tests {
		skip := (name == "rpc" && test.SkipRPC) ||
			(name == "http" && test.SkipHTTP) ||
			(name == "grpc" && (test.SkipGRPC || test.SkipRPC))
		if skip {
			continue
		}

		t.Run(test.Name, func(t *testing.T) {
			ctx := ctxlog.WithFields(s.Ctx, log.String("subtest_name", t.Name()))
			test.Test(ctx, t, client)
		})
	}
}
