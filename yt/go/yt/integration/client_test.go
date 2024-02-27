package integration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
	"go.ytsaurus.tech/yt/go/ytlog"
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
	Test    func(t *testing.T, yc yt.Client)
	SkipRPC bool
}

func RunClientTests(t *testing.T, tests []ClientTest) {
	httpClient := NewHTTPClient(t)
	rpcClient := NewRPCClient(t)

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
						test.Test(t, tc.client)
					})
				}
			}
		})
	}
}

func NewHTTPClient(t *testing.T) yt.Client {
	t.Helper()

	yc, err := ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
	require.NoError(t, err)

	return yc
}

func NewRPCClient(t *testing.T) yt.Client {
	t.Helper()

	yc, err := ytrpc.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
	require.NoError(t, err)

	return yc
}
