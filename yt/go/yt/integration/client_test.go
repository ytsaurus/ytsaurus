package integration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/yttest"
)

type Suite struct {
	*yttest.Env
}

func NewSuite(t *testing.T) *Suite {
	return &Suite{Env: yttest.New(t)}
}

type ClientTest struct {
	Name string
	Test func(t *testing.T, yc yt.Client)
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
				t.Run(test.Name, func(t *testing.T) {
					test.Test(t, tc.client)
				})
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
