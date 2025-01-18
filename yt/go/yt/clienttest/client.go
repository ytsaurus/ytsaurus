package clienttest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
)

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
