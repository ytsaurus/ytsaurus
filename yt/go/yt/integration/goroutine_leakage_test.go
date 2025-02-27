package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/clienttest"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestClientGoroutineLeakage(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	httpClient := clienttest.NewHTTPClient(t, ytEnv.L)
	defer httpClient.Stop()
	rpcClient := clienttest.NewRPCClient(t, ytEnv.L)
	defer rpcClient.Stop()

	_, err := httpClient.CreateNode(ctx, ytEnv.TmpPath(), yt.NodeTable, nil)
	require.NoError(t, err)

	_, err = rpcClient.CreateNode(ctx, ytEnv.TmpPath(), yt.NodeTable, nil)
	require.NoError(t, err)
}
