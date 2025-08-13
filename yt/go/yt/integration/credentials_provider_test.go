package integration

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestCredentialsProviderBasic(t *testing.T) {
	var callCount int32

	credentialsProvider := func(ctx context.Context) (yt.Credentials, error) {
		atomic.AddInt32(&callCount, 1)
		return &yt.BearerCredentials{Token: "token123"}, nil
	}

	env := yttest.New(t, yttest.WithConfig(yt.Config{
		CredentialsProviderFn: credentialsProvider,
	}))

	_, _ = env.YT.WhoAmI(env.Ctx, nil)
	require.Greater(t, atomic.LoadInt32(&callCount), int32(0), "CredentialsProviderFn should have been called for HTTP client")
}

func TestCredentialsProviderCalledOnEachRequest(t *testing.T) {
	var callCount int32

	credentialsProvider := func(ctx context.Context) (yt.Credentials, error) {
		atomic.AddInt32(&callCount, 1)
		return &yt.TokenCredentials{Token: "token123"}, nil
	}

	env := yttest.New(t, yttest.WithConfig(yt.Config{
		CredentialsProviderFn: credentialsProvider,
	}))

	initialCallCount := atomic.LoadInt32(&callCount)

	_, _ = env.YT.WhoAmI(env.Ctx, nil)
	_, _ = env.YT.NodeExists(env.Ctx, ypath.Path("//home"), nil)

	finalCallCount := atomic.LoadInt32(&callCount)
	require.Greater(t, finalCallCount, initialCallCount, "CredentialsProviderFn should be called for each request")
	require.GreaterOrEqual(t, finalCallCount, int32(2), "CredentialsProviderFn should be called at least twice")
}

func TestCredentialsProviderError(t *testing.T) {
	var callCount int32

	credentialsProvider := func(ctx context.Context) (yt.Credentials, error) {
		atomic.AddInt32(&callCount, 1)
		return nil, xerrors.New("credentials fetch failed")
	}

	env := yttest.New(t, yttest.WithConfig(yt.Config{
		CredentialsProviderFn: credentialsProvider,
	}))

	_, err := env.YT.WhoAmI(env.Ctx, nil)

	require.Error(t, err, "Request should fail when CredentialsProviderFn returns error")
	require.Greater(t, atomic.LoadInt32(&callCount), int32(0), "CredentialsProviderFn should have been called")
}

func TestCredentialsProviderWithRPCClient(t *testing.T) {
	env := yttest.New(t)

	var callCount int32

	credentialsProvider := func(ctx context.Context) (yt.Credentials, error) {
		atomic.AddInt32(&callCount, 1)
		return &yt.TokenCredentials{Token: "token123"}, nil
	}

	rpcClient, err := ytrpc.NewClient(&yt.Config{
		CredentialsProviderFn: credentialsProvider,
	})
	require.NoError(t, err)

	_, _ = rpcClient.NodeExists(env.Ctx, ypath.Path("//home"), nil)
	require.Greater(t, atomic.LoadInt32(&callCount), int32(0), "CredentialsProviderFn should been called for RPC client")
}
