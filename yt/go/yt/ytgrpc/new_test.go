package ytgrpc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytgrpc"
)

func TestNewClientRejectsTVMOnlyEndpoint(t *testing.T) {
	_, err := ytgrpc.NewClient(&yt.Config{UseTVMOnlyEndpoint: true, Proxy: "localhost"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "UseTVMOnlyEndpoint")
}
