package rpcclient

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestUnixConfig(t *testing.T) {
	for _, conf := range []*yt.Config{
		{RPCProxyUnixSocket: "/tmp/proxy.sock", RPCProxy: "localhost:1"},
		{RPCProxyUnixSocket: "/tmp/proxy.sock", UseTLS: true},
	} {
		_, err := NewClient(conf)
		require.Error(t, err)
	}
	t.Setenv("YT_PROXY", "")
	c, err := NewClient(&yt.Config{RPCProxyUnixSocket: "/tmp/proxy.sock"})
	require.NoError(t, err)
	defer c.Stop()
	// Even banning the endpoint must not cause discovery or a TCP fallback.
	c.proxySet.BanProxy("/tmp/proxy.sock")
	addr, err := c.pickRPCProxy(context.Background())
	require.NoError(t, err)
	require.Equal(t, "/tmp/proxy.sock", addr)
}

func TestUnixRequestDeadline(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "rpc.sock")
	l, err := net.Listen("unix", socket)
	require.NoError(t, err)
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			accepted <- c
		}
		close(accepted)
	}()
	t.Cleanup(func() {
		l.Close()
		for c := range accepted {
			c.Close()
		}
	})
	c, err := NewClient(&yt.Config{RPCProxyUnixSocket: socket})
	require.NoError(t, err)
	defer c.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var value string
	start := time.Now()
	err = c.GetNode(ctx, ypath.Path("//sys/@cluster_name"), &value, nil)
	require.Error(t, err)
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.Less(t, time.Since(start), 2*time.Second)
}
