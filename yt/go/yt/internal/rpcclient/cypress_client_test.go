package rpcclient

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestCypressClient_listRPCProxies(t *testing.T) {
	_ = yttest.New(t)

	t.Run("empty", func(t *testing.T) {
		c, err := NewCypressClient(&yt.Config{
			Proxy: os.Getenv("YT_PROXY"),
		})
		require.NoError(t, err)

		proxies, err := c.listRPCProxies()
		require.NoError(t, err)
		t.Logf("got %d local rpc proxies: %s", len(proxies), proxies)
		require.NotEmpty(t, proxies)
	})

	t.Run("default", func(t *testing.T) {
		c, err := NewCypressClient(&yt.Config{
			Proxy:     os.Getenv("YT_PROXY"),
			ProxyRole: "default",
		})
		require.NoError(t, err)

		proxies, err := c.listRPCProxies()
		require.NoError(t, err)
		t.Logf("got %d local rpc proxies: %s", len(proxies), proxies)
		require.NotEmpty(t, proxies)
	})

	t.Run("missing", func(t *testing.T) {
		c, err := NewCypressClient(&yt.Config{
			Proxy:     os.Getenv("YT_PROXY"),
			ProxyRole: "missing-role",
		})
		require.NoError(t, err)

		_, err = c.listRPCProxies()
		require.Error(t, err)
	})
}
