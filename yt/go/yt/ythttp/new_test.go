package ythttp_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

func TestBuildHTTPClient(t *testing.T) {
	t.Run("with custom HTTPClient", func(t *testing.T) {
		customClient := &http.Client{
			Timeout: 10 * time.Second,
		}

		config := &yt.Config{
			Proxy:      "localhost",
			HTTPClient: customClient,
		}

		client, err := ythttp.BuildHTTPClient(config)
		require.NoError(t, err)
		require.Equal(t, customClient, client)
		require.Equal(t, 10*time.Second, client.Timeout)
	})

	t.Run("without custom HTTPClient", func(t *testing.T) {
		config := &yt.Config{
			Proxy: "localhost",
		}

		client, err := ythttp.BuildHTTPClient(config)
		require.NoError(t, err)
		require.NotNil(t, client)
		require.NotNil(t, client.Transport)

		// Verify default settings are applied
		transport, ok := client.Transport.(*http.Transport)
		require.True(t, ok)
		require.Equal(t, 100, transport.MaxConnsPerHost)
		require.Equal(t, 0, transport.MaxIdleConns)
		require.Equal(t, 100, transport.MaxIdleConnsPerHost)
		require.Equal(t, 30*time.Second, transport.IdleConnTimeout)
		require.Equal(t, 10*time.Second, transport.TLSHandshakeTimeout)
	})
}
