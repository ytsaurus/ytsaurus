package httpclient

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/yt"
)

func TestErrorDecoding(t *testing.T) {
	h := http.Header{}

	ytErr, err := decodeYTErrorFromHeaders(h)
	require.NoError(t, err)
	require.Nil(t, ytErr)
}

func TestDecodeFidAndTid(t *testing.T) {
	h := http.Header{
		http.CanonicalHeaderKey("X-YT-Error"): []string{`{"message": "error", "attributes": {"trace_id": 1197470164874488892, "span_id": 298838154694968802}}`},
	}

	ytErr, err := decodeYTErrorFromHeaders(h)
	require.NoError(t, err)
	require.NotNil(t, ytErr)

	require.Equal(t, json.Number("1197470164874488892"), ytErr.Attributes["trace_id"])
	require.Equal(t, json.Number("298838154694968802"), ytErr.Attributes["span_id"])
}

func TestHTTPClient_listHeavyProxies(t *testing.T) {
	makeClient := func(t *testing.T, conf *yt.Config) *httpClient {
		t.Helper()

		iface, err := NewTestHTTPClient(t, conf)
		require.NoError(t, err)

		c, ok := iface.(*httpClient)
		require.True(t, ok)

		return c
	}

	t.Run("default", func(t *testing.T) {
		c := makeClient(t, &yt.Config{
			Proxy: os.Getenv("YT_PROXY"),
		})

		proxies, err := c.listHeavyProxies()
		require.NoError(t, err)
		require.NotEmpty(t, proxies)
	})

	t.Run("data", func(t *testing.T) {
		c := makeClient(t, &yt.Config{
			Proxy:     os.Getenv("YT_PROXY"),
			ProxyRole: "data",
		})

		proxies, err := c.listHeavyProxies()
		require.NoError(t, err)
		require.NotEmpty(t, proxies)
	})

	t.Run("missing", func(t *testing.T) {
		c := makeClient(t, &yt.Config{
			Proxy:     os.Getenv("YT_PROXY"),
			ProxyRole: "missing-role",
		})

		_, err := c.listHeavyProxies()
		require.Error(t, err)
	})
}
