package httpclient

import (
	"os"
	"testing"

	"a.yandex-team.ru/yt/go/yt"
)

// NewTestHTTPClient creates new http client from config to be used in integration tests.
func NewTestHTTPClient(t testing.TB, c *yt.Config) (yt.Client, error) {
	if os.Getenv("YT_PROXY") == "" {
		t.Skip("Skipping testing as there is no local yt.")
	}

	return NewHTTPClient(c)
}
