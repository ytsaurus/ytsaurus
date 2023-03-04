package rpcclient

import (
	"os"
	"testing"

	"a.yandex-team.ru/yt/go/yt"
)

// NewTestClient creates new rpc client from config to be used in integration tests.
func NewTestClient(t testing.TB, c *yt.Config) (yt.Client, error) {
	if os.Getenv("YT_PROXY") == "" {
		t.Skip("Skipping testing as there is no local yt.")
	}

	return NewClient(c)
}
