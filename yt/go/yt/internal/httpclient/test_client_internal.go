//go:build internal
// +build internal

package httpclient

import (
	"testing"

	"a.yandex-team.ru/yt/go/yt"
)

// NewTestHTTPClient creates new http client from config to be used in integration tests.
func NewTestHTTPClient(t testing.TB, c *yt.Config) (yt.Client, error) {
	return NewHTTPClient(c)
}
