//go:build internal
// +build internal

package rpcclient

import (
	"testing"

	"a.yandex-team.ru/yt/go/yt"
)

// NewTestClient creates new rpc client from config to be used in integration tests.
func NewTestClient(t testing.TB, c *yt.Config) (yt.Client, error) {
	return NewClient(c)
}
