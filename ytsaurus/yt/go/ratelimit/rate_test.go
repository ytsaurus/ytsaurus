package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRate(t *testing.T) {
	count, dt, err := ParseRate("10/1s")
	require.NoError(t, err)
	require.Equal(t, 10, count)
	require.Equal(t, time.Second, dt)

	count, dt, err = ParseRate("1/1ms")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, time.Millisecond, dt)
}

func TestInvalidRate(t *testing.T) {
	for _, invalid := range []string{
		"",
		"1/2",
		"/10m",
		"abc",
	} {
		_, _, err := ParseRate(invalid)
		require.Errorf(t, err, "rate %q is invalid", invalid)
	}
}
