package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStopGroup(t *testing.T) {
	g := NewStopGroup()

	select {
	case <-g.C():
		require.Fail(t, "group is not stopped")
	default:
	}

	require.True(t, g.TryAdd())
	g.Done()

	g.Stop()
	select {
	case <-g.C():
	default:
		require.Fail(t, "group stopped")
	}
}
