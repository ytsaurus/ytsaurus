package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerbHttpMethod(t *testing.T) {
	require.Equal(t, "GET", VerbSelectRows.HTTPMethod())
}

func TestVerbGetTableColumnarStatistics(t *testing.T) {
	// Read-only command routed to heavy proxies, like in the python client.
	require.Equal(t, "GET", VerbGetTableColumnarStatistics.HTTPMethod())
	require.True(t, VerbGetTableColumnarStatistics.IsHeavy())
}
