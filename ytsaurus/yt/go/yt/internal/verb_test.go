package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerbHttpMethod(t *testing.T) {
	require.Equal(t, "GET", VerbSelectRows.HTTPMethod())
}
