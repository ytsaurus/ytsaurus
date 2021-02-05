package yterrors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASCII(t *testing.T) {
	require.Equal(t, "Привет", decodeNonASCII(encodeNonASCII("Привет")))
}
