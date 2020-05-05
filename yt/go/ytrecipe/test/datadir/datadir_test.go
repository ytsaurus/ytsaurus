package datadir

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataDir(t *testing.T) {
	st, err := os.Stat("layers")
	require.NoError(t, err)
	require.True(t, st.IsDir())
}
