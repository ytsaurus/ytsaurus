package datadir

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataDir(t *testing.T) {
	_, err := os.Stat("layers/static-bin.tar.gz")
	require.NoError(t, err)

	_, err = os.Stat("layers/static_cat")
	require.NoError(t, err)
}
