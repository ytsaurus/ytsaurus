package secret

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetYTToken(t *testing.T) {
	if os.Getenv("AUTOCHECK") == "" {
		t.Skip("must be run inside distbuild")
	}

	token, err := GetYTToken()
	require.NoError(t, err)
	require.NotEmpty(t, token)
}
