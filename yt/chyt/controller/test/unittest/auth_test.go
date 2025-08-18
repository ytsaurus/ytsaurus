package unittest

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/auth"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestAuthorizationHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "request", bytes.NewReader([]byte("")))
	require.NoError(t, err)

	credentials, err := auth.GetCredentialsFromHeader(req)
	require.Equal(t, credentials, nil)
	require.Error(t, err)

	req.Header.Set("Authorization", "Bad_prefix token")
	require.Equal(t, credentials, nil)
	require.Error(t, err)

	req.Header.Set("Authorization", "OAuth token")
	credentials, err = auth.GetCredentialsFromHeader(req)
	require.Equal(t, credentials, &yt.TokenCredentials{Token: "token"})
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer token")
	credentials, err = auth.GetCredentialsFromHeader(req)
	require.Equal(t, credentials, &yt.BearerCredentials{Token: "token"})
	require.NoError(t, err)
}
