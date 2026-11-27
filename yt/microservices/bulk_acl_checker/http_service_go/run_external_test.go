//go:build !internal
// +build !internal

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestTokenCallerAuthenticator(t *testing.T) {
	var whoamiRequests int
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/auth/whoami", r.URL.Path)
		whoamiRequests++
		if r.Header.Get("Authorization") != "OAuth alice-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":900,"message":"Authentication failed"}`))
			return
		}
		_, _ = w.Write([]byte(`{"login":"alice"}`))
	}))
	defer proxy.Close()
	cluster := strings.TrimPrefix(proxy.URL, "http://")

	for _, tt := range []struct {
		name          string
		authorization string
		expected      string
		// Empty means that the caller is authenticated.
		errorContains   string
		expectedErrCode yterrors.ErrorCode
		expectedWhoami  int
	}{
		{
			name:           "valid token",
			authorization:  "OAuth alice-token",
			expected:       "alice",
			expectedWhoami: 1,
		},
		{
			name:            "missing token",
			authorization:   "",
			errorContains:   "an OAuth token is required",
			expectedErrCode: yterrors.CodeInvalidCredentials,
			expectedWhoami:  0,
		},
		{
			name:            "token of another scheme",
			authorization:   "Bearer alice-token",
			errorContains:   "an OAuth token is required",
			expectedErrCode: yterrors.CodeInvalidCredentials,
			expectedWhoami:  0,
		},
		{
			name:           "token rejected by the cluster",
			authorization:  "OAuth bob-token",
			errorContains:  "could not authenticate the caller",
			expectedWhoami: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			whoamiRequests = 0
			req := httptest.NewRequest(http.MethodGet, "/user-acl-tree/"+cluster, nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}

			caller, err := (&tokenCallerAuthenticator{}).AuthenticateCaller(req, cluster)

			if tt.errorContains == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expected, caller)
			} else {
				require.ErrorContains(t, err, tt.errorContains)
			}
			if tt.expectedErrCode != 0 {
				require.True(t, yterrors.ContainsErrorCode(err, tt.expectedErrCode), err)
			}
			require.Equal(t, tt.expectedWhoami, whoamiRequests)
		})
	}
}
