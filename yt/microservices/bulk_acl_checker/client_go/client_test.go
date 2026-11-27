package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yterrors"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
)

func TestGetUserACLTree(t *testing.T) {
	for _, tt := range []struct {
		name            string
		permission      string
		status          int
		body            string
		expectedQuery   url.Values
		expected        *bac_lib.UserACLTreeResponse
		errorContains   string
		expectedErrCode yterrors.ErrorCode
	}{
		{
			name:          "tree",
			permission:    "",
			status:        http.StatusOK,
			body:          `{"path":"//home/a\\/b","acl_snapshot_id":"1-2","acl_tree":{"access":{"object":true,"immediate_descendants":false,"other_descendants":false}}}`,
			expectedQuery: url.Values{"path": {`//home/a\/b`}, "username": {"alice"}},
			expected: &bac_lib.UserACLTreeResponse{
				Path:          `//home/a\/b`,
				ACLSnapshotID: "1-2",
				ACLTree:       &bac_lib.ACLTree{Access: &bac_lib.NodeAccess{Object: true, ImmediateDescendants: false, OtherDescendants: false}},
			},
		},
		{
			name:          "tree for write permission",
			permission:    "write",
			status:        http.StatusOK,
			body:          `{"path":"//home/a\\/b","acl_snapshot_id":"1-2","acl_tree":{"access":{"object":false,"immediate_descendants":false,"other_descendants":false}}}`,
			expectedQuery: url.Values{"path": {`//home/a\/b`}, "username": {"alice"}, "permission": {"write"}},
			expected: &bac_lib.UserACLTreeResponse{
				Path:          `//home/a\/b`,
				ACLSnapshotID: "1-2",
				ACLTree:       &bac_lib.ACLTree{Access: &bac_lib.NodeAccess{Object: false, ImmediateDescendants: false, OtherDescendants: false}},
			},
		},
		{
			name:            "YT error",
			permission:      "",
			status:          http.StatusBadRequest,
			body:            `{"code":901,"message":"The caller has no use permission on the get_acl_tree access control object"}`,
			expectedQuery:   url.Values{"path": {`//home/a\/b`}, "username": {"alice"}},
			errorContains:   "has no use permission on the get_acl_tree access control object",
			expectedErrCode: yterrors.CodeAuthorizationError,
		},
		{
			name:          "response that is not a YT error",
			permission:    "",
			status:        http.StatusBadGateway,
			body:          "Bad Gateway",
			expectedQuery: url.Values{"path": {`//home/a\/b`}, "username": {"alice"}},
			errorContains: "unexpected response of Bulk ACL Checker with HTTP status 502: Bad Gateway",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/user-acl-tree/ytserver", r.URL.Path)
				assert.Equal(t, tt.expectedQuery, r.URL.Query())
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			client := &Client{c: &Config{BaseURL: server.URL}}

			response, err := client.GetUserACLTree(context.Background(), &UserACLTreeRequest{
				Cluster:    "ytserver",
				Path:       `//home/a\/b`,
				Username:   "alice",
				Permission: tt.permission,
			})

			if tt.errorContains == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expected, response)
				return
			}
			require.ErrorContains(t, err, tt.errorContains)
			if tt.expectedErrCode != 0 {
				require.True(t, yterrors.ContainsErrorCode(err, tt.expectedErrCode), err)
			}
		})
	}
}
