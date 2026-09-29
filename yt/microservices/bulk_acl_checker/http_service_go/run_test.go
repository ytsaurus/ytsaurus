package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestDebugLoginRejectsAnotherSubject(t *testing.T) {
	previousLogger := logger
	logger = &zap.Logger{L: zaptest.NewLogger(t)}
	t.Cleanup(func() { logger = previousLogger })

	handler := createDebugRouter("alice")
	body := `{"cluster":"ytserver","subject":"bob","paths":["//home/a"]}`
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/check-acl", strings.NewReader(body))
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "cannot check subject")
}

func newACLTestRouter(t *testing.T, dump *ClusterACLDump) http.Handler {
	t.Helper()
	previousCache, previousLogger := Cache, logger
	Cache = InitCache()
	Cache.Set("ytserver", dump)
	logger = &zap.Logger{L: zaptest.NewLogger(t)}
	t.Cleanup(func() {
		Cache, logger = previousCache, previousLogger
	})
	return createDebugRouter("alice")
}

func checkTestACL(t *testing.T, handler http.Handler, permission string) *httptest.ResponseRecorder {
	t.Helper()
	body := map[string]any{
		"cluster": "ytserver",
		"subject": "alice",
		"paths":   []string{"//home/a", "//home/a"},
	}
	if permission != "" {
		body["permission"] = permission
	}
	data, err := json.Marshal(body)
	require.NoError(t, err)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/check-acl", strings.NewReader(string(data))))
	return response
}

func TestCheckACLPermissions(t *testing.T) {
	for _, superuser := range []bool{false, true} {
		groups := Groups{"alice": {}}
		if superuser {
			groups["superusers"] = struct{}{}
		}
		handler := newACLTestRouter(t, &ClusterACLDump{
			ACLDump: &ACLDump{
				ReadACL:  CompressedACL{1: {"alice"}},
				WriteACL: CompressedACL{1: {"alice"}, 5: {"alice"}},
			},
			UsersExport: map[string]Groups{"alice": groups},
		})
		for _, permission := range []string{"", "read", "write"} {
			expected := "allow"
			if permission == "write" && !superuser {
				expected = "deny"
			}
			response := checkTestACL(t, handler, permission)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.JSONEq(t, `{"actions":["`+expected+`","`+expected+`"]}`, response.Body.String())
		}
		response := checkTestACL(t, handler, "remove")
		require.Equal(t, http.StatusBadRequest, response.Code)
		require.Contains(t, response.Body.String(), "Unsupported permission")
	}
}

func TestCheckACLWithOldDump(t *testing.T) {
	var data any
	require.NoError(t, yson.Unmarshal([]byte(`[{};{"1"=[alice;];};]`), &data))
	dump, err := DumpToACLDump(data)
	require.NoError(t, err)
	handler := newACLTestRouter(t, &ClusterACLDump{
		ACLDump:     dump,
		UsersExport: map[string]Groups{"alice": {"alice": {}}},
	})
	response := checkTestACL(t, handler, "read")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, `{"actions":["allow","allow"]}`, response.Body.String())
	response = checkTestACL(t, handler, "write")
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "does not contain write permissions")
}

func TestWriteACLInheritance(t *testing.T) {
	var data any
	require.NoError(t, yson.Unmarshal([]byte(`[
		{""=[{"home"=[{};{};{"1"=[alice;];"2"=[bob;];};];};#;#;];};
		{};
		{};
	]`), &data))
	dump, err := DumpToACLDump(data)
	require.NoError(t, err)
	for _, path := range []string{"//home/child", "//home/child/grandchild"} {
		acl, err := getCompressedACL(dump, path, yt.PermissionWrite)
		require.NoError(t, err)
		require.ElementsMatch(t, Subjects{"alice", "bob"}, acl[1])
		readACL, err := getCompressedACL(dump, path, yt.PermissionRead)
		require.NoError(t, err)
		require.Empty(t, readACL)
	}
}

type permissionTestClient struct {
	yt.Client
	permissions []yt.Permission
}

func (c *permissionTestClient) CheckPermissionByACL(
	ctx context.Context,
	user string,
	permission yt.Permission,
	acl []yt.ACE,
	options *yt.CheckPermissionByACLOptions,
) (*yt.CheckPermissionResponse, error) {
	c.permissions = append(c.permissions, permission)
	for _, ace := range acl {
		if len(ace.Permissions) != 1 || ace.Permissions[0] != permission {
			panic("unexpected ACL permission")
		}
	}
	action := yt.ActionAllow
	if permission == yt.PermissionWrite {
		action = yt.ActionDeny
	}
	return &yt.CheckPermissionResponse{CheckPermissionResult: yt.CheckPermissionResult{Action: action}}, nil
}

func TestRemoteACLPermissionCache(t *testing.T) {
	acl := CompressedACL{
		1: {"bob", "alice"},
		2: {"carol"},
		5: {"eve", "dave"},
		6: {"frank"},
	}
	reorderedACL := CompressedACL{
		6: {"frank"},
		5: {"dave", "eve"},
		2: {"carol"},
		1: {"alice", "bob"},
	}
	hash := Hash(acl)
	for range 32 {
		require.Equal(t, hash, Hash(acl))
		require.Equal(t, hash, Hash(reorderedACL))
	}
	require.NotEqual(t, Hash(CompressedACL{1: {"alice"}}), Hash(CompressedACL{5: {"alice"}}))
	require.NotEqual(t, Hash(CompressedACL{1: {"alice"}}), Hash(CompressedACL{1: {"bob"}}))

	client := &permissionTestClient{}
	handler := newACLTestRouter(t, &ClusterACLDump{
		ACLDump: &ACLDump{
			ReadACL:  acl,
			WriteACL: reorderedACL,
		},
		YtClient: client,
	})
	for _, permission := range []string{"read", "write", "read", "write"} {
		expected := "allow"
		if permission == "write" {
			expected = "deny"
		}
		response := checkTestACL(t, handler, permission)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		require.JSONEq(t, `{"actions":["`+expected+`","`+expected+`"]}`, response.Body.String())
	}
	require.Equal(t, []yt.Permission{yt.PermissionRead, yt.PermissionWrite}, client.permissions)
	require.Equal(t, Subjects{"bob", "alice"}, acl[1])
	require.Equal(t, Subjects{"eve", "dave"}, acl[5])
}
