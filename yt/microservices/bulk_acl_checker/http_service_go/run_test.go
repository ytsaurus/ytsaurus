package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc/ytmock"
)

func TestDebugLoginRejectsAnotherSubject(t *testing.T) {
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, &ytmock.Config{})
	Cache.Set("ytserver", &ClusterACLDump{ACLDump: &ACLDump{}, YtClient: client})
	handler := createDebugRouter("alice")
	body := `{"cluster":"ytserver","subject":"bob","paths":["//home/a"]}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/check-acl", strings.NewReader(body)))
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "cannot check subject")
	require.Empty(t, client.GetNodeCalls())
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
		setupACLTest(t)
		client := newACLTestClientWithMasterCacheChecks(t, newACLTestConfig())
		Cache.Set("ytserver", &ClusterACLDump{
			ACLDump: &ACLDump{
				ReadACL:  CompressedACL{1: {"alice"}},
				WriteACL: CompressedACL{1: {"alice"}, 5: {"alice"}},
			},
			UsersExport: map[string]Groups{"alice": groups},
			YtClient:    client,
		})
		handler := createDebugRouter("alice")
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
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, newACLTestConfig())
	Cache.Set("ytserver", &ClusterACLDump{
		ACLDump:     dump,
		UsersExport: map[string]Groups{"alice": {"alice": {}}},
		YtClient:    client,
	})
	handler := createDebugRouter("alice")
	response := checkTestACL(t, handler, "read")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, `{"actions":["allow","allow"]}`, response.Body.String())
	response = checkTestACL(t, handler, "write")
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "does not contain write permissions")
}

func TestCheckACLHandlerReturnsErrorActionsOnBannedReadError(t *testing.T) {
	config := newACLTestConfig()
	config.Objects[0].Error = errors.New("master cache unavailable")
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, config)
	Cache.Set("ytserver", &ClusterACLDump{
		ACLDump:     &ACLDump{ReadACL: CompressedACL{1: {"alice"}}},
		UsersExport: map[string]Groups{"alice": {"alice": {}}},
		YtClient:    client,
	})
	handler := createDebugRouter("alice")
	response := checkTestACL(t, handler, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, `{"actions":["error","error"]}`, response.Body.String())
	config.Objects[0].Error = nil
	response = checkTestACL(t, handler, "")
	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"actions":["allow","allow"]}`, response.Body.String())
	require.Len(t, client.GetNodeCalls(), 2)
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

	config := newACLTestConfig()
	for _, tc := range []struct {
		permission yt.Permission
		action     yt.SecurityAction
	}{
		{yt.PermissionRead, yt.ActionAllow},
		{yt.PermissionWrite, yt.ActionDeny},
	} {
		config.Permissions = append(config.Permissions, ytmock.Permission{
			User:       "alice",
			Permission: tc.permission,
			ACL: []yt.ACE{
				{
					Action:          yt.ActionAllow,
					Subjects:        []string{"alice", "bob", "carol"},
					Permissions:     []yt.Permission{tc.permission},
					InheritanceMode: yt.InheritanceModeObjectAndDescendants,
				},
				{
					Action:          yt.ActionDeny,
					Subjects:        []string{"dave", "eve", "frank"},
					Permissions:     []yt.Permission{tc.permission},
					InheritanceMode: yt.InheritanceModeObjectAndDescendants,
				},
			},
			Action: tc.action,
		})
	}
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, config)
	Cache.Set("ytserver", &ClusterACLDump{
		ACLDump: &ACLDump{
			ReadACL:  acl,
			WriteACL: reorderedACL,
		},
		YtClient: client,
	})
	handler := createDebugRouter("alice")
	for range 2 {
		for _, configured := range config.Permissions {
			response := checkTestACL(t, handler, configured.Permission)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			expected := string(configured.Action)
			require.JSONEq(t, `{"actions":["`+expected+`","`+expected+`"]}`, response.Body.String())
		}
	}
	require.Len(t, client.CheckPermissionByACLCalls(), 2)
	require.Equal(t, Subjects{"bob", "alice"}, acl[1])
	require.Equal(t, Subjects{"eve", "dave"}, acl[5])
}
