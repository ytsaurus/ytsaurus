package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc/ytmock"
)

var (
	allowedEverywhere = &bac_lib.NodeAccess{Object: true, ImmediateDescendants: true, OtherDescendants: true}
	deniedEverywhere  = &bac_lib.NodeAccess{Object: false, ImmediateDescendants: false, OtherDescendants: false}
)

type stubCallerAuthenticator struct {
	caller string
	err    error
}

func (a *stubCallerAuthenticator) AuthenticateCaller(_ *http.Request, _ string) (string, error) {
	return a.caller, a.err
}

// newACLTreeTestDump returns a dump with ACLs for read and write. A dump keeps the effective ACL only of the nodes
// that have their own ACEs or do not inherit the ACL of the parent.
func newACLTreeTestDump() *ACLDump {
	return &ACLDump{ReadACL: CompressedACL{}, WriteACL: CompressedACL{}, Paths: ACLDumpMap{"": {
		ReadACL:  CompressedACL{1: {"users"}},
		WriteACL: CompressedACL{},
		Paths: ACLDumpMap{
			"home": {Paths: ACLDumpMap{
				"a": {
					ReadACL:  CompressedACL{1: {"alice"}},
					WriteACL: CompressedACL{1: {"alice"}},
					Paths: ACLDumpMap{
						"denied": {ReadACL: CompressedACL{}, Paths: ACLDumpMap{
							"reallowed": {ReadACL: CompressedACL{1: {"users"}}},
						}},
						"private": {ReadACL: CompressedACL{1: {"alice"}, 4: {"alice"}}},
					},
				},
				"b": {ReadACL: CompressedACL{3: {"alice"}}, Paths: ACLDumpMap{"c": {}}},
			}},
			"tmp": {ReadACL: CompressedACL{5: {"users"}}},
		},
	}}}
}

func setupUserACLTreeTest(t *testing.T, usersExport map[string]Groups, objects ...ytmock.Object) (*ytmock.Client, *ytmock.Config) {
	t.Helper()
	setupACLTest(t)
	config := &ytmock.Config{
		Objects: objects,
		ObjectPermissions: []ytmock.ObjectPermission{
			{User: "caller", Permission: yt.PermissionUse, Path: aclTreePrincipalPath, Action: yt.ActionAllow},
		},
	}
	client := newACLTestClientWithMasterCacheChecks(t, config)
	Cache.Set(context.Background(), "ytserver", &ClusterACLDump{
		ACLDump:     newACLTreeTestDump(),
		UsersExport: usersExport,
		YtClient:    client,
		Version:     "//sys/admin/yt-microservices/bulk_acl_checker/1:000155754.snapshot_0c8b0cb7.effective_acl_skeleton",
		SnapshotID:  "000155754.snapshot_0c8b0cb7",
	})
	return client, config
}

var aliceUsersExport = map[string]Groups{"alice": {"alice": {}, "users": {}}}

var aliceNotBanned = ytmock.Object{Path: "//sys/users/alice/@banned", Value: false}

func requestUserACLTree(t *testing.T, authenticator CallerAuthenticator, target string) *httptest.ResponseRecorder {
	t.Helper()
	router := chi.NewRouter()
	router.Get("/user-acl-tree/{cluster}", ytmsvc.FormatResponse(createUserACLTreeHandler(authenticator)))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
	return response
}

func requireTreeResponse(t *testing.T, response *httptest.ResponseRecorder, expected *bac_lib.UserACLTreeResponse) {
	t.Helper()
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	var actual bac_lib.UserACLTreeResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &actual))
	require.Equal(t, *expected, actual)
}

func TestNodeAccess(t *testing.T) {
	alice := Groups{"alice": {}, "users": {}}
	for _, tt := range []struct {
		name     string
		acl      CompressedACL
		expected bac_lib.NodeAccess
	}{
		{name: "object_only", acl: CompressedACL{0: {"alice"}}, expected: bac_lib.NodeAccess{Object: true, ImmediateDescendants: false, OtherDescendants: false}},
		{name: "object_and_descendants", acl: CompressedACL{1: {"alice"}}, expected: bac_lib.NodeAccess{Object: true, ImmediateDescendants: true, OtherDescendants: true}},
		{name: "descendants_only", acl: CompressedACL{2: {"alice"}}, expected: bac_lib.NodeAccess{Object: false, ImmediateDescendants: true, OtherDescendants: true}},
		{name: "immediate_descendants_only", acl: CompressedACL{3: {"alice"}}, expected: bac_lib.NodeAccess{Object: false, ImmediateDescendants: true, OtherDescendants: false}},
		{name: "deny of the user overrides allow of a group", acl: CompressedACL{1: {"users"}, 7: {"alice"}}, expected: bac_lib.NodeAccess{Object: true, ImmediateDescendants: false, OtherDescendants: true}},
		{name: "ACE of another subject does not apply", acl: CompressedACL{1: {"bob"}}, expected: bac_lib.NodeAccess{Object: false, ImmediateDescendants: false, OtherDescendants: false}},
		{name: "allow for owner grants nothing", acl: CompressedACL{1: {"owner"}}, expected: bac_lib.NodeAccess{Object: false, ImmediateDescendants: false, OtherDescendants: false}},
		{name: "deny for owner applies to any user", acl: CompressedACL{1: {"alice"}, 4: {"owner"}}, expected: bac_lib.NodeAccess{Object: false, ImmediateDescendants: true, OtherDescendants: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, *nodeAccess(&ACLDump{ReadACL: tt.acl}, yt.PermissionRead, alice))
		})
	}
}

func TestBuildACLTree(t *testing.T) {
	alice := Groups{"alice": {}, "users": {}}
	root := newACLTreeTestDump().Paths[""]
	nodeA := &bac_lib.ACLTree{Access: allowedEverywhere, Children: map[string]*bac_lib.ACLTree{
		"denied": {Access: deniedEverywhere, Children: map[string]*bac_lib.ACLTree{
			"reallowed": {Access: allowedEverywhere},
		}},
		"private": {Access: &bac_lib.NodeAccess{Object: false, ImmediateDescendants: true, OtherDescendants: true}},
	}}
	nodeB := &bac_lib.ACLTree{
		Access:   &bac_lib.NodeAccess{Object: false, ImmediateDescendants: true, OtherDescendants: false},
		Children: map[string]*bac_lib.ACLTree{"c": {}},
	}
	for _, tt := range []struct {
		name       string
		components []string
		permission yt.Permission
		expected   *bac_lib.ACLTree
	}{
		{
			name:       "requested subtree with the branch of its ancestors only",
			components: []string{"home", "a"},
			permission: yt.PermissionRead,
			expected: &bac_lib.ACLTree{Access: allowedEverywhere, Children: map[string]*bac_lib.ACLTree{
				"home": {Children: map[string]*bac_lib.ACLTree{"a": nodeA}},
			}},
		},
		{
			name:       "ancestor ACL keeps its position above the requested node",
			components: []string{"home", "b", "c"},
			permission: yt.PermissionRead,
			expected: &bac_lib.ACLTree{Access: allowedEverywhere, Children: map[string]*bac_lib.ACLTree{
				"home": {Children: map[string]*bac_lib.ACLTree{"b": nodeB}},
			}},
		},
		{
			name:       "missing requested node ends the branch at its nearest ancestor",
			components: []string{"home", "absent", "table"},
			permission: yt.PermissionRead,
			expected: &bac_lib.ACLTree{Access: allowedEverywhere, Children: map[string]*bac_lib.ACLTree{
				"home": {},
			}},
		},
		{
			name:       "Cypress root",
			components: nil,
			permission: yt.PermissionRead,
			expected: &bac_lib.ACLTree{Access: allowedEverywhere, Children: map[string]*bac_lib.ACLTree{
				"home": {Children: map[string]*bac_lib.ACLTree{"a": nodeA, "b": nodeB}},
				"tmp":  {Access: deniedEverywhere},
			}},
		},
		{
			name:       "write permission",
			components: []string{"home"},
			permission: yt.PermissionWrite,
			expected: &bac_lib.ACLTree{Access: deniedEverywhere, Children: map[string]*bac_lib.ACLTree{
				"home": {Children: map[string]*bac_lib.ACLTree{
					"a": {Access: allowedEverywhere, Children: map[string]*bac_lib.ACLTree{
						"denied":  {Children: map[string]*bac_lib.ACLTree{"reallowed": {}}},
						"private": {},
					}},
					"b": {Children: map[string]*bac_lib.ACLTree{"c": {}}},
				}},
			}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, buildACLTree(root, tt.permission, alice, tt.components))
		})
	}
}

func TestUserACLTree(t *testing.T) {
	client, _ := setupUserACLTreeTest(t, aliceUsersExport, aliceNotBanned)
	authenticator := &stubCallerAuthenticator{caller: "caller"}

	for _, target := range []string{
		"/user-acl-tree/ytserver?path=%2F%2Fhome%2Fb%2Fc&username=alice",
		"/user-acl-tree/ytserver?path=%2F%2Fhome%2Fb%2Fc&username=alice&permission=read",
	} {
		response := requestUserACLTree(t, authenticator, target)

		requireTreeResponse(t, response, &bac_lib.UserACLTreeResponse{
			Path:          "//home/b/c",
			ACLSnapshotID: "000155754.snapshot_0c8b0cb7",
			ACLTree: &bac_lib.ACLTree{Access: allowedEverywhere, Children: map[string]*bac_lib.ACLTree{
				"home": {Children: map[string]*bac_lib.ACLTree{"b": {
					Access:   &bac_lib.NodeAccess{Object: false, ImmediateDescendants: true, OtherDescendants: false},
					Children: map[string]*bac_lib.ACLTree{"c": {}},
				}}},
			}},
		})
	}
	// The second request takes the decision on the ACO from the cache.
	require.Len(t, client.CheckPermissionCalls(), 1)
}

func TestUserACLTreeForWritePermission(t *testing.T) {
	setupUserACLTreeTest(t, aliceUsersExport, aliceNotBanned)

	response := requestUserACLTree(t, &stubCallerAuthenticator{caller: "caller"}, "/user-acl-tree/ytserver?path=%2F%2Fhome%2Fa%2Fprivate&username=alice&permission=write")

	requireTreeResponse(t, response, &bac_lib.UserACLTreeResponse{
		Path:          "//home/a/private",
		ACLSnapshotID: "000155754.snapshot_0c8b0cb7",
		ACLTree: &bac_lib.ACLTree{Access: deniedEverywhere, Children: map[string]*bac_lib.ACLTree{
			"home": {Children: map[string]*bac_lib.ACLTree{"a": {
				Access:   allowedEverywhere,
				Children: map[string]*bac_lib.ACLTree{"private": {}},
			}}},
		}},
	})
}

func TestUserACLTreeForBannedUser(t *testing.T) {
	for _, tt := range []struct {
		name   string
		banned ytmock.Object
	}{
		{name: "banned user", banned: ytmock.Object{Path: "//sys/users/alice/@banned", Value: true}},
		{name: "user missing on the cluster", banned: ytmock.Object{Path: "//sys/users/alice/@banned", Error: &yterrors.Error{
			Code:    yterrors.CodeResolveError,
			Message: "No such user: alice",
		}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setupUserACLTreeTest(t, aliceUsersExport, tt.banned)

			response := requestUserACLTree(t, &stubCallerAuthenticator{caller: "caller"}, "/user-acl-tree/ytserver?path=%2F%2Fhome&username=alice")

			require.Equal(t, http.StatusBadRequest, response.Code)
			var ytError yterrors.Error
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &ytError))
			require.Equal(t, yterrors.CodeUserBanned, ytError.Code, response.Body.String())
			require.Equal(t, "The user is banned or no longer exists on the cluster", ytError.Message)
		})
	}
}

func TestUserACLTreeForSuperuser(t *testing.T) {
	setupUserACLTreeTest(t, map[string]Groups{"admin": {"admin": {}, "superusers": {}}}, ytmock.Object{Path: "//sys/users/admin/@banned", Value: false})

	response := requestUserACLTree(t, &stubCallerAuthenticator{caller: "caller"}, "/user-acl-tree/ytserver?path=%2F%2Ftmp&username=admin")

	requireTreeResponse(t, response, &bac_lib.UserACLTreeResponse{
		Path:          "//tmp",
		ACLSnapshotID: "000155754.snapshot_0c8b0cb7",
		ACLTree:       &bac_lib.ACLTree{Access: allowedEverywhere},
	})
}

func TestUserACLTreeACOPermission(t *testing.T) {
	for _, tt := range []struct {
		name      string
		acoAction yt.SecurityAction
		acoErr    error
		// Empty means that access is granted.
		errorContains   string
		expectedChecks  int
		expectedErrCode yterrors.ErrorCode
	}{
		{
			name:           "allow is cached",
			acoAction:      yt.ActionAllow,
			acoErr:         nil,
			errorContains:  "",
			expectedChecks: 1,
		},
		{
			name:            "deny is cached",
			acoAction:       yt.ActionDeny,
			acoErr:          nil,
			errorContains:   "has no use permission on the get_acl_tree access control object",
			expectedChecks:  1,
			expectedErrCode: yterrors.CodeAuthorizationError,
		},
		{
			name:            "missing ACO is an error and is not cached",
			acoErr:          yterrors.Err(yterrors.CodeResolveError, "Node bulk_acl_checker has no child get_acl_tree"),
			errorContains:   "Could not check the permission to get ACL trees",
			expectedChecks:  2,
			expectedErrCode: yterrors.CodeResolveError,
		},
		{
			name:           "failed check is not cached",
			acoErr:         errors.New("master is unavailable"),
			errorContains:  "Master is unavailable",
			expectedChecks: 2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, config := setupUserACLTreeTest(t, aliceUsersExport, aliceNotBanned)
			config.ObjectPermissions[0].Action, config.ObjectPermissions[0].Error = tt.acoAction, tt.acoErr

			for range 2 {
				response := requestUserACLTree(t, &stubCallerAuthenticator{caller: "caller"}, "/user-acl-tree/ytserver?path=%2F&username=alice")

				if tt.errorContains == "" {
					require.Equal(t, http.StatusOK, response.Code, response.Body.String())
					continue
				}
				require.Equal(t, http.StatusBadRequest, response.Code)
				require.Contains(t, response.Body.String(), tt.errorContains)
				if tt.expectedErrCode != 0 {
					var ytError yterrors.Error
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &ytError))
					require.True(t, yterrors.ContainsErrorCode(&ytError, tt.expectedErrCode), response.Body.String())
				}
			}
			require.Len(t, client.CheckPermissionCalls(), tt.expectedChecks)
		})
	}
}

func TestUserACLTreeRequestErrors(t *testing.T) {
	for _, tt := range []struct {
		name          string
		target        string
		authenticator CallerAuthenticator
		usersExport   map[string]Groups
		errorContains string
		// The ACO is not checked when the request fails before the authorization.
		expectedChecks int
	}{
		{
			name:          "path is not canonical",
			target:        "/user-acl-tree/ytserver?path=%2F%2Fhome%2F%40acl&username=alice",
			authenticator: &stubCallerAuthenticator{caller: "caller"},
			usersExport:   aliceUsersExport,
			errorContains: `must be / or an absolute Cypress path in the canonical YPath escaping`,
		},
		{
			name:          "username is missing",
			target:        "/user-acl-tree/ytserver?path=%2F",
			authenticator: &stubCallerAuthenticator{caller: "caller"},
			usersExport:   aliceUsersExport,
			errorContains: "Username is required",
		},
		{
			name:          "permission is unsupported",
			target:        "/user-acl-tree/ytserver?path=%2F&username=alice&permission=use",
			authenticator: &stubCallerAuthenticator{caller: "caller"},
			usersExport:   aliceUsersExport,
			errorContains: `Unsupported permission \"use\"`,
		},
		{
			name:          "cluster is unknown",
			target:        "/user-acl-tree/unknown?path=%2F&username=alice",
			authenticator: &stubCallerAuthenticator{err: errors.New("invalid credentials")},
			usersExport:   aliceUsersExport,
			errorContains: "Unknown cluster unknown",
		},
		{
			name:          "caller authentication fails",
			target:        "/user-acl-tree/ytserver?path=%2F&username=alice",
			authenticator: &stubCallerAuthenticator{err: errors.New("invalid credentials")},
			usersExport:   aliceUsersExport,
			errorContains: "Invalid credentials",
		},
		{
			name:           "user export is not loaded",
			target:         "/user-acl-tree/ytserver?path=%2F&username=alice",
			authenticator:  &stubCallerAuthenticator{caller: "caller"},
			usersExport:    nil,
			errorContains:  "The user export of ACL snapshot 000155754.snapshot_0c8b0cb7 of cluster ytserver is not loaded",
			expectedChecks: 1,
		},
		{
			name:           "user is missing from the snapshot",
			target:         "/user-acl-tree/ytserver?path=%2F&username=bob",
			authenticator:  &stubCallerAuthenticator{caller: "caller"},
			usersExport:    aliceUsersExport,
			errorContains:  `User \"bob\" is missing from ACL snapshot 000155754.snapshot_0c8b0cb7 of cluster ytserver`,
			expectedChecks: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := setupUserACLTreeTest(t, tt.usersExport)

			response := requestUserACLTree(t, tt.authenticator, tt.target)

			require.Equal(t, http.StatusBadRequest, response.Code)
			require.Contains(t, response.Body.String(), tt.errorContains)
			require.Len(t, client.CheckPermissionCalls(), tt.expectedChecks)
		})
	}
}

func TestUserACLTreeWithoutACLOfPermission(t *testing.T) {
	for _, tt := range []struct {
		permission yt.Permission
		dump       *ACLDump
	}{
		{permission: yt.PermissionRead, dump: &ACLDump{ReadACL: nil, WriteACL: CompressedACL{}}},
		{permission: yt.PermissionWrite, dump: &ACLDump{ReadACL: CompressedACL{}, WriteACL: nil}},
	} {
		t.Run(string(tt.permission), func(t *testing.T) {
			setupUserACLTreeTest(t, aliceUsersExport, aliceNotBanned)
			Cache.Get("ytserver").ACLDump = tt.dump

			response := requestUserACLTree(t, &stubCallerAuthenticator{caller: "caller"}, "/user-acl-tree/ytserver?path=%2F&username=alice&permission="+string(tt.permission))

			require.Equal(t, http.StatusBadRequest, response.Code)
			require.Contains(t, response.Body.String(), "dump for cluster ytserver does not contain "+string(tt.permission)+" permissions")
		})
	}
}
