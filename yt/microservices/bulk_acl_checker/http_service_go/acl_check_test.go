package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc/ytmock"
)

func newACLTestConfig() *ytmock.Config {
	return &ytmock.Config{
		Objects: []ytmock.Object{{Path: "//sys/users/alice/@banned", Value: false}},
	}
}

func setupACLTest(t *testing.T) {
	t.Helper()
	previousCache, previousLogger := Cache, logger
	cache := InitCache(defaultACLCacheTTL, defaultBannedCacheTTL)
	Cache = cache
	logger = &zap.Logger{L: zaptest.NewLogger(t)}
	t.Cleanup(func() {
		cache.ACLLRU.Purge()
		cache.BannedLRU.Purge()
		Cache, logger = previousCache, previousLogger
	})
}

func newACLTestClientWithMasterCacheChecks(t *testing.T, config *ytmock.Config) *ytmock.Client {
	t.Helper()
	client := ytmock.NewClient(t, config)
	t.Cleanup(func() {
		for _, call := range client.GetNodeCalls() {
			require.NotNil(t, call.Options)
			require.NotNil(t, call.Options.MasterReadOptions)
			require.Equal(t, yt.ReadFromCache, call.Options.ReadFrom)
		}
		for _, call := range client.CheckPermissionByACLCalls() {
			require.NotNil(t, call.Options)
			require.NotNil(t, call.Options.MasterReadOptions)
			require.Equal(t, yt.ReadFromCache, call.Options.ReadFrom)
		}
	})
	return client
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

func TestBulkCheckACLBannedOverridesAllow(t *testing.T) {
	for _, tc := range []struct {
		name        string
		usersExport map[string]Groups
		permissions []ytmock.Permission
	}{
		{
			name:        "local",
			usersExport: map[string]Groups{"alice": {"alice": {}}},
		},
		{
			name:        "superuser",
			usersExport: map[string]Groups{"alice": {"superusers": {}}},
		},
		{
			name: "remote",
			permissions: []ytmock.Permission{{
				User:       "alice",
				Permission: yt.PermissionRead,
				ACL: []yt.ACE{{
					Action:          yt.ActionAllow,
					Subjects:        []string{"alice"},
					Permissions:     []yt.Permission{yt.PermissionRead},
					InheritanceMode: yt.InheritanceModeObjectAndDescendants,
				}},
				Action: yt.ActionAllow,
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := newACLTestConfig()
			config.Objects[0].Value = true
			config.Permissions = tc.permissions
			setupACLTest(t)
			client := newACLTestClientWithMasterCacheChecks(t, config)
			Cache.Set("ytserver", &ClusterACLDump{
				ACLDump:     &ACLDump{ReadACL: CompressedACL{1: {"alice"}}},
				UsersExport: tc.usersExport,
				YtClient:    client,
			})
			req := bac_lib.CheckACLRequest{
				Cluster: "ytserver", Subject: "alice",
				Paths: []string{"//home/a", "//home/b", "//home/a"},
			}
			actions, err := BulkCheckACL(context.Background(), req, "test")
			require.NoError(t, err)
			require.Equal(t, []yt.SecurityAction{yt.ActionDeny, yt.ActionDeny, yt.ActionDeny}, actions)
			require.Empty(t, client.CheckPermissionByACLCalls())
			require.Zero(t, Cache.ACLLRU.Len())

			config.Objects[0].Value = false
			Cache.BannedLRU.Purge()
			actions, err = BulkCheckACL(context.Background(), req, "test")
			require.NoError(t, err)
			require.Equal(t, []yt.SecurityAction{yt.ActionAllow, yt.ActionAllow, yt.ActionAllow}, actions)
			require.Len(t, client.CheckPermissionByACLCalls(), len(tc.permissions))
			require.Equal(t, len(tc.permissions), Cache.ACLLRU.Len())

			config.Objects[0].Value = true
			Cache.BannedLRU.Purge()
			actions, err = BulkCheckACL(context.Background(), req, "test")
			require.NoError(t, err)
			require.Equal(t, []yt.SecurityAction{yt.ActionDeny, yt.ActionDeny, yt.ActionDeny}, actions)
			require.Len(t, client.CheckPermissionByACLCalls(), len(tc.permissions))
		})
	}
}

func TestUserBannedCachesSuccessfulReads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		banned bool
	}{
		{name: "unbanned", banned: false},
		{name: "banned", banned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := newACLTestConfig()
			config.Objects[0].Value = tc.banned
			setupACLTest(t)
			client := newACLTestClientWithMasterCacheChecks(t, config)
			banned, err := getUserBanned(context.Background(), "ytserver", "alice", client)
			require.NoError(t, err)
			require.Equal(t, tc.banned, banned)
			config.Objects[0].Value = !tc.banned
			banned, err = getUserBanned(context.Background(), "ytserver", "alice", client)
			require.NoError(t, err)
			require.Equal(t, tc.banned, banned)
			require.Len(t, client.GetNodeCalls(), 1)
		})
	}
}

func TestUserBannedCachesMissingUser(t *testing.T) {
	config := newACLTestConfig()
	config.Objects[0].Error = &yterrors.Error{
		Code:    yterrors.CodeResolveError,
		Message: "No such user: alice",
	}
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, config)
	banned, err := getUserBanned(context.Background(), "ytserver", "alice", client)
	require.NoError(t, err)
	require.True(t, banned)
	config.Objects[0].Error = nil
	banned, err = getUserBanned(context.Background(), "ytserver", "alice", client)
	require.NoError(t, err)
	require.True(t, banned)
	require.Len(t, client.GetNodeCalls(), 1)
}

func TestUserBannedCacheSeparatesClustersAndSubjects(t *testing.T) {
	setupACLTest(t)
	clients := map[string]*ytmock.Client{
		"ytserver-a": newACLTestClientWithMasterCacheChecks(t, &ytmock.Config{
			Objects: []ytmock.Object{
				{Path: "//sys/users/alice/@banned", Value: false},
				{Path: "//sys/users/bob/@banned", Value: true},
			},
		}),
		"ytserver-b": newACLTestClientWithMasterCacheChecks(t, &ytmock.Config{
			Objects: []ytmock.Object{{Path: "//sys/users/alice/@banned", Value: true}},
		}),
	}
	for range 2 {
		for _, tc := range []struct {
			cluster string
			subject string
			banned  bool
		}{
			{cluster: "ytserver-a", subject: "alice", banned: false},
			{cluster: "ytserver-b", subject: "alice", banned: true},
			{cluster: "ytserver-a", subject: "bob", banned: true},
		} {
			banned, err := getUserBanned(context.Background(), tc.cluster, tc.subject, clients[tc.cluster])
			require.NoError(t, err)
			require.Equal(t, tc.banned, banned, "cluster=%s subject=%s", tc.cluster, tc.subject)
		}
	}
	require.Len(t, clients["ytserver-a"].GetNodeCalls(), 2)
	require.Len(t, clients["ytserver-b"].GetNodeCalls(), 1)
}

func TestUserBannedDoesNotCacheErrors(t *testing.T) {
	backendError := errors.New("master cache unavailable")
	config := newACLTestConfig()
	config.Objects[0].Error = backendError
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, config)
	_, err := getUserBanned(context.Background(), "ytserver", "alice", client)
	require.ErrorIs(t, err, backendError)
	require.Zero(t, Cache.BannedLRU.Len())
	config.Objects[0].Error = nil
	config.Objects[0].Value = true
	banned, err := getUserBanned(context.Background(), "ytserver", "alice", client)
	require.NoError(t, err)
	require.True(t, banned)
	require.Len(t, client.GetNodeCalls(), 2)
}

func TestUserBannedDoesNotCacheCancellation(t *testing.T) {
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, &ytmock.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := getUserBanned(ctx, "ytserver", "alice", client)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, Cache.BannedLRU.Len())
}

func TestUserBannedCacheExpires(t *testing.T) {
	config := newACLTestConfig()
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, config)
	Cache.BannedLRU = lru.NewLRU[BannedCacheKey, bool](10, nil, 20*time.Millisecond)
	_, err := getUserBanned(context.Background(), "ytserver", "alice", client)
	require.NoError(t, err)
	config.Objects[0].Value = true
	key := BannedCacheKey{Cluster: "ytserver", Subject: "alice"}
	require.Eventually(t, func() bool {
		_, ok := Cache.BannedLRU.Get(key)
		return !ok
	}, time.Second, 5*time.Millisecond)
	banned, err := getUserBanned(context.Background(), "ytserver", "alice", client)
	require.NoError(t, err)
	require.True(t, banned)
	require.Len(t, client.GetNodeCalls(), 2)
}

func TestDropCacheClearsBannedAndACL(t *testing.T) {
	config := newACLTestConfig()
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, config)
	_, err := getUserBanned(context.Background(), "ytserver", "alice", client)
	require.NoError(t, err)
	Cache.ACLLRU.Add(LRUCacheKey{Subject: "alice"}, yt.ActionAllow)
	_, err = GetDropCacheHandler()(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/drop-cache", nil))
	require.NoError(t, err)
	require.Zero(t, Cache.ACLLRU.Len())
	require.Zero(t, Cache.BannedLRU.Len())
	config.Objects[0].Value = true
	banned, err := getUserBanned(context.Background(), "ytserver", "alice", client)
	require.NoError(t, err)
	require.True(t, banned)
	require.Len(t, client.GetNodeCalls(), 2)
}

func TestUserBannedRejectsMissingClient(t *testing.T) {
	setupACLTest(t)
	_, err := getUserBanned(context.Background(), "ytserver", "alice", nil)
	require.Error(t, err)
	require.Zero(t, Cache.BannedLRU.Len())
}

func TestBulkCheckACLRejectsEmptySubject(t *testing.T) {
	setupACLTest(t)
	client := newACLTestClientWithMasterCacheChecks(t, &ytmock.Config{})
	Cache.Set("ytserver", &ClusterACLDump{ACLDump: &ACLDump{}, YtClient: client})
	req := bac_lib.CheckACLRequest{Cluster: "ytserver", Paths: []string{"//home/a"}}
	_, err := BulkCheckACL(context.Background(), req, "test")
	require.ErrorContains(t, err, "subject cannot be empty")
	require.Empty(t, client.GetNodeCalls())
}
