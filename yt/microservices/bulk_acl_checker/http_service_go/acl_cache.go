package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/spf13/cobra"

	"go.ytsaurus.tech/library/go/core/log"
	yaslices "go.ytsaurus.tech/library/go/slices"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

type ACLDumpPart struct {
	Idx  int64  `yson:"idx"`
	Part []byte `yson:"part"`
}

type Groups map[string]struct{}
type UserExportRow struct {
	Name            string   `yson:"name"`
	MemberOfClosure []string `yson:"member_of_closure"`
}

type ClusterACLDump struct {
	ACLDump     *ACLDump
	UsersExport map[string]Groups
	YtClient    yt.Client
	Version     ypath.Path
	SnapshotID  string
}

type LRUCacheKey struct {
	Version    ypath.Path
	Subject    string
	Permission yt.Permission
	ACLHash    string
}

type BannedCacheKey struct {
	Cluster string
	Subject string
}

type ACOCacheKey struct {
	Cluster   string
	Caller    string
	Principal ypath.Path
}

type ACLCache struct {
	IsInitialized atomic.Bool
	KnownClusters map[string]struct{}
	Clusters      map[string]*ClusterACLDump
	Mutex         sync.Mutex
	ACLLRU        *lru.LRU[LRUCacheKey, yt.SecurityAction]
	BannedLRU     *lru.LRU[BannedCacheKey, bool]
	ACOLRU        *lru.LRU[ACOCacheKey, bool]
}

// Set stores nothing once ctx is canceled: perClusterRunner cancels the loader of a cluster before it removes the cluster,
// so a load that finishes after the removal does not restore the cluster.
func (cache *ACLCache) Set(ctx context.Context, cluster string, cacheItem *ClusterACLDump) {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()
	if ctx.Err() == nil {
		cache.Clusters[cluster] = cacheItem
	}
}

func (cache *ACLCache) Remove(cluster string) {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()
	delete(Cache.Clusters, cluster)
}

func (cache *ACLCache) Get(cluster string) *ClusterACLDump {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()
	result := cache.Clusters[cluster]
	return result
}

const (
	lruCacheSize          = 100 * 1024
	defaultACLCacheTTL    = 10 * time.Minute
	defaultBannedCacheTTL = 10 * time.Minute
	defaultACOCacheTTL    = time.Hour
)

func InitCache(aclCacheTTL, bannedCacheTTL, acoCacheTTL time.Duration) *ACLCache {
	aclLRU := lru.NewLRU[LRUCacheKey, yt.SecurityAction](lruCacheSize, nil, aclCacheTTL)
	bannedLRU := lru.NewLRU[BannedCacheKey, bool](lruCacheSize, nil, bannedCacheTTL)
	acoLRU := lru.NewLRU[ACOCacheKey, bool](lruCacheSize, nil, acoCacheTTL)
	result := ACLCache{
		Clusters:  make(map[string]*ClusterACLDump),
		ACLLRU:    aclLRU,
		BannedLRU: bannedLRU,
		ACOLRU:    acoLRU,
	}
	result.IsInitialized.Store(false)
	return &result
}

var Cache *ACLCache

func readACLDumpTable(ctx context.Context, ytClient yt.Client, path ypath.YPath) (result []byte, err error) {
	reader, err := ytClient.ReadTable(ctx, path, nil)
	if err != nil {
		return
	}
	defer reader.Close()
	var parts []ACLDumpPart
	for reader.Next() {
		var x ACLDumpPart
		if err = reader.Scan(&x); err != nil {
			return
		}
		parts = append(parts, x)
	}
	if err = reader.Err(); err != nil {
		return
	}
	slices.SortFunc(parts, func(a, b ACLDumpPart) int {
		return cmp.Compare(a.Idx, b.Idx)
	})
	for _, part := range parts {
		result = append(result, part.Part...)
	}
	if err = reader.Close(); err != nil {
		return
	}
	return
}

func readUserExport(ctx context.Context, ytClient yt.Client, path ypath.Path) (result map[string]Groups, err error) {
	reader, err := ytClient.ReadTable(ctx, path, nil)
	if err != nil {
		return
	}
	defer reader.Close()
	result = make(map[string]Groups)
	for reader.Next() {
		var x UserExportRow
		if err = reader.Scan(&x); err != nil {
			return
		}
		groups := make(Groups)
		groups[x.Name] = struct{}{}
		for _, g := range x.MemberOfClosure {
			groups[g] = struct{}{}
		}
		result[x.Name] = groups
	}
	if err = reader.Err(); err != nil {
		return
	}
	if err = reader.Close(); err != nil {
		return
	}
	return
}

func loadFromClusterIteration(ctx context.Context, sem chan struct{}, cluster string, tokenEnvVariable string, aclDumpPath ypath.Path, userExports ypath.Path) error {
	sem <- struct{}{}
	defer func() { <-sem }()
	ytClient := ytmsvc.MustNewYTClient(cluster, tokenEnvVariable)
	var tables []string
	if err := ytClient.ListNode(ctx, aclDumpPath, &tables, nil); err != nil {
		return err
	}
	tables = yaslices.Filter(tables, func(e string) bool {
		return strings.HasSuffix(e, "effective_acl_skeleton")
	})
	slices.SortFunc(tables, func(a, b string) int {
		aInt, err := strconv.Atoi(strings.Split(a, ":")[0])
		if err != nil {
			return 1
		}
		bInt, err := strconv.Atoi(strings.Split(b, ":")[0])
		if err != nil {
			return -1
		}
		return -cmp.Compare(aInt, bInt)
	})
	if len(tables) == 0 {
		return fmt.Errorf("no dumps for cluster %s found", cluster)
	}
	lastACLDump := ypath.Path(aclDumpPath.String() + "/" + tables[0])
	_, snapshotName, found := strings.Cut(tables[0], ":")
	if !found {
		return fmt.Errorf("ACL dump %s does not match <timestamp>:<snapshot_id>.effective_acl_skeleton", lastACLDump)
	}
	snapshotID := strings.TrimSuffix(snapshotName, ".effective_acl_skeleton")
	userExportTable := ypath.Path(fmt.Sprintf("%s/%s_user_export", userExports.String(), snapshotID))
	current := Cache.Get(cluster)
	var cacheItem ClusterACLDump
	if current != nil && current.Version == lastACLDump {
		// A missing user export is read again so that the snapshot can serve ACL trees once the export is readable.
		if current.UsersExport != nil {
			return nil
		}
		cacheItem = *current
	} else {
		data, err := readACLDumpTable(ctx, ytClient, lastACLDump)
		if err != nil {
			return err
		}
		var unmarshalled any
		if err = yson.Unmarshal(data, &unmarshalled); err != nil {
			return err
		}
		aclDump, err := DumpToACLDump(unmarshalled)
		if err != nil {
			return err
		}
		cacheItem = ClusterACLDump{ACLDump: aclDump, YtClient: ytClient, Version: lastACLDump, SnapshotID: snapshotID}
	}
	usersExport, err := readUserExport(ctx, ytClient, userExportTable)
	if err != nil {
		logger.Error("Failed to read the user export", log.String("cluster", cluster), log.String("path", userExportTable.String()), log.Error(err))
	} else {
		cacheItem.UsersExport = usersExport
	}
	Cache.Set(ctx, cluster, &cacheItem)
	return nil
}

func DumpToACLDump(data any) (*ACLDump, error) {
	result := new(ACLDump)
	list := data.([]any)
	if list[0] != nil {
		pathMap := list[0].(map[string]any)
		result.Paths = make(ACLDumpMap)
		for path, subdata := range pathMap {
			child, err := DumpToACLDump(subdata)
			if err != nil {
				return nil, err
			}
			result.Paths[path] = child
		}
	}
	readACL, err := dumpToCompressedACL(list[1])
	if err != nil {
		return nil, err
	}
	result.ReadACL = readACL
	if len(list) > 2 {
		writeACL, err := dumpToCompressedACL(list[2])
		if err != nil {
			return nil, err
		}
		result.WriteACL = writeACL
	}
	return result, nil
}

func dumpToCompressedACL(data any) (CompressedACL, error) {
	if data == nil {
		return nil, nil
	}
	result := make(CompressedACL)
	for indexStr, anySubjects := range data.(map[string]any) {
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return nil, err
		}
		for _, subject := range anySubjects.([]any) {
			result[index] = append(result[index], subject.(string))
		}
	}
	return result, nil
}

func loadFromClusterLoop(ctx context.Context, sem chan struct{}, cluster string, tokenEnvVariable string, aclDumpPath ypath.Path, userExportsPath ypath.Path, delay time.Duration) {
	timer := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			currentCacheItem := Cache.Get(cluster)
			if err := loadFromClusterIteration(ctx, sem, cluster, tokenEnvVariable, aclDumpPath, userExportsPath); err != nil {
				logger.Errorf("cluster %s: %s", cluster, err.Error())
				if currentCacheItem == nil {
					Cache.Set(ctx, cluster, nil)
				}
			}
			timer.Reset(delay)
		}
	}
}

func perClusterRunner(ctx context.Context, ytClient yt.Client, cmd *cobra.Command) {
	concurrencyLevel := ytmsvc.Must(cmd.Flags().GetUint16("concurrency"))
	includeArr := ytmsvc.Must(cmd.Flags().GetStringSlice("include"))
	excludeArr := ytmsvc.Must(cmd.Flags().GetStringSlice("exclude"))
	useClusterDirectory := ytmsvc.Must(cmd.Flags().GetString("proxy")) != ""
	aclDumpPathStr := ytmsvc.Must(cmd.Flags().GetString("snapshot-root"))
	aclDumpPath := ypath.Path(aclDumpPathStr)
	userExportsPathStr := ytmsvc.Must(cmd.Flags().GetString("user-root"))
	userExportsPath := ypath.Path(userExportsPathStr)
	tokenEnvVariable := ytmsvc.Must(cmd.Flags().GetString("token-env-variable"))
	delay := ytmsvc.Must(cmd.Flags().GetDuration("cache-update-period"))
	sem := make(chan struct{}, concurrencyLevel)
	runningClusters := make(map[string]context.CancelCauseFunc)
	timer := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if clusters := ytmsvc.GetClusters(ctx, ytClient, includeArr, excludeArr, useClusterDirectory); clusters != nil {
				for cluster, cancelFunc := range runningClusters {
					_, ok := clusters[cluster]
					if !ok {
						cancelFunc(fmt.Errorf("cluster %s not available now", cluster))
						delete(runningClusters, cluster)
						Cache.Remove(cluster)
					}
				}
				for cluster := range clusters {
					_, exists := runningClusters[cluster]
					if !exists {
						clusterCtx, cancel := context.WithCancelCause(ctx)
						runningClusters[cluster] = cancel
						go loadFromClusterLoop(clusterCtx, sem, cluster, tokenEnvVariable, aclDumpPath, userExportsPath, delay)
					}
				}
				if Cache.IsInitialized.Load() {
					Cache.Mutex.Lock()
					Cache.KnownClusters = clusters
					Cache.Mutex.Unlock()
				} else {
					Cache.Mutex.Lock()
					Cache.KnownClusters = clusters
					Cache.IsInitialized.Store(len(Cache.Clusters) == len(clusters))
					Cache.Mutex.Unlock()
				}
			}
			timer.Reset(delay)
		}
	}
}
