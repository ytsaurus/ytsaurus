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
}

type LRUCacheKey struct {
	Version ypath.Path
	Subject string
	ACLHash string
}

type ACLCache struct {
	IsInitialized atomic.Bool
	KnownClusters map[string]struct{}
	Clusters      map[string]*ClusterACLDump
	Mutex         sync.Mutex
	LRU           *lru.LRU[LRUCacheKey, yt.SecurityAction]
}

func (cache *ACLCache) Set(cluster string, cacheItem *ClusterACLDump) {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()
	cache.Clusters[cluster] = cacheItem
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

func InitCache() *ACLCache {
	lru := lru.NewLRU[LRUCacheKey, yt.SecurityAction](100*1024, nil, time.Duration(10*time.Minute))
	result := ACLCache{
		Clusters: make(map[string]*ClusterACLDump),
		LRU:      lru,
	}
	result.IsInitialized.Store(false)
	return &result
}

var Cache *ACLCache = InitCache()

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
	if err = reader.Close(); err != nil {
		return
	}
	return
}

func loadFromClusterIteration(ctx context.Context, sem chan struct{}, cluster string, aclDumpPath ypath.Path, userExports ypath.Path, lastReadedTable ypath.Path) (err error) {
	sem <- struct{}{}
	defer func() { <-sem }()
	ytClient := ytmsvc.MustNewYTClient(cluster)
	var tables []string
	if err = ytClient.ListNode(ctx, aclDumpPath, &tables, nil); err != nil {
		return
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
		err = fmt.Errorf("no dumps for cluster %s found", cluster)
		return
	}
	lastACLDump := ypath.Path(aclDumpPath.String() + "/" + tables[0])
	if lastReadedTable == lastACLDump {
		return
	}
	data, err := readACLDumpTable(ctx, ytClient, lastACLDump)
	if err != nil {
		return
	}

	var unmarshalled any
	if err = yson.Unmarshal(data, &unmarshalled); err != nil {
		return
	}

	cacheItem := ClusterACLDump{}
	cacheItem.Version = lastACLDump
	cacheItem.YtClient = ytClient
	cacheItem.ACLDump, err = DumpToACLDump(unmarshalled)
	if err != nil {
		return
	}
	withoutTs := strings.Split(tables[0], ":")[1]
	userExportTable := ypath.Path(fmt.Sprintf("%s/%s_user_export", userExports.String(), strings.TrimSuffix(withoutTs, ".effective_acl_skeleton")))
	cacheItem.UsersExport, err = readUserExport(ctx, ytClient, userExportTable)
	if err != nil {
		cacheItem.UsersExport = nil
		err = nil
	}
	Cache.Set(cluster, &cacheItem)
	return
}

func DumpToACLDump(data any) (result *ACLDump, err error) {
	result = new(ACLDump)
	list := data.([]any)
	if list[0] != nil {
		pathMap := list[0].(map[string]any)
		result.Paths = make(ACLDumpMap)
		for path, subdata := range pathMap {
			result.Paths[path], err = DumpToACLDump(subdata)
			if err != nil {
				return
			}
		}
	}
	if list[1] != nil {
		aclMap := list[1].(map[string]any)
		result.ACL = make(CompressedACL)
		for indexStr, anySubjects := range aclMap {
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return result, err
			}
			for _, subject := range anySubjects.([]any) {
				result.ACL[index] = append(result.ACL[index], subject.(string))
			}
		}
	}
	return
}

func loadFromClusterLoop(ctx context.Context, sem chan struct{}, cluster string, aclDumpPath ypath.Path, userExportsPath ypath.Path) {
	timer := time.NewTimer(0)
	delay := time.Duration(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			var cacheVersion ypath.Path
			currentCacheItem := Cache.Get(cluster)
			if currentCacheItem != nil {
				cacheVersion = currentCacheItem.Version
			}
			if err := loadFromClusterIteration(ctx, sem, cluster, aclDumpPath, userExportsPath, cacheVersion); err != nil {
				logger.Errorf("cluster %s: %s", cluster, err.Error())
				if currentCacheItem == nil {
					Cache.Set(cluster, nil)
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
	sem := make(chan struct{}, concurrencyLevel)
	runningClusters := make(map[string]context.CancelCauseFunc)
	timer := time.NewTimer(0)
	delay := time.Duration(30 * time.Second)
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
						go loadFromClusterLoop(clusterCtx, sem, cluster, aclDumpPath, userExportsPath)
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
