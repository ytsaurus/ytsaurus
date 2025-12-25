package resourceusage

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

const (
	roundingPolicyClosest  = "closest"
	roundingPolicyBackward = "backward"
	roundingPolicyForward  = "forward"
)

func (cluster *Cluster) GetListTimestamps(minTimestamp int64, maxTimestamp int64) ([]int64, error) {
	if maxTimestamp == 0 {
		maxTimestamp = time.Now().Unix()
	}
	timestamps := []int64{}
	for _, resourceUsageTable := range cluster.ResourceUsageTables {
		timestamp := resourceUsageTable.Timestamp
		if minTimestamp <= timestamp && timestamp <= maxTimestamp {
			timestamps = append(timestamps, timestamp)
		}
	}
	if len(timestamps) == 0 {
		return nil, fmt.Errorf("no timestamps found in the range [%d, %d]", minTimestamp, maxTimestamp)
	}
	return timestamps, nil
}

func (cluster *Cluster) GetVersionedResourceUsage(ctx context.Context, timestamp int64, timestampRoundingPolicy string, account string, path string, typ string) (*map[string]any, bool, error) {
	resourceUsageTable, err := cluster.GetResourceUsageTableByTimestamp(timestamp, timestampRoundingPolicy)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "failed to get table by timestamp",
			log.Int64("timestamp", timestamp),
			log.String("timestampRoundingPolicy", timestampRoundingPolicy),
			log.Error(err),
		)
		return nil, false, err
	}

	features, err := resourceUsageTable.GetFeatures(ctx)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "failed to get features", log.Error(err))
		return nil, false, err
	}

	if features.RecursiveVersionedResourceUsage < 1 {
		return nil, false, nil
	}

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  cluster.Config.Proxy,
		Logger: resourceUsageTable.l,
		Token:  ytmsvc.TokenFromEnvVariable(cluster.TokenEnvVariable),
	})
	if err != nil {
		ctxlog.Error(ctx, resourceUsageTable.l.Logger(), "error creating yt client", log.Error(err))
		return nil, false, err
	}

	keys := []any{map[string]any{
		"account": account,
		"path":    path,
		"depth":   strings.Count(path, "/"),
	}}
	if features.TypeInKey > 0 {
		keys[0].(map[string]any)["type"] = typ
	}
	r, err := yc.LookupRows(ctx, resourceUsageTable.Path.YPath(), keys, nil)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "failed lookup rows", log.Any("path", resourceUsageTable.Path), log.Any("keys", keys))
		return nil, false, err
	}
	defer r.Close()

	type Item struct {
		RecursiveVersionedResourceUsage map[string]any `yson:"recursive_versioned_resource_usage"`
		VersionedResourceUsage          map[string]any `yson:"versioned_resource_usage"`
	}
	var item Item
	items := make(map[string]any, 0)
	for r.Next() {
		if err := r.Scan(&item); err != nil {
			ctxlog.Error(ctx, resourceUsageTable.l.Logger(), "error scanning row", log.Error(err))
			return nil, false, err
		}
		for key, value := range item.RecursiveVersionedResourceUsage {
			items[key] = value
		}
		for key, value := range item.VersionedResourceUsage {
			items[key] = value
		}
	}

	return &items, true, nil
}

func (cluster *Cluster) GetResourceUsageTableByTimestamp(timestamp int64, timestampRoundingPolicy string) (*ResourceUsageTable, error) {
	if timestampRoundingPolicy == roundingPolicyClosest {
		var closestResourceUsageTable *ResourceUsageTable
		var closestTimestampDiff int64 = 1<<63 - 1
		for _, ts := range cluster.ResourceUsageTables {
			timestampDiff := ts.Timestamp - timestamp
			if timestampDiff < 0 {
				timestampDiff = -timestampDiff
			}
			if timestampDiff <= closestTimestampDiff {
				closestTimestampDiff = timestampDiff
				closestResourceUsageTable = ts
			}
		}
		if closestResourceUsageTable == nil {
			return nil, fmt.Errorf("no closest timestamp found for %d", timestamp)
		}
		return closestResourceUsageTable, nil
	} else if timestampRoundingPolicy == roundingPolicyBackward {
		var backwardTimestampDiff int64 = 1<<63 - 1
		var backwardResourceUsageTable *ResourceUsageTable
		for _, ts := range cluster.ResourceUsageTables {
			timestampDiff := ts.Timestamp - timestamp
			if timestampDiff <= 0 && -timestampDiff <= backwardTimestampDiff {
				backwardTimestampDiff = -timestampDiff
				backwardResourceUsageTable = ts
			}
		}
		if backwardResourceUsageTable == nil {
			return nil, fmt.Errorf("no backward timestamp found for %d", timestamp)
		}
		return backwardResourceUsageTable, nil
	} else if timestampRoundingPolicy == roundingPolicyForward {
		var forwardTimestampDiff int64 = 1<<63 - 1
		var forwardResourceUsageTable *ResourceUsageTable
		for _, ts := range cluster.ResourceUsageTables {
			timestampDiff := ts.Timestamp - timestamp
			if timestampDiff >= 0 && timestampDiff <= forwardTimestampDiff {
				forwardTimestampDiff = timestampDiff
				forwardResourceUsageTable = ts
			}
		}
		if forwardResourceUsageTable == nil {
			return nil, fmt.Errorf("no forward timestamp found for %d", timestamp)
		}
		return forwardResourceUsageTable, nil
	}
	return nil, fmt.Errorf("invalid timestamp rounding policy: %s. supported rounding polices: [%s, %s, %s]", timestampRoundingPolicy, roundingPolicyBackward, roundingPolicyClosest, roundingPolicyForward)
}

func (cluster *Cluster) updateClusterData(ctx context.Context) {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  cluster.Config.Proxy,
		Logger: cluster.l,
		Token:  ytmsvc.TokenFromEnvVariable(cluster.TokenEnvVariable),
	})
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error creating yt client", log.Error(err))
		return
	}
	nodes := []string{}
	err = yc.ListNode(ctx, cluster.Config.BasePath, &nodes, nil)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting cluster data", log.Error(err))
		return
	}
	resourceUsageTables := []*ResourceUsageTable{}
	previousSnapshotId := ""
	for _, node := range nodes {
		if node == "tmp" {
			continue
		}
		lastDotIndex := strings.LastIndex(node, ".")
		if lastDotIndex == -1 {
			ctxlog.Error(ctx, cluster.l.Logger(), "node name is invalid: cannot find '.'", log.String("node", node))
			continue
		}
		snapshotID := node[:lastDotIndex]
		firstColonIndex := strings.Index(snapshotID, ":")
		if firstColonIndex == -1 {
			ctxlog.Error(ctx, cluster.l.Logger(), "node name is invalid: cannot find ':'", log.String("node", node), log.String("snapshotID", snapshotID))
			continue
		}
		if snapshotID != previousSnapshotId {
			timestampStr := snapshotID[:firstColonIndex]
			timestampInt, err := strconv.ParseInt(timestampStr, 10, 64)
			if err != nil {
				ctxlog.Error(ctx, cluster.l.Logger(), "node name is invalid: cannot convert timestamp to int",
					log.String("node", node),
					log.String("snapshotID", snapshotID),
					log.String("timestampStr", timestampStr),
					log.Error(err),
				)
				continue
			}
			resourceUsageTableLogger := log.With(cluster.l.Logger(), log.String("resource_usage_table", snapshotID)).Structured()

			resourceUsageTablePath := cluster.Config.BasePath.YPath().Child(node)
			resourceUsageTable := &ResourceUsageTable{
				Timestamp:                   timestampInt,
				SnapshotID:                  timestampInt,
				l:                           resourceUsageTableLogger,
				Path:                        resourceUsageTablePath,
				Proxy:                       cluster.Config.Proxy,
				ExcludedFields:              cluster.Config.ExcludedFields,
				ClusterSchemaCache:          cluster.SchemasCache,
				ClusterFeaturesCache:        cluster.FeaturesCache,
				TokenEnvVariable:            cluster.TokenEnvVariable,
				ContinuationTokenSerializer: cluster.ContinuationTokenSerializer,
			}
			resourceUsageTables = append(resourceUsageTables, resourceUsageTable)
			previousSnapshotId = ""
		} else {
			previousSnapshotId = snapshotID
		}
	}
	sort.Slice(resourceUsageTables, func(i, j int) bool {
		return resourceUsageTables[i].Timestamp < resourceUsageTables[j].Timestamp
	})
	cluster.ResourceUsageTables = resourceUsageTables
}

func (cluster *Cluster) GetClusterStatus(ctx context.Context) (TableReaderStatus, error) {
	return TableReaderStatus{
		MemoryUsage: TableReaderMemoryStatus{
			SchemaCache: CacheMemoryStatus{
				CurrentItemCount: cluster.SchemasCache.Len(),
				MaxItemCount:     100 * 1024,
			},
			FeaturesCache: CacheMemoryStatus{
				CurrentItemCount: cluster.FeaturesCache.Len(),
				MaxItemCount:     100 * 1024,
			},
		},
	}, nil
}

func (cluster *Cluster) GetResourceUsage(ctx context.Context, input ResourceUsageInput) (output ResourceUsageOutput, err error) {
	output.Items = &[]Item{}
	resourceUsageTable, err := cluster.GetResourceUsageTableByTimestamp(input.Timestamp, input.TimestampRoundingPolicy)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting timestamp", log.Error(err))
		return
	}

	output.SnapshotTimestamp = resourceUsageTable.Timestamp

	_, err = resourceUsageTable.GetSchema(ctx)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting schema", log.Error(err))
		return
	}
	_, err = resourceUsageTable.GetFeatures(ctx)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting features", log.Error(err))
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	var errCount error
	var errSelect error

	go func() {
		defer wg.Done()
		output.Items, output.ContinuationToken, errSelect = resourceUsageTable.SelectRows(
			ctx,
			selectRowsInput{
				account:      input.Account,
				filter:       input.RowFilter,
				pageSelector: input.PageSelector,
				sortOrders:   input.SortOrders,
			},
		)
	}()
	go func() {
		defer wg.Done()
		output.RowCount, errCount = resourceUsageTable.CountRows(
			ctx,
			countRowsInput{
				account:      input.Account,
				filter:       input.RowFilter,
				pageSelector: input.PageSelector,
			},
		)
	}()
	wg.Wait()
	if errSelect != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error selecting rows", log.Error(errSelect))
		err = errSelect
		return
	}
	if errCount != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error counting rows", log.Error(errCount))
		err = errCount
		return
	}

	output.Fields, err = resourceUsageTable.GetClearedFields(ctx)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting fields", log.Error(err))
		return
	}

	output.Mediums, err = resourceUsageTable.GetMediums(ctx)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting mediums", log.Error(err))
		return
	}

	return
}

func (cluster *Cluster) GetResourceUsageDiff(ctx context.Context, input ResourceUsageDiffInput) (output ResourceUsageDiffOutput, err error) {
	output.Items = &[]Item{}
	olderResourceUsageTable, err := cluster.GetResourceUsageTableByTimestamp(
		input.OlderTimestampSelector.Timestamp,
		input.OlderTimestampSelector.TimestampRoundingPolicy,
	)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting older timestamp", log.Error(err))
		return
	}
	output.OldSelectedSnapshot = olderResourceUsageTable.Timestamp

	newerResourceUsageTable, err := cluster.GetResourceUsageTableByTimestamp(
		input.NewerTimestampSelector.Timestamp,
		input.NewerTimestampSelector.TimestampRoundingPolicy,
	)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting newer timestamp", log.Error(err))
		return
	}
	output.NewSelectedSnapshot = newerResourceUsageTable.Timestamp

	intersections, _, _, err := olderResourceUsageTable.GetFieldsDiffs(ctx, newerResourceUsageTable)
	if err != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error getting fields diffs", log.Error(err))
		return
	}
	for field := range intersections {
		output.Fields = append(output.Fields, field)
		if strings.HasPrefix(field, "medium:") {
			output.Mediums = append(output.Mediums, strings.TrimPrefix(field, "medium:"))
		}
	}

	for _, resourceUsageTable := range []*ResourceUsageTable{olderResourceUsageTable, newerResourceUsageTable} {
		_, err = resourceUsageTable.GetSchema(ctx)
		if err != nil {
			ctxlog.Error(ctx, cluster.l.Logger(), "error getting schema", log.Error(err))
			return
		}
		_, err = resourceUsageTable.GetFeatures(ctx)
		if err != nil {
			ctxlog.Error(ctx, cluster.l.Logger(), "error getting features", log.Error(err))
			return
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	var errSelect error
	var errCount error

	go func() {
		defer wg.Done()
		output.Items, output.ContinuationToken, errSelect = olderResourceUsageTable.SelectRowsDiff(
			ctx,
			selectRowsDiffInput{
				account:      input.Account,
				newerRUT:     newerResourceUsageTable,
				filter:       input.RowFilter,
				pageSelector: input.PageSelector,
				sortOrders:   input.SortOrders,
			},
		)
	}()
	go func() {
		defer wg.Done()
		output.RowCount, errCount = olderResourceUsageTable.CountRowsDiff(
			ctx,
			countRowsDiffInput{
				account:      input.Account,
				filter:       input.RowFilter,
				pageSelector: input.PageSelector,
				newerRUT:     newerResourceUsageTable,
			},
		)
	}()
	wg.Wait()
	if errSelect != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error selecting rows diff", log.Error(errSelect))
		err = errSelect
		return
	}
	if errCount != nil {
		ctxlog.Error(ctx, cluster.l.Logger(), "error counting rows diff", log.Error(errCount))
		err = errCount
		return
	}

	return
}
