package resourceusage

import (
	"context"
	"fmt"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
)

func NewResourceUsage(c *Config, l log.Structured) *ResourceUsage {
	return &ResourceUsage{
		conf: c,
		l:    l,
		data: &Data{ServedClusters: map[string]*Cluster{}},
	}
}

func (ru *ResourceUsage) GetCluster(ctx context.Context, cluster string) (*Cluster, error) {
	if cluster, ok := ru.data.ServedClusters[cluster]; ok {
		if ru.conf.UpdateSnapshotsOnEveryRequest {
			cluster.updateClusterData(ctx)
		}
		return cluster, nil
	}
	servedClusters, err := ru.GetServedClusters()
	if err != nil {
		return nil, fmt.Errorf("error getting served clusters: %w", err)
	}
	return nil, fmt.Errorf("cluster not found: `%s`. Served clusters: `%s`", cluster, strings.Join(servedClusters, "`, `"))
}

func (ru *ResourceUsage) StartServingClusters(ctx context.Context) {
	timer := time.NewTimer(0)
	delay := time.Duration(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			newServedClusters := map[string]*Cluster{}
			for _, clusterConfig := range ru.conf.IncludedClusters {
				if cluster, alreadyServed := ru.data.ServedClusters[clusterConfig.ClusterName]; alreadyServed {
					newServedClusters[clusterConfig.ClusterName] = cluster
				} else {
					newServedClusters[clusterConfig.ClusterName] = ru.newCluster(clusterConfig)
				}
				go newServedClusters[clusterConfig.ClusterName].updateClusterData(ctx)
			}
			ru.data.ServedClusters = newServedClusters
			timer.Reset(delay)
		}
	}
}

func (ru *ResourceUsage) newCluster(clusterConfig *ClusterConfig) *Cluster {
	clusterLogger := log.With(ru.l.Logger(), log.String("cluster", clusterConfig.ClusterName), log.String("proxy", clusterConfig.Proxy)).Structured()
	if clusterConfig.BasePath == nil {
		clusterConfig.BasePath = ru.conf.SnapshotRoot
	}
	if clusterConfig.ExcludedFields == nil {
		clusterConfig.ExcludedFields = ru.conf.ExcludedFields
	}
	return &Cluster{
		Config:              clusterConfig,
		ResourceUsageTables: []*ResourceUsageTable{},
		l:                   clusterLogger,
		SchemasCache:        lru.NewLRU[ypath.YPath, *schema.Schema](100*1024, nil, 0),
		FeaturesCache:       lru.NewLRU[ypath.YPath, *ResourceUsageTableFeatures](100*1024, nil, 0),
	}
}

func (ru *ResourceUsage) GetListTimestamps(proxy string, min_timestamp int64, max_timestamp int64) []int64 {
	if max_timestamp == 0 {
		max_timestamp = time.Now().Unix()
	}
	timestamps := []int64{}
	for _, resourceUsageTable := range ru.data.ServedClusters[proxy].ResourceUsageTables {
		timestamp := resourceUsageTable.Timestamp
		if min_timestamp <= timestamp && timestamp <= max_timestamp {
			timestamps = append(timestamps, timestamp)
		}
	}
	return timestamps
}

func (ru *ResourceUsage) GetServedClusters() ([]string, error) {
	if ru.data.ServedClusters == nil {
		return nil, fmt.Errorf("no served clusters found")
	}
	clusters := []string{}
	for cluster := range ru.data.ServedClusters {
		clusters = append(clusters, cluster)
	}
	return clusters, nil
}
