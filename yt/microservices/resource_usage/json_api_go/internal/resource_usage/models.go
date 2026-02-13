package resourceusage

import (
	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

type Item map[string]any

type Filter struct {
	Owner           string           `json:"owner,omitempty"`
	PathRegexp      string           `json:"path_regexp,omitempty"`
	ExcludeMapNodes bool             `json:"exclude_map_nodes,omitempty"`
	TableType       string           `json:"table_type,omitempty"`
	FieldFilters    []IntFilterParam `json:"field_filters,omitempty"`
	BasePath        string           `json:"base_path,omitempty"`
}

type ResourceUsageTableKeys struct {
	Account string `yson:"account"`
	Path    string `yson:"path"`
	Depth   int    `yson:"depth"`
}

type IntFilterParam struct {
	Field      string `json:"field"`
	Value      int    `json:"value"`
	Comparison string `json:"comparison"`
}

type SortOrder struct {
	Field string `json:"field,omitempty"`
	Desc  bool   `json:"desc,omitempty"`
}

type PageSelector struct {
	Index                   int    `json:"index,omitempty"`
	Size                    int    `json:"size,omitempty"`
	ContinuationToken       string `json:"continuation_token,omitempty"`
	EnableContinuationToken bool   `json:"enable_continuation_token,omitempty"`
}

type TableReaderStatus struct {
	MemoryUsage TableReaderMemoryStatus `json:"memory_usage"`
}

type TableReaderMemoryStatus struct {
	SchemaCache   CacheMemoryStatus `json:"schema_cache"`
	FeaturesCache CacheMemoryStatus `json:"features_cache"`
}

type ResourceUsageTable struct {
	Timestamp                   int64
	SnapshotID                  int64
	Path                        ypath.YPath
	l                           log.Structured
	Proxy                       string
	ExcludedFields              []string
	ClusterSchemaCache          *lru.LRU[ypath.YPath, *schema.Schema]
	ClusterFeaturesCache        *lru.LRU[ypath.YPath, *ResourceUsageTableFeatures]
	TokenEnvVariable            string
	ContinuationTokenSerializer *ytmsvc.CryptoSerializer[ContinuationToken]
}

type TimestampSelector struct {
	Timestamp               int64  `json:"timestamp"`
	TimestampRoundingPolicy string `json:"timestamp_rounding_policy"`
}

type ResourceUsageTableFeatures struct {
	TypeInKey                       int `yson:"type_in_key"`
	RecursiveVersionedResourceUsage int `yson:"recursive_versioned_resource_usage"`
}

type ResourceUsage struct {
	l                           log.Structured
	conf                        *Config
	data                        *Data
	continuationTokenSerializer *ytmsvc.CryptoSerializer[ContinuationToken]
}

type Config struct {
	ServeClustersFrom             string
	IncludedClusters              []*ClusterConfig
	ExcludedClusters              []*ClusterConfig
	SnapshotRoot                  ypath.YPath
	ExcludedFields                []string
	UpdateSnapshotsOnEveryRequest bool
	TokenEnvVariable              string
}

type Data struct {
	ServedClusters map[string]*Cluster
}

type Cluster struct {
	ResourceUsageTables         []*ResourceUsageTable
	SchemasCache                *lru.LRU[ypath.YPath, *schema.Schema]
	FeaturesCache               *lru.LRU[ypath.YPath, *ResourceUsageTableFeatures]
	l                           log.Structured
	Config                      *ClusterConfig
	TokenEnvVariable            string
	ContinuationTokenSerializer *ytmsvc.CryptoSerializer[ContinuationToken]
}

type ClusterConfig struct {
	BasePath       ypath.YPath `yaml:"base_path"`
	Proxy          string      `yaml:"proxy"`
	ClusterName    string      `yaml:"cluster_name"`
	ExcludedFields []string    `yaml:"excluded_fields"`
}

type CacheMemoryStatus struct {
	CurrentItemCount int `json:"current_item_count"`
	MaxItemCount     int `json:"max_item_count"`
}

type ResourceUsageInput struct {
	Account                 string
	Timestamp               int64
	TimestampRoundingPolicy string
	RowFilter               *Filter
	SortOrders              []*SortOrder
	PageSelector            *PageSelector
}

type ResourceUsageOutput struct {
	SnapshotTimestamp int64
	Fields            []string
	RowCount          int
	Items             *[]Item
	Mediums           []string
	ContinuationToken string
}

type ResourceUsageDiffInput struct {
	Account                string
	OlderTimestampSelector *TimestampSelector
	NewerTimestampSelector *TimestampSelector
	RowFilter              *Filter
	SortOrders             []*SortOrder
	PageSelector           *PageSelector
}

type ResourceUsageDiffOutput struct {
	OldSelectedSnapshot int64
	NewSelectedSnapshot int64
	Fields              []string
	RowCount            int
	Items               *[]Item
	Mediums             []string
	ContinuationToken   string
}

type selectRowsDiffInput struct {
	account      string
	newerRUT     *ResourceUsageTable
	filter       *Filter
	pageSelector *PageSelector
	sortOrders   []*SortOrder
}

type selectRowsInput struct {
	account      string
	filter       *Filter
	pageSelector *PageSelector
	sortOrders   []*SortOrder
}

type countRowsInput struct {
	account      string
	filter       *Filter
	pageSelector *PageSelector
}

type countRowsDiffInput struct {
	account      string
	filter       *Filter
	newerRUT     *ResourceUsageTable
	pageSelector *PageSelector
}
