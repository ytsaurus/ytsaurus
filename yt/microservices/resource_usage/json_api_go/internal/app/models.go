package app

import (
	access "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/access"
	resourceusage "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/resource_usage"
)

type RequestWithLoginOverride struct {
	ViewAsLogin string `json:"view_as_login,omitempty"`
}

type AtomsStatus struct {
	ACLCache    access.ACLCacheStatus           `json:"acl_cache"`
	TableReader resourceusage.TableReaderStatus `json:"table_reader"`
}

type StatusResponse struct {
	Stats map[string]AtomsStatus `json:"stats"`
	Error string                 `json:"error,omitempty"`
}

type GetVersionedResourceUsageRequest struct {
	resourceusage.TimestampSelector
	Cluster string `json:"cluster,omitempty"`
	Account string `json:"account,omitempty"`
	Path    string `json:"path,omitempty"`
	Type    string `json:"type,omitempty"`
}

type GetVersionedResourceUsageResponse struct {
	Transactions *map[string]any `json:"transactions"`
	Available    bool            `json:"available"`
	Error        string          `json:"error,omitempty"`
}

type GetResourceUsageDiffRequest struct {
	RequestWithLoginOverride
	Cluster    string                      `json:"cluster,omitempty"`
	Account    string                      `json:"account,omitempty"`
	Timestamps *DiffSnapshotSelector       `json:"timestamps,omitempty"`
	RowFilter  *resourceusage.Filter       `json:"row_filter,omitempty"`
	SortOrders []*resourceusage.SortOrder  `json:"sort_order,omitempty"`
	Page       *resourceusage.PageSelector `json:"page,omitempty"`
}

type GetResourceUsageDiffResponse struct {
	SnapshotTimestamps DiffSnapshotSelectorResponse `json:"snapshot_timestamps"`
	Fields             []string                     `json:"fields"`
	RowCount           int                          `json:"row_count"`
	Mediums            []string                     `json:"mediums"`
	Items              *[]resourceusage.Item        `json:"items"`
	ContinuationToken  string                       `json:"continuation_token,omitempty"`
	Error              string                       `json:"error,omitempty"`
}

type DiffSnapshotSelectorResponse struct {
	Older int64 `json:"older,omitempty"`
	Newer int64 `json:"newer,omitempty"`
}

type GetResourceUsageRequest struct {
	RequestWithLoginOverride
	Cluster                 string                      `json:"cluster,omitempty"`
	Account                 string                      `json:"account,omitempty"`
	Timestamp               int64                       `json:"timestamp,omitempty"`
	TimestampRoundingPolicy string                      `json:"timestamp_rounding_policy,omitempty"`
	RowFilter               *resourceusage.Filter       `json:"row_filter,omitempty"`
	SortOrders              []*resourceusage.SortOrder  `json:"sort_order,omitempty"`
	Page                    *resourceusage.PageSelector `json:"page,omitempty"`
}

type GetResourceUsageResponse struct {
	SnapshotTimestamp int64                 `json:"snapshot_timestamp"`
	Fields            []string              `json:"fields"`
	RowCount          int                   `json:"row_count"`
	Mediums           []string              `json:"mediums"`
	Items             *[]resourceusage.Item `json:"items"`
	ContinuationToken string                `json:"continuation_token,omitempty"`
	Error             string                `json:"error,omitempty"`
}

type DiffSnapshotSelector struct {
	Older *resourceusage.TimestampSelector `json:"older,omitempty"`
	Newer *resourceusage.TimestampSelector `json:"newer,omitempty"`
}

type ServedClustersResponse struct {
	Clusters []string `json:"clusters"`
	Error    string   `json:"error,omitempty"`
}

type ListTimestampsResponse struct {
	SnapshotTimestamps []int64 `json:"snapshot_timestamps"`
	Error              string  `json:"error,omitempty"`
}

type ListTimestampsRequest struct {
	Cluster string `json:"cluster"`
	From    int64  `json:"from"`
	To      int64  `json:"to"`
}

type WhoamiResponse struct {
	User      string `json:"user"`
	IsService bool   `json:"is_service"`
	Error     string `json:"error,omitempty"`
}
