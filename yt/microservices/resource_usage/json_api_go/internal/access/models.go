package access

import (
	"go.ytsaurus.tech/library/go/core/log"
	bulkaclcheckerclient "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/client_go"
	resourceusage "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/resource_usage"
)

type Data struct {
	ServedClusters map[string]*Cluster
}

type Cluster struct {
	l      log.Structured
	Config *Config
}

type Response struct {
	User string `json:"user"`
}

type contextKey string

const AuthInfoKey contextKey = "auth_info"

type AuthInfo struct {
	Login     string `json:"login"`
	IsService bool   `json:"is_service"`
}

type ACLCacheStatus struct {
	Ready       bool                 `json:"ready"`
	MemoryUsage ACLCacheMemoryStatus `json:"memory_usage"`
}

type ACLCacheMemoryStatus struct {
	SnapshotCache resourceusage.CacheMemoryStatus `json:"snapshot_cache"`
	ACLCheckCache resourceusage.CacheMemoryStatus `json:"acl_check_cache"`
}

type AccessCheckerBase struct {
	l         log.Structured
	conf      *Config
	aclClient *bulkaclcheckerclient.Client
}

type ConfigBase struct {
	DisableACL        bool
	DebugLogin        string
	BulkACLCheckerURL string
	IncludedClusters  []*resourceusage.ClusterConfig
}
