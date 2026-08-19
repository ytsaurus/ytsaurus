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

// AuthInfo describes who a request is executed on behalf of.
//
// UserLogin is the subject of the request; ServiceLogin identifies the calling
// service. Both may be set at once, when an authenticated service acts on behalf
// of an authenticated user. Either may be empty, but not both.
type AuthInfo struct {
	UserLogin    string `json:"user_login,omitempty"`
	ServiceLogin string `json:"service_login,omitempty"`
}

// ActingLogin returns the login the request is executed on behalf of: the user
// when one is present, the calling service otherwise.
func (a AuthInfo) ActingLogin() string {
	if a.UserLogin != "" {
		return a.UserLogin
	}
	return a.ServiceLogin
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
	TokenEnvVariable  string
}
