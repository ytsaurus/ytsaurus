package lib

import "go.ytsaurus.tech/yt/go/yt"

type CheckACLRequest struct {
	Cluster    string        `json:"cluster"`
	Subject    string        `json:"subject"`
	Permission yt.Permission `json:"permission,omitempty"`
	Paths      []string      `json:"paths"`
}

type ClickHouseDictRequest struct {
	Cluster string `json:"cluster"`
	Subject string `json:"subject"`
	Path    string `json:"path"`
}

type ClickHouseDictResponse struct {
	Cluster string `json:"cluster"`
	Subject string `json:"subject"`
	Path    string `json:"path"`
	Action  string `json:"action"`
}

type CheckACLResponse struct {
	Actions []yt.SecurityAction `json:"actions"`
}

type ClusterInfo struct {
	Version       string `json:"version"`
	CheckACLLocal bool   `json:"check_acl_local"`
}

type InfoResponse struct {
	Clusters map[string]ClusterInfo `json:"clusters"`
}

type ServedClustersResponse struct {
	Clusters []string `json:"clusters"`
}
