package lib

import "go.ytsaurus.tech/yt/go/yt"

type CheckACLRequest struct {
	Cluster string   `json:"cluster"`
	Subject string   `json:"subject"`
	Paths   []string `json:"paths"`
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
