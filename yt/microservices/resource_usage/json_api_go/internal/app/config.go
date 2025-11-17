package app

import (
	"time"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
	resourceusage "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/resource_usage"
)

const (
	defaultHTTPHandlerTimeout = 2 * time.Minute
	defaultTokenEnvVariable   = "YT_RESOURCE_USAGE_TOKEN"
)

type ConfigBase struct {
	// HTTP server address where the API will be available.
	HTTPAddr string `yaml:"http_addr"`
	// Timeout for HTTP requests.
	HTTPHandlerTimeout time.Duration `yaml:"http_handler_timeout"`
	// HTTP server address where metrics will be available.
	DebugHTTPAddr string `yaml:"debug_http_addr"`

	// CORS settings.
	CORS *ytmsvc.CORSConfig `yaml:"cors"`

	// Path to the directory with resource_usage preprocessing.
	SnapshotRoot string `yaml:"snapshot_root"`

	// Where to write logs.
	LogsDir string `yaml:"logs_dir"`

	// Which fields to exclude from the response.
	ExcludedFields []string `yaml:"excluded_fields"`

	// Whether to update the list of prepared preprocessings on each request.
	// Required only for tests.
	UpdateSnapshotsOnEveryRequest bool `yaml:"update_snapshots_on_every_request"`

	// List of clusters that will be examined.
	IncludedClusters []*resourceusage.ClusterConfig `yaml:"included_clusters"`

	// URL of the bulk ACL checker.
	BulkACLCheckerURL string `yaml:"bulk_acl_checker_base_url"`

	// Debug login for testing purposes.
	DebugLogin string `yaml:"debug_login"`

	// Disable ACL for testing purposes.
	DisableACL bool `yaml:"disable_acl"`

	// Environment variable that specifies the token used when accessing YT.
	// By default, YT_RESOURCE_USAGE_TOKEN is used.
	TokenEnvVariable string `yaml:"token_env_variable"`
}

func (c *Config) UnmarshalYAML(unmarshal func(any) error) error {
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.SnapshotRoot == "" {
		return xerrors.New("snapshot_root can not be empty")
	}
	if c.LogsDir == "" {
		return xerrors.New("logs_dir can not be empty")
	}

	if c.ExcludedFields == nil {
		c.ExcludedFields = []string{}
	}

	if c.BulkACLCheckerURL == "" {
		return xerrors.New("bulk_acl_checker_base_url is required")
	}

	if c.HTTPHandlerTimeout == 0 {
		c.HTTPHandlerTimeout = defaultHTTPHandlerTimeout
	}

	if c.TokenEnvVariable == "" {
		c.TokenEnvVariable = defaultTokenEnvVariable
	}

	return nil
}
