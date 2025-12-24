package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
)

type Processing struct {
	// Node in YT where the final, processed tables will be stored.
	OutputPath ypath.Path `yaml:"output_path"`
	// Defines the time duration for each output table (e.g., 1h, 24h).
	// Logs will be grouped into tables based on this interval.
	Interval time.Duration `yaml:"interval"`
	// Duration specifies how long to wait after a time interval ends before creating the final table,
	// allowing late-arriving logs to be included.
	CutoffTime time.Duration `yaml:"cutoff_time"`
	// Format string for the output table names (e.g., "2006-01-02" for daily tables).
	// It uses Go's time layout format.
	TableFormat string `yaml:"table_format"`
}

type Config struct {
	// YT cluster proxy to connect to.
	Proxy string `yaml:"proxy"`
	// YT token env variable to use for authentication.
	TokenEnvVariable string `yaml:"token_env_variable"`
	// Path to the node for exclusive lock.
	LockPath ypath.Path `yaml:"lock_path"`
	// If set, will replace `cluster` from raw log with the specified one.
	ForceClusterName string `yaml:"force_cluster_name"`
	// Path to the node containing the raw access log tables.
	AccessLogExportPath ypath.Path `yaml:"access_log_export_path"`
	// WorkPath is a node used for storing intermediate processing results.
	WorkPath ypath.Path `yaml:"work_path"`
	// Maximum number of raw input tables to process in a single run.
	MaxInputTables int `yaml:"max_input_tables"`
	// Map of one or more processing configurations to apply.
	Processings map[string]*Processing `yaml:"processings"`
}

func ReadConfig(path string) (*Config, error) {
	if path == "" {
		return nil, xerrors.New("config path is required")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	conf := newDefaultConfig()
	err = yaml.Unmarshal(data, &conf)
	return conf, err
}

func (c *Config) Validate() error {
	if c.AccessLogExportPath == "" {
		return xerrors.New("access_log_export_path is required")
	}
	if c.WorkPath == "" {
		return xerrors.New("work_path is required")
	}
	if c.LockPath == "" {
		return xerrors.New("lock_path is required")
	}
	if c.MaxInputTables <= 0 {
		return xerrors.New("max_input_tables is required and must be more than 0")
	}
	if len(c.Processings) == 0 {
		return xerrors.New("processings is required")
	}
	for _, processingConfig := range c.Processings {
		if err := processingConfig.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (p *Processing) Validate() error {
	if p.OutputPath == "" {
		return xerrors.New("output_path is required")
	}
	if p.Interval == 0 {
		return xerrors.New("interval is required")
	}
	t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if t.Format(p.TableFormat) == t.Add(p.Interval).Format(p.TableFormat) {
		return xerrors.Errorf("table_format %q does not provide enough granularity for interval %s", p.TableFormat, p.Interval)
	}
	return nil
}

func newDefaultConfig() *Config {
	return &Config{
		MaxInputTables: 2,
	}
}
