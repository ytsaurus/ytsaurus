package chyt

import (
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
)

type Speclet struct {
	Resources

	CHYTVersion       *string `yson:"chyt_version"`
	TrampolineVersion *string `yson:"trampoline_version"`

	EnableGeodata *bool       `yson:"enable_geodata"`
	GeodataPath   *ypath.Path `yson:"geodata_path"`

	EnableRuntimeData *bool `yson:"enable_runtime_data"`
	// RuntimeDataPath defines where all clique table belongings reside (e.g. stderr/core-tables, log dyntables, etc).
	RuntimeDataPath              *ypath.Path `yson:"runtime_data_path"`
	RuntimeDataExpirationTimeout *uint64     `yson:"runtime_data_expiration_timeout"`

	// QuerySettings defines default settings for queries.
	QuerySettings map[string]any `yson:"query_settings"`

	// ClickHouseConfig is a base config for ClickHouse part of CHYT. Its usage is highly discouraged.
	ClickHouseConfig map[string]any `yson:"clickhouse_config"`
	// YTConfig is a base config for YT part of CHYT. Its usage is highly discouraged.
	YTConfig map[string]any `yson:"yt_config"`

	// BuiltinLogRotationPolicy contains options for builtin log rotation.
	BuiltinLogRotationPolicy map[string]any `yson:"builtin_log_rotation_policy"`

	ExportSystemLogTables *bool `yson:"export_system_log_tables"`

	EnableStickyQueryDistribution bool `yson:"enable_sticky_query_distribution"`
	QueryStickyGroupSize          *int `yson:"query_sticky_group_size"`
}

type runtimeDataSpec struct {
	RuntimeDataPath              ypath.Path
	RuntimeDataExpirationTimeout uint64
}

const (
	DefaultCHYTVersion       = "ytserver-clickhouse"
	DefaultTrampolineVersion = "clickhouse-trampoline"

	DefaultGeodataPath = ypath.Path("//sys/clickhouse/geodata/geodata.tgz")

	DefaultRuntimeDataPath              = ypath.Path("//sys/clickhouse/kolkhoz")
	DefaultRuntimeDataExpirationTimeout = 12 * 7 * (24 * time.Hour)

	DefaultQueryStickyGroupSize = 2
)

func (speclet *Speclet) CHYTVersionOrDefault() string {
	if speclet.CHYTVersion != nil {
		return *speclet.CHYTVersion
	}
	return DefaultCHYTVersion
}

func (speclet *Speclet) TrampolineVersionOrDefault() string {
	if speclet.TrampolineVersion != nil {
		return *speclet.TrampolineVersion
	}
	return DefaultTrampolineVersion
}

func (speclet *Speclet) EnableGeodataOrDefault(defaultValue bool) bool {
	if speclet.EnableGeodata != nil {
		return *speclet.EnableGeodata
	}
	return defaultValue
}

func (speclet *Speclet) GeodataPathOrDefault() ypath.Path {
	if speclet.GeodataPath != nil {
		return *speclet.GeodataPath
	}
	return DefaultGeodataPath
}

func (speclet *Speclet) EnableRuntimeDataOrDefault(defaultValue bool) bool {
	if speclet.EnableRuntimeData != nil {
		return *speclet.EnableRuntimeData
	}
	return defaultValue
}

func (speclet *Speclet) RuntimeDataSpecOrDefault() runtimeDataSpec {
	spec := runtimeDataSpec{
		RuntimeDataPath:              DefaultRuntimeDataPath,
		RuntimeDataExpirationTimeout: uint64(DefaultRuntimeDataExpirationTimeout.Milliseconds()),
	}

	if speclet.RuntimeDataPath != nil {
		spec.RuntimeDataPath = *speclet.RuntimeDataPath
	}
	if speclet.RuntimeDataExpirationTimeout != nil {
		spec.RuntimeDataExpirationTimeout = *speclet.RuntimeDataExpirationTimeout
	}

	return spec
}

func (speclet *Speclet) ExportSystemLogTablesOrDefault(defaultValue bool) bool {
	if speclet.ExportSystemLogTables != nil {
		return *speclet.ExportSystemLogTables
	}
	return defaultValue
}

func (speclet *Speclet) QueryStickyGroupSizeOrDefault() int {
	if speclet.QueryStickyGroupSize != nil {
		return *speclet.QueryStickyGroupSize
	}
	return DefaultQueryStickyGroupSize
}
