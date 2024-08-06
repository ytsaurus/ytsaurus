package chyt

import (
	"go.ytsaurus.tech/yt/go/ypath"
)

type Speclet struct {
	Resources

	CHYTVersion       *string `yson:"chyt_version"`
	TrampolineVersion *string `yson:"trampoline_version"`

	EnableGeodata *bool       `yson:"enable_geodata"`
	GeodataPath   *ypath.Path `yson:"geodata_path"`

	// RuntimeDataPath defines where all clique table belongings reside (e.g. stderr/core-tables, log dyntables, etc).
	RuntimeDataPath *ypath.Path `yson:"runtime_data_path"`

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

const (
	DefaultCHYTVersion       = "ytserver-clickhouse"
	DefaultTrampolineVersion = "clickhouse-trampoline"

	DefaultGeodataPath = ypath.Path("//sys/clickhouse/geodata/geodata.tgz")

	DefaultRuntimeDataPath = ypath.Path("//sys/clickhouse/kolkhoz")

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

func (speclet *Speclet) RuntimeDataPathOrDefault() ypath.Path {
	if speclet.RuntimeDataPath != nil {
		return *speclet.RuntimeDataPath
	}
	return DefaultRuntimeDataPath
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
