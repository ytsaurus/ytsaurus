package chyt

import (
	"go.ytsaurus.tech/yt/go/ypath"
)

type Speclet struct {
	Resources

	CHYTVersion       *string `yson:"chyt_version"`
	TrampolineVersion *string `yson:"trampoline_version"`

	EnableGeoData *bool       `yson:"enable_geodata"`
	GeoDataPath   *ypath.Path `yson:"geodata_path"`

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
}

const (
	DefaultCHYTVersion       = "ytserver-clickhouse"
	DefaultTrampolineVersion = "clickhouse-trampoline"

	DefaultEnableGeoData = true
	DefaultGeoDataPath   = ypath.Path("//sys/clickhouse/geodata/geodata.tgz")

	DefaultRuntimeDataPath = ypath.Path("//sys/clickhouse/kolkhoz")
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

func (speclet *Speclet) EnableGeoDataOrDefault() bool {
	if speclet.EnableGeoData != nil {
		return *speclet.EnableGeoData
	}
	return DefaultEnableGeoData
}

func (speclet *Speclet) GeoDataPathOrDefault() ypath.Path {
	if speclet.GeoDataPath != nil {
		return *speclet.GeoDataPath
	}
	return DefaultGeoDataPath
}

func (speclet *Speclet) RuntimeDataPathOrDefault() ypath.Path {
	if speclet.RuntimeDataPath != nil {
		return *speclet.RuntimeDataPath
	}
	return DefaultRuntimeDataPath
}
