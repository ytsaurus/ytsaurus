package chyt

import (
	"a.yandex-team.ru/yt/go/ypath"
)

type Speclet struct {
	Resources

	CHYTVersion       *string `yson:"chyt_version"`
	LogTailerVersion  *string `yson:"log_tailer_version"`
	TrampolineVersion *string `yson:"trampoline_version"`

	EnableGeoData *bool       `yson:"enable_geodata"`
	GeoDataPath   *ypath.Path `yson:"geodata_path"`

	// RuntimeDataPath defines where all clique table belongings reside (e.g. stderr/core-tables, log dyntables, etc).
	RuntimeDataPath *ypath.Path `yson:"runtime_data_path"`

	// QuerySettings defines default settings for queries.
	QuerySettings map[string]interface{} `yson:"query_settings"`

	// ClickHouseConfig is a base config for ClickHouse part of CHYT. Its usage is highly discouraged.
	ClickHouseConfig map[string]interface{} `yson:"clickhouse_config"`
	// YTConfig is a base config for YT part of CHYT. Its usage is highly discouraged.
	YTConfig map[string]interface{} `yson:"yt_config"`
}

const (
	DefaultCHYTVersion       = "stable-latest"
	DefaultLogTailerVersion  = "stable-latest"
	DefaultTrampolineVersion = "stable-latest"

	DefaultEnableGeoData = true
	DefaultGeoDataPath   = ypath.Path("//sys/clickhouse/geodata/geodata.gz")
)

func (speclet *Speclet) CHYTVersionOrDefault() string {
	if speclet.CHYTVersion != nil {
		return *speclet.CHYTVersion
	}
	return DefaultCHYTVersion
}

func (speclet *Speclet) LogTailerVersionOrDefault() string {
	if speclet.LogTailerVersion != nil {
		return *speclet.LogTailerVersion
	}
	return DefaultLogTailerVersion
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
