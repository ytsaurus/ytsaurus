package chyt

import "a.yandex-team.ru/yt/go/ypath"

type ArtifactSpec struct {
	Path       *ypath.Path `yson:"path"`
	AutoUpdate *bool       `yson:"auto_update"`
}

type YTServerClickHouse = ArtifactSpec
type YTServerLogTailer = ArtifactSpec
type ClickHouseTrampoline = ArtifactSpec
type GeoData = ArtifactSpec

type Speclet struct {
	// Resources defines CPU, memory and instance count for Clique.
	Resources *Resources `yson:"resources"`

	// YTServerClickHouse, YTServerLogTailer, ClickHouseTrampoline binaries and GeoData archive are the
	// artifacts required for proper clique functioning.
	YTServerClickHouse   *YTServerClickHouse   `yson:"ytserver_clickhouse"`
	YTServerLogTailer    *YTServerLogTailer    `yson:"ytserver_log_tailer"`
	ClickHouseTrampoline *ClickHouseTrampoline `yson:"clickhouse_trampoline"`
	GeoData              *GeoData              `yson:"geodata"`

	// OperationDescription is visible in clique operation UI.
	// OperationTitle is YT operation title visible in operation UI.
	// OperationAnnotations allows adding arbitrary human-readable annotations visible via YT list_operations API.
	OperationDescription map[string]interface{} `yson:"operation_description"`
	OperationTitle       *string                `yson:"operation_title"`
	OperationAnnotations map[string]interface{} `yson:"operation_annotations"`

	// TODO(max42)
	ClickHouseConfigPatch map[string]interface{} `yson:"clickhouse_config_patch"`
	YTConfigPatch         map[string]interface{} `yson:"yt_config_patch"`
	RootConfigPatch       map[string]interface{} `yson:"root_config_patch"`

	// RuntimeDataPath defines where all clique table belongings reside (e.g. stderr/core-tables, log dyntables, etc).
	RuntimeDataPath *ypath.Path `yson:"runtime_data_path"`
}
