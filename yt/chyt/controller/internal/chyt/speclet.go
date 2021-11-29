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
	OperationDescription map[string]interface{} `yson:"operation_description"`
	// OperationTitle is YT operation title visible in operation UI.
	OperationTitle *string `yson:"operation_title"`
	// OperationAnnotations allows adding arbitrary human-readable annotations visible via YT list_operations API.
	OperationAnnotations map[string]interface{} `yson:"operation_annotations"`

	ClickHouseConfig map[string]interface{} `yson:"clickhouse_config"`
	YTConfig         map[string]interface{} `yson:"yt_config"`
	// TODO(dakovalkov): RootConfigPatch is not supported yet.
	RootConfigPatch map[string]interface{} `yson:"root_config_patch"`

	// RuntimeDataPath defines where all clique table belongings reside (e.g. stderr/core-tables, log dyntables, etc).
	RuntimeDataPath *ypath.Path `yson:"runtime_data_path"`
}
