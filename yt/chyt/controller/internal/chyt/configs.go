package chyt

import (
	"context"
	"errors"
	"fmt"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

func cloneNode(ysonNode interface{}) (ysonNodeCopy interface{}, err error) {
	ysonString, err := yson.Marshal(ysonNode)
	if err != nil {
		return
	}
	err = yson.Unmarshal(ysonString, &ysonNodeCopy)
	return
}

func asMapNode(ysonNode interface{}) (asMap map[string]interface{}, err error) {
	asMap, ok := yson.ValueOf(ysonNode).(map[string]interface{})
	if !ok {
		err = errors.New("yson node type is not 'Map'")
	}
	return
}

func getPatchedClickHouseConfig(speclet *Speclet) (config interface{}, err error) {
	config, err = cloneNode(speclet.ClickHouseConfig)
	if err != nil {
		return
	}
	if config == nil {
		config = make(map[string]interface{})
	}
	configAsMap, err := asMapNode(config)
	if err != nil {
		return
	}

	if _, ok := configAsMap["path_to_regions_names_files"]; !ok {
		configAsMap["path_to_regions_names_files"] = "./geodata/"
	}

	if _, ok := configAsMap["path_to_regions_hierarchy_file"]; !ok {
		configAsMap["path_to_regions_hierarchy_file"] = "./geodata/regions_hierarchy.txt"
	}

	if _, ok := configAsMap["settings"]; !ok {
		configAsMap["settings"] = make(map[string]interface{})
	}
	settings, err := asMapNode(configAsMap["settings"])
	if err != nil {
		err = fmt.Errorf("invalid settings config: %v", err)
		return
	}
	if _, ok := settings["max_threads"]; !ok {
		settings["max_threads"] = *speclet.Resources.InstanceCPU
	}
	if _, ok := settings["queue_max_wait_ms"]; !ok {
		settings["queue_max_wait_ms"] = 30 * 1000
	}
	if _, ok := settings["max_concurrent_queries_for_user"]; !ok {
		settings["max_concurrent_queries_for_user"] = 10 * 1000
	}
	if _, ok := settings["max_temporary_non_const_columns"]; !ok {
		settings["max_temporary_non_const_columns"] = 1234
	}

	return
}

func getPatchedYtConfig(speclet *Speclet) (config interface{}, err error) {
	config, err = cloneNode(speclet.YTConfig)
	if err != nil {
		return
	}
	if config == nil {
		config = make(map[string]interface{})
	}
	configAsMap, err := asMapNode(config)
	if err != nil {
		return
	}
	// TODO(max42): put to preprocessor similarly to yt/cpu_limit.
	if _, ok := configAsMap["worker_thread_count"]; !ok {
		configAsMap["worker_thread_count"] = *speclet.Resources.InstanceCPU
	}

	if _, ok := configAsMap["enable_dynamic_tables"]; !ok {
		configAsMap["enable_dynamic_tables"] = true
	}

	if _, ok := configAsMap["discovery"]; !ok {
		configAsMap["discovery"] = make(map[string]interface{})
	}
	discovery, err := asMapNode(configAsMap["discovery"])
	if err != nil {
		err = fmt.Errorf("invalid discovery config: %v", err)
		return
	}
	if _, ok := discovery["transaction_timeout"]; !ok {
		discovery["transaction_timeout"] = 30 * 1000
	}

	if _, ok := configAsMap["health_checker"]; !ok {
		configAsMap["health_checker"] = make(map[string]interface{})
	}
	healthChecker, err := asMapNode(configAsMap["health_checker"])
	if err != nil {
		err = fmt.Errorf("invalid health_checker config: %v", err)
		return
	}
	if _, ok := healthChecker["queries"]; !ok {
		healthChecker["queries"] = [1]string{"select * from `//sys/clickhouse/sample_table`"}
	}
	if _, ok := healthChecker["preiod"]; !ok {
		healthChecker["preiod"] = 60 * 1000
	}

	return
}

func (c *Controller) uploadConfig(ctx context.Context, alias string, filename string, config interface{}) (richPath ypath.Rich, err error) {
	configYson, err := yson.MarshalFormat(config, yson.FormatPretty)
	if err != nil {
		return
	}
	path := c.root.Child(alias).Child(filename)
	_, err = c.ytc.CreateNode(ctx, path, yt.NodeFile, &yt.CreateNodeOptions{IgnoreExisting: true})
	if err != nil {
		return
	}
	w, err := c.ytc.WriteFile(ctx, path, nil)
	if err != nil {
		return
	}
	_, err = w.Write(configYson)
	if err != nil {
		return
	}
	err = w.Close()
	if err != nil {
		return
	}
	richPath = ypath.Rich{Path: path, FileName: filename}
	return
}

func (c *Controller) appendConfigs(ctx context.Context, alias string, speclet *Speclet, filePaths *[]ypath.Rich) (err error) {
	r := speclet.Resources

	clickhouseConfig, err := getPatchedClickHouseConfig(speclet)
	if err != nil {
		return fmt.Errorf("invalid clickhouse config: %v", err)
	}
	ytConfig, err := getPatchedYtConfig(speclet)
	if err != nil {
		return fmt.Errorf("invalid yt config: %v", err)
	}
	ytServerClickHouseConfig := map[string]interface{}{
		"clickhouse":         clickhouseConfig,
		"yt":                 ytConfig,
		"cpu_limit":          r.InstanceCPU,
		"memory":             r.InstanceMemory.memoryConfig(),
		"cluster_connection": c.clusterConnection,
		"profile_manager": map[string]interface{}{
			"global_tags": map[string]interface{}{
				"operation_alias": alias,
				"cookie":          "$YT_JOB_COOKIE",
			},
		},
		"logging": map[string]interface{}{
			"writers": map[string]interface{}{
				"error": map[string]interface{}{
					"file_name": "./clickhouse.error.log",
					"type":      "file",
				},
				"stderr": map[string]interface{}{
					"type": "stderr",
				},
				"debug": map[string]interface{}{
					"file_name": "./clickhouse.debug.log",
					"type":      "file",
				},
				"info": map[string]interface{}{
					"file_name": "./clickhouse.log",
					"type":      "file",
				},
			},
			"suppressed_messages": [2]string{
				"Reinstall peer",
				"Pass started",
			},
			"rules": [3](map[string]interface{}){
				{
					"min_level": "trace",
					"writers": [1]string{
						"debug",
					},
					"exclude_categories": [2]string{
						"Concurrency",
						"Bus",
					},
				},
				{
					"min_level": "info",
					"writers": [1]string{
						"info",
					},
				},
				{
					"min_level": "error",
					"writers": [2]string{
						"stderr",
						"error",
					},
				},
			},
		},
	}
	ytServerClickHouseConfigPath, err := c.uploadConfig(ctx, alias, "config.yson", ytServerClickHouseConfig)
	if err != nil {
		return
	}

	logTailerConfig := map[string]interface{}{
		"profile_manager": map[string]interface{}{
			"global_tags": map[string]interface{}{
				"operation_alias": alias,
				"cookie":          "$YT_JOB_COOKIE",
			},
		},
		"cluster_connection": c.clusterConnection,
		"log_tailer": map[string]interface{}{
			"log_rotation": map[string]interface{}{
				"enable":            true,
				"rotation_delay":    15000,
				"log_segment_count": 100,
				"rotation_period":   900000,
			},
			"log_files": [2](map[string]interface{}){
				{
					"ttl":  604800000,
					"path": "clickhouse.debug.log",
				},
				{
					"ttl":  604800000,
					"path": "clickhouse.log",
				},
			},
			"log_writer_liveness_checker": map[string]interface{}{
				"enable":                true,
				"liveness_check_period": 5000,
			},
		},
		"logging": map[string]interface{}{
			"writers": map[string]interface{}{
				"error": map[string]interface{}{
					"file_name": "./log_tailer.error.log",
					"type":      "file",
				},
				"stderr": map[string]interface{}{
					"type": "stderr",
				},
				"debug": map[string]interface{}{
					"file_name": "./log_tailer.debug.log",
					"type":      "file",
				},
				"info": map[string]interface{}{
					"file_name": "./log_tailer.log",
					"type":      "file",
				},
			},
			"rules": [3](map[string]interface{}){
				{
					"min_level": "trace",
					"writers": [1]string{
						"debug",
					},
				},
				{
					"min_level": "info",
					"writers": [1]string{
						"info",
					},
				},
				{
					"min_level": "error",
					"writers": [2]string{
						"error",
						"stderr",
					},
				},
			},
		},
	}
	logTailerConfigPath, err := c.uploadConfig(ctx, alias, "log_tailer_config.yson", logTailerConfig)
	if err != nil {
		return
	}
	*filePaths = append(*filePaths, ytServerClickHouseConfigPath, logTailerConfigPath)

	return
}
