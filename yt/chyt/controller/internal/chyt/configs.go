package chyt

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func cloneNode(ysonNode any) (ysonNodeCopy any, err error) {
	ysonString, err := yson.Marshal(ysonNode)
	if err != nil {
		return
	}
	err = yson.Unmarshal(ysonString, &ysonNodeCopy)
	return
}

func asMapNode(ysonNode any) (asMap map[string]any, err error) {
	asMap, ok := yson.ValueOf(ysonNode).(map[string]any)
	if !ok {
		err = errors.New("yson node type is not 'Map'")
	}
	return
}

func getPatchedClickHouseConfig(speclet *Speclet) (config any, err error) {
	config, err = cloneNode(speclet.ClickHouseConfig)
	if err != nil {
		return
	}
	if config == nil {
		config = make(map[string]any)
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
		configAsMap["settings"] = make(map[string]any)
	}
	settings, err := asMapNode(configAsMap["settings"])
	if err != nil {
		err = fmt.Errorf("invalid settings config: %v", err)
		return
	}
	if speclet.QuerySettings != nil {
		for name, value := range speclet.QuerySettings {
			settings[name] = value
		}
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

func getDiscoveryServerAddresses(ctx context.Context, ytc yt.Client) (addresses []string, err error) {
	err = ytc.ListNode(ctx, ypath.Path("//sys/discovery_servers"), &addresses, nil)
	if yterrors.ContainsResolveError(err) {
		err = nil
	}
	return
}

func getPatchedYtConfig(ctx context.Context, ytc yt.Client, oplet *strawberry.Oplet, speclet *Speclet) (config any, err error) {
	config, err = cloneNode(speclet.YTConfig)
	if err != nil {
		return
	}
	if config == nil {
		config = make(map[string]any)
	}
	configAsMap, err := asMapNode(config)
	if err != nil {
		return
	}
	if _, ok := configAsMap["clique_alias"]; !ok {
		configAsMap["clique_alias"] = oplet.Alias()
	}
	if _, ok := configAsMap["clique_incarnation"]; !ok {
		configAsMap["clique_incarnation"] = oplet.NextIncarnationIndex()
	}
	// TODO(max42): put to preprocessor similarly to yt/cpu_limit.
	if _, ok := configAsMap["worker_thread_count"]; !ok {
		configAsMap["worker_thread_count"] = *speclet.Resources.InstanceCPU
	}

	if _, ok := configAsMap["enable_dynamic_tables"]; !ok {
		configAsMap["enable_dynamic_tables"] = true
	}

	if _, ok := configAsMap["discovery"]; !ok {
		configAsMap["discovery"] = make(map[string]any)
	}
	discovery, err := asMapNode(configAsMap["discovery"])
	if err != nil {
		err = fmt.Errorf("invalid discovery config: %v", err)
		return
	}
	if _, ok := discovery["version"]; !ok {
		discovery["version"] = 2
		discovery["read_quorum"] = 1
		discovery["write_quorum"] = 1
	}
	if _, ok := discovery["transaction_timeout"]; !ok {
		discovery["transaction_timeout"] = 30 * 1000
	}
	if _, ok := discovery["server_addresses"]; !ok {
		var serverAddresses []string
		serverAddresses, err = getDiscoveryServerAddresses(ctx, ytc)
		if err != nil {
			serverAddresses = []string{}
		}
		discovery["server_addresses"] = serverAddresses
	}

	if _, ok := configAsMap["health_checker"]; !ok {
		configAsMap["health_checker"] = make(map[string]any)
	}
	healthChecker, err := asMapNode(configAsMap["health_checker"])
	if err != nil {
		err = fmt.Errorf("invalid health_checker config: %v", err)
		return
	}
	if _, ok := healthChecker["queries"]; !ok {
		healthChecker["queries"] = [1]string{"select * from `//sys/clickhouse/sample_table`"}
	}
	if _, ok := healthChecker["period"]; !ok {
		healthChecker["period"] = 60 * 1000
	}

	return
}

func getPatchedLogRotationConfig(speclet *Speclet) (map[string]any, error) {
	configNode, err := cloneNode(speclet.LogRotation)
	if err != nil {
		return nil, err
	}
	config, ok := configNode.(map[string]any)
	if !ok && configNode != nil {
		return nil, yterrors.Err("log_rotation config has unexpected type",
			log.String("type", reflect.TypeOf(configNode).String()))
	}
	if config == nil {
		config = map[string]any{}
	}
	if _, ok := config["enable"]; !ok {
		config["enable"] = true
	}
	if _, ok := config["rotation_delay"]; !ok {
		config["rotation_delay"] = 15000
	}
	if _, ok := config["log_segment_count"]; !ok {
		config["log_segment_count"] = 10
	}
	if _, ok := config["rotation_period"]; !ok {
		config["rotation_period"] = 900000
	}
	return config, nil
}

func (c *Controller) uploadConfig(ctx context.Context, alias string, filename string, config any) (richPath ypath.Rich, err error) {
	configYson, err := yson.MarshalFormat(config, yson.FormatPretty)
	if err != nil {
		return
	}
	path := c.artifactDir(alias).Child(filename)
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

func (c Controller) artifactDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("artifacts")
}

func (c *Controller) createArtifactDirIfNotExists(ctx context.Context, alias string) error {
	_, err := c.ytc.CreateNode(ctx, c.artifactDir(alias), yt.NodeMap,
		&yt.CreateNodeOptions{
			IgnoreExisting: true,
			Attributes: map[string]any{
				"opaque": true,
			},
		})
	return err
}

func (c *Controller) appendConfigs(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet, filePaths *[]ypath.Rich) (err error) {
	r := speclet.Resources

	clickhouseConfig, err := getPatchedClickHouseConfig(speclet)
	if err != nil {
		return fmt.Errorf("invalid clickhouse config: %v", err)
	}
	ytConfig, err := getPatchedYtConfig(ctx, c.ytc, oplet, speclet)
	if err != nil {
		return fmt.Errorf("invalid yt config: %v", err)
	}

	var nativeAuthenticatorConfig map[string]any
	if tvmID, ok := c.getTvmID(); ok {
		if c.tvmSecret != "" {
			nativeAuthenticatorConfig = map[string]any{
				"tvm_service": map[string]any{
					"enable_mock":                           false,
					"enable_ticket_parse_cache":             true,
					"client_self_id":                        tvmID,
					"client_enable_service_ticket_fetching": true,
					"client_enable_service_ticket_checking": true,
				},
				"enable_validation": true,
			}
		} else {
			c.l.Warn("tvm id specified, but no tvm secret provided in env")
		}
	}

	err = c.createArtifactDirIfNotExists(ctx, oplet.Alias())
	if err != nil {
		return fmt.Errorf("error creating artifact dir: %v", err)
	}

	ytServerClickHouseConfig := map[string]any{
		"clickhouse":         clickhouseConfig,
		"yt":                 ytConfig,
		"cpu_limit":          r.InstanceCPU,
		"memory":             r.InstanceMemory.memoryConfig(),
		"cluster_connection": c.cachedClusterConnection,
		// TODO(dakovalkov): "profile_manager" is a compat for older CHYT versions.
		// Remove it when all cliques are 2.09+
		"profile_manager": map[string]any{
			"global_tags": map[string]any{
				"operation_alias": oplet.Alias(),
				"cookie":          "$YT_JOB_COOKIE",
			},
		},
		"solomon_exporter": map[string]any{
			// NOTE(dakovalkov): override host, otherwise metric count will bloat.
			"host": "",
			"instance_tags": map[string]any{
				"operation_alias": oplet.Alias(),
				"cookie":          "$YT_JOB_COOKIE",
			},
		},
		"logging": map[string]any{
			"writers": map[string]any{
				"error": map[string]any{
					"file_name": "./clickhouse.error.log",
					"type":      "file",
				},
				"stderr": map[string]any{
					"type": "stderr",
				},
				"debug": map[string]any{
					"file_name": "./clickhouse.debug.log",
					"type":      "file",
				},
				"info": map[string]any{
					"file_name": "./clickhouse.log",
					"type":      "file",
				},
			},
			"suppressed_messages": [2]string{
				"Reinstall peer",
				"Pass started",
			},
			"rules": [3](map[string]any){
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
	if nativeAuthenticatorConfig != nil {
		ytServerClickHouseConfig["native_authentication_manager"] = nativeAuthenticatorConfig
	}
	if c.config.AddressResolver != nil {
		ytServerClickHouseConfig["address_resolver"] = c.config.AddressResolver
	}
	ytServerClickHouseConfigPath, err := c.uploadConfig(ctx, oplet.Alias(), "config.yson", ytServerClickHouseConfig)
	if err != nil {
		return
	}

	logRotationConfig, err := getPatchedLogRotationConfig(speclet)
	if err != nil {
		return
	}
	logTailerConfig := map[string]any{
		// TODO(dakovalkov): "profile_manager" is a compat for older CHYT versions.
		// Remove it after updating log tailer to new version.
		"profile_manager": map[string]any{
			"global_tags": map[string]any{
				"operation_alias": oplet.Alias(),
				"cookie":          "$YT_JOB_COOKIE",
			},
		},
		"solomon_exporter": map[string]any{
			// NOTE(dakovalkov): override host, otherwise metric count will bloat.
			"host": "",
			"instance_tags": map[string]any{
				"operation_alias": oplet.Alias(),
				"cookie":          "$YT_JOB_COOKIE",
			},
		},
		"cluster_connection": c.cachedClusterConnection,
		"log_tailer": map[string]any{
			"log_rotation": logRotationConfig,
			"log_files": [2](map[string]any){
				{
					"path": "clickhouse.debug.log",
				},
				{
					"path": "clickhouse.log",
				},
			},
			"log_writer_liveness_checker": map[string]any{
				"enable":                true,
				"liveness_check_period": 5000,
			},
		},
		"logging": map[string]any{
			"writers": map[string]any{
				"error": map[string]any{
					"file_name": "./log_tailer.error.log",
					"type":      "file",
				},
				"stderr": map[string]any{
					"type": "stderr",
				},
				"debug": map[string]any{
					"file_name": "./log_tailer.debug.log",
					"type":      "file",
				},
				"info": map[string]any{
					"file_name": "./log_tailer.log",
					"type":      "file",
				},
			},
			"rules": [3](map[string]any){
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
	if nativeAuthenticatorConfig != nil {
		logTailerConfig["native_authentication_manager"] = nativeAuthenticatorConfig
	}
	logTailerConfigPath, err := c.uploadConfig(ctx, oplet.Alias(), "log_tailer_config.yson", logTailerConfig)
	if err != nil {
		return
	}
	*filePaths = append(*filePaths, ytServerClickHouseConfigPath, logTailerConfigPath)

	return
}
