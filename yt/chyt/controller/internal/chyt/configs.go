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

func getPatchedClickHouseConfig(oplet *strawberry.Oplet, speclet *Speclet) (config any, err error) {
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

func (c Controller) getPatchedYtConfig(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet) (config any, err error) {
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
	if _, ok := configAsMap["clique_instance_count"]; !ok {
		configAsMap["clique_instance_count"] = speclet.Resources.InstanceCount
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
		serverAddresses, err = getDiscoveryServerAddresses(ctx, c.ytc)
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

	if _, ok := configAsMap["user_defined_sql_objects_storage"]; !ok {
		configAsMap["user_defined_sql_objects_storage"] = make(map[string]any)
	}
	sqlUDFStorage, err := asMapNode(configAsMap["user_defined_sql_objects_storage"])
	if err != nil {
		err = fmt.Errorf("invalid user_defined_sql_objects_storage config: %v", err)
		return
	}
	if _, ok := sqlUDFStorage["path"]; ok {
		err = fmt.Errorf("path in user_defined_sql_objects_storage config cannot be set by user")
		return
	} else {
		sqlUDFStorage["path"] = c.sqlUDFDir(oplet.Alias())
	}
	if _, ok := sqlUDFStorage["enabled"]; !ok {
		sqlUDFStorage["enabled"] = true
	}

	return
}

func getPatchedBuiltinLogRotationPolicy(speclet *Speclet) (map[string]any, error) {
	configNode, err := cloneNode(speclet.BuiltinLogRotationPolicy)
	if err != nil {
		return nil, err
	}
	config, ok := configNode.(map[string]any)
	if !ok && configNode != nil {
		return nil, yterrors.Err("builtin_log_rotation_policy config has unexpected type",
			log.String("type", reflect.TypeOf(configNode).String()))
	}
	if config == nil {
		config = map[string]any{}
	}
	if _, ok := config["max_segment_count_to_keep"]; !ok {
		config["max_segment_count_to_keep"] = 10
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

func (c Controller) sqlUDFDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("user_defined_sql_functions")
}

func (c *Controller) createOpaqueDirIfNotExists(ctx context.Context, dir ypath.Path) error {
	_, err := c.ytc.CreateNode(ctx, dir, yt.NodeMap,
		&yt.CreateNodeOptions{
			IgnoreExisting: true,
			Attributes: map[string]any{
				"opaque": true,
			},
		})
	return err
}

func (c *Controller) appendConfigs(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet, filePaths *[]ypath.Rich) error {
	r := speclet.Resources

	clickhouseConfig, err := getPatchedClickHouseConfig(oplet, speclet)
	if err != nil {
		return fmt.Errorf("invalid clickhouse config: %v", err)
	}
	ytConfig, err := c.getPatchedYtConfig(ctx, oplet, speclet)
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

	err = c.createOpaqueDirIfNotExists(ctx, c.artifactDir(oplet.Alias()))
	if err != nil {
		return fmt.Errorf("error creating artifact dir: %v", err)
	}

	if err := c.createOpaqueDirIfNotExists(ctx, c.sqlUDFDir(oplet.Alias())); err != nil {
		return fmt.Errorf("error creating sql udf dir: %v", err)
	}

	var logRotationPolicy map[string]any
	if c.config.LogRotationModeOrDefault() == LogRotationModeBuiltin {
		logRotationPolicy, err = getPatchedBuiltinLogRotationPolicy(speclet)
		if err != nil {
			return err
		}
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
			"rotation_check_period": 60000,
			"writers": map[string]any{
				"error": map[string]any{
					"file_name":       "./clickhouse.error.log",
					"type":            "file",
					"rotation_policy": logRotationPolicy,
				},
				"stderr": map[string]any{
					"type": "stderr",
				},
				"debug": map[string]any{
					"file_name":       "./clickhouse.debug.log",
					"type":            "file",
					"rotation_policy": logRotationPolicy,
				},
				"info": map[string]any{
					"file_name":       "./clickhouse.log",
					"type":            "file",
					"rotation_policy": logRotationPolicy,
				},
			},
			"suppressed_messages": []string{
				"Reinstall peer",
				"Pass started",
			},
			"rules": [](map[string]any){
				{
					"min_level": "trace",
					"writers": []string{
						"debug",
					},
					"exclude_categories": []string{
						"Concurrency",
						"Bus",
						"ReaderMemoryManager",
						"RpcClient",
						"ChunkClient",
					},
				},
				{
					"min_level": "info",
					"writers": []string{
						"info",
					},
					"exclude_categories": []string{
						"ChunkClient",
					},
				},
				{
					"min_level": "error",
					"writers": []string{
						"stderr",
						"error",
					},
					"exclude_categories": []string{
						"ChunkClient",
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
		return err
	}
	*filePaths = append(*filePaths, ytServerClickHouseConfigPath)

	return nil
}
