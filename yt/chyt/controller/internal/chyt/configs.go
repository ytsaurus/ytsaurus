package chyt

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"strings"

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

type configAliases map[string]configAlias

type configAlias struct {
	aliases  []string
	children configAliases
}

func aliases(names ...string) configAlias {
	return configAlias{aliases: names}
}

func normalizeConfigAliases(configMapNode map[string]any, configWithAliases configAliases) (map[string]any, error) {
	config, err := cloneNode(configMapNode)
	if err != nil {
		return nil, err
	}
	if config == nil {
		config = make(map[string]any)
	}
	configMapNode, err = asMapNode(config)
	if err != nil {
		return nil, err
	}

	for key, fieldAliases := range configWithAliases {
		for _, alias := range fieldAliases.aliases {
			value, ok := configMapNode[alias]
			if !ok {
				continue
			}
			if _, ok := configMapNode[key]; ok {
				return nil, fmt.Errorf("config field %q is specified more than once (alias %q)", key, alias)
			}
			configMapNode[key] = value
			delete(configMapNode, alias)
		}
		if child, ok := configMapNode[key].(map[string]any); ok && len(fieldAliases.children) > 0 {
			child, err = normalizeConfigAliases(child, fieldAliases.children)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			configMapNode[key] = child
		}
	}
	return configMapNode, nil
}

func pruneSpecletConfig(configMapNode, exclusionMapNode map[string]any) bool {
	if len(exclusionMapNode) == 0 {
		return true
	}

	for key, val := range exclusionMapNode {
		configVal, ok := configMapNode[key]
		if !ok {
			continue
		}

		// It's fine if the cast doesn't succeed; the recursive calls will handle it.
		exclusionValMap, _ := val.(map[string]any)
		configValMap, _ := configVal.(map[string]any)
		if pruneSpecletConfig(configValMap, exclusionValMap) {
			delete(configMapNode, key)
		}
	}

	return false
}

// Add missing keys from defaultsMapNode to configMapNode.
func applyConfigDefaults(configMapNode, defaultsMapNode map[string]any) {
	for key, defaultVal := range defaultsMapNode {
		configVal, ok := configMapNode[key]
		if !ok {
			configMapNode[key] = defaultVal
			continue
		}

		configValMap, configValIsMap := configVal.(map[string]any)
		defaultValMap, defaultValIsMap := defaultVal.(map[string]any)
		if configValIsMap && defaultValIsMap {
			applyConfigDefaults(configValMap, defaultValMap)
		}
	}
}

func (c *Controller) getPatchedClickHouseConfig(oplet *strawberry.Oplet, speclet *Speclet) (config any, err error) {
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

	if len(c.config.SpecletConfigExclusionTree) > 0 {
		ytCfgExclusionVal := c.config.SpecletConfigExclusionTree["clickhouse"]
		ytCfgExclusionMapNode, _ := ytCfgExclusionVal.(map[string]any)
		pruneSpecletConfig(configAsMap, ytCfgExclusionMapNode)
	}

	if c.config.DefaultSpeclet != nil {
		applyConfigDefaults(configAsMap, c.config.DefaultSpeclet.ClickHouseConfig)
	}

	if _, ok := configAsMap["path_to_regions_names_files"]; !ok {
		configAsMap["path_to_regions_names_files"] = "./geodata/"
	}

	if _, ok := configAsMap["path_to_regions_hierarchy_file"]; !ok {
		configAsMap["path_to_regions_hierarchy_file"] = "./geodata/regions_hierarchy.txt"
	}

	if speclet.ODBCConfig.EnableOrDefault() {
		if _, ok := configAsMap["application"]; !ok {
			// ClickHouse ODBC bridge is launched by the ClickHouse server.
			// The "application.dir" setting tells ClickHouse where to look for it.
			// In a YT job sandbox all binaries land in the working directory, so we point
			// ClickHouse there.
			configAsMap["application"] = map[string]any{
				"dir": "./",
			}
		}
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

// COMPAT(buyval01): migrate speclet configs and remove alias
var ytConfigAliases = configAliases{
	"query_settings":    aliases("settings"),
	"object_repository": aliases("dictionary_repository"),
}

func (c *Controller) getPatchedYtConfig(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet) (config any, err error) {
	configAsMap, err := normalizeConfigAliases(speclet.YTConfig, ytConfigAliases)
	if err != nil {
		return nil, err
	}
	config = configAsMap

	if len(c.config.SpecletConfigExclusionTree) > 0 {
		ytCfgExclusionVal := c.config.SpecletConfigExclusionTree["yt"]
		ytCfgExclusionMapNode, _ := ytCfgExclusionVal.(map[string]any)
		ytCfgExclusionMapNode, err = normalizeConfigAliases(ytCfgExclusionMapNode, ytConfigAliases)
		if err != nil {
			return nil, err
		}
		pruneSpecletConfig(configAsMap, ytCfgExclusionMapNode)
	}

	if c.config.DefaultSpeclet != nil {
		defaults, err := normalizeConfigAliases(c.config.DefaultSpeclet.YTConfig, ytConfigAliases)
		if err != nil {
			return nil, err
		}
		applyConfigDefaults(configAsMap, defaults)
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
	configAsMap["orchid_root"] = c.orchidDir(oplet.Alias())
	// TODO(max42): put to preprocessor similarly to yt/cpu_limit.
	if _, ok := configAsMap["worker_thread_count"]; !ok {
		configAsMap["worker_thread_count"] = *speclet.Resources.InstanceCPU
	}

	if _, ok := configAsMap["query_sticky_group_size"]; !ok && speclet.EnableStickyQueryDistribution {
		configAsMap["query_sticky_group_size"] = speclet.QueryStickyGroupSizeOrDefault()
	}

	{
		var discovery map[string]any

		if _, ok := configAsMap["discovery"]; !ok {
			configAsMap["discovery"] = make(map[string]any)
		}
		discovery, err = asMapNode(configAsMap["discovery"])
		if err != nil {
			err = fmt.Errorf("invalid discovery config: %v", err)
			return
		}

		if versionVal, ok := discovery["version"]; ok {
			version, ok := versionVal.(int64)
			if !ok || version != 2 {
				err = fmt.Errorf("expected discovery version 2, got %v(%T)", versionVal, versionVal)
				return
			}
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

	if objectConfig, ok := configAsMap["object_repository"]; ok {
		objectRepository, convertErr := asMapNode(objectConfig)
		if convertErr != nil {
			err = fmt.Errorf("invalid object_repository config: %v", convertErr)
			return
		}
		if _, ok := objectRepository["root_path"]; ok {
			err = fmt.Errorf("root_path in object_repository config cannot be set by user")
			return
		} else {
			objectRepository["root_path"] = c.storageArtifactsDir(oplet.Alias())
		}
	}

	const always_blocked_headers = "authentication|x-clickhouse-user"
	if val, ok := configAsMap["http_header_blacklist"]; !ok {
		configAsMap["http_header_blacklist"] = always_blocked_headers
	} else if !strings.HasPrefix(val.(string), always_blocked_headers) {
		configAsMap["http_header_blacklist"] = always_blocked_headers + "|(" + strings.ToLower(val.(string)) + ")"
	}
	if _, ok := configAsMap["enable_http_header_log"]; !ok {
		configAsMap["enable_http_header_log"] = true
	}
	if _, ok := configAsMap["table_attribute_cache"]; !ok {
		configAsMap["table_attribute_cache"] = make(map[string]any)
	}
	tableAttributeCache, err := asMapNode(configAsMap["table_attribute_cache"])
	if err != nil {
		err = fmt.Errorf("invalid table_attribute_cache config: %v", err)
		return
	}
	if _, ok := tableAttributeCache["master_read_options"]; !ok {
		tableAttributeCache["master_read_options"] = make(map[string]any)
	}
	masterReadOptions, err := asMapNode(tableAttributeCache["master_read_options"])
	if err != nil {
		err = fmt.Errorf("invalid master_read_options config: %v", err)
		return
	}
	if _, ok := masterReadOptions["read_from"]; !ok {
		masterReadOptions["read_from"] = "cache"
	}
	if _, ok := configAsMap["system_log_table_exporters"]; !ok {
		configAsMap["system_log_table_exporters"] = make(map[string]any)
	}
	systemLogTableExporters, err := asMapNode(configAsMap["system_log_table_exporters"])
	if err != nil {
		err = fmt.Errorf("invalid system_log_table_exporters config: %v", err)
		return
	}
	systemLogTableExporters["cypress_root_directory"] = c.systemLogTableRootDir(oplet.Alias())
	if _, ok := systemLogTableExporters["default"]; !ok {
		systemLogTableExporters["default"] = make(map[string]any)
	}
	exporterDefaultConfig, err := asMapNode(systemLogTableExporters["default"])
	if err != nil {
		err = fmt.Errorf("invalid default system log table exporter config: %v", err)
		return
	}
	exporterDefaultConfig["enabled"] = speclet.ExportSystemLogTablesOrDefault(c.config.ExportSystemLogTablesOrDefault())

	return
}

func getPatchedBuiltinLogRotationPolicy(speclet *Speclet) (map[string]any, error) {
	configNode, err := cloneNode(speclet.BuiltinLogRotationPolicy)
	if err != nil {
		return nil, err
	}
	config, err := asMapNode(configNode)
	if err != nil {
		return nil, err
	}

	if config == nil {
		config = map[string]any{}
	}

	defaultConfig := map[string]any{
		"max_segment_count_to_keep": 10,
		"rotation_period":           900000,
	}

	for key, value := range defaultConfig {
		if _, ok := config[key]; !ok {
			config[key] = value
		}
	}

	return config, nil
}

func (c *Controller) getLoggingConfig(configNode any, speclet *Speclet) (map[string]any, error) {
	if configNode == nil {
		configNode = map[string]any{}
	}
	configNodeClone, err := cloneNode(configNode)
	if err != nil {
		return nil, err
	}
	config, err := asMapNode(configNodeClone)
	if err != nil {
		return nil, err
	}

	var logRotationPolicy map[string]any
	if c.config.LogRotationModeOrDefault() == LogRotationModeBuiltin {
		var err error
		logRotationPolicy, err = getPatchedBuiltinLogRotationPolicy(speclet)
		if err != nil {
			return nil, err
		}
	}

	errorLogFile := defaultErrorLogFile
	debugLogFile := defaultDebugLogFile
	infoLogFile := defaultInfoLogFile
	if speclet.logsDir != nil {
		errorLogFile = filepath.Join(*speclet.logsDir, "clickhouse-$YT_JOB_INDEX.error.log")
		debugLogFile = filepath.Join(*speclet.logsDir, "clickhouse-$YT_JOB_INDEX.debug.log")
		infoLogFile = filepath.Join(*speclet.logsDir, "clickhouse-$YT_JOB_INDEX.log")
	}

	defaultConfig := map[string]any{
		"rotation_check_period": 60000,
		"writers": map[string]any{
			"error": map[string]any{
				"file_name":       errorLogFile,
				"type":            "file",
				"rotation_policy": logRotationPolicy,
			},
			"stderr": map[string]any{
				"type": "stderr",
			},
			"debug": map[string]any{
				"file_name":       debugLogFile,
				"type":            "file",
				"rotation_policy": logRotationPolicy,
			},
			"info": map[string]any{
				"file_name":       infoLogFile,
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
	}

	for key, value := range defaultConfig {
		if _, ok := config[key]; !ok {
			config[key] = value
		}
	}

	return config, nil
}

func (c *Controller) getSolomonExporterConfig(configNode any, opletAlias string) (map[string]any, error) {
	if configNode == nil {
		configNode = map[string]any{}
	}
	configNodeCopy, err := cloneNode(configNode)
	if err != nil {
		return nil, err
	}
	config, err := asMapNode(configNodeCopy)
	if err != nil {
		return nil, err
	}

	defaultConfig := map[string]any{
		// NOTE(dakovalkov): override host, otherwise metric count will bloat.
		"host": "",
		"instance_tags": map[string]any{
			"operation_alias": opletAlias,
			"cookie":          "$YT_JOB_COOKIE",
		},
		"window_size": 72,
	}

	for key, value := range defaultConfig {
		if _, ok := config[key]; !ok {
			config[key] = value
		}
	}

	return config, nil
}

func (c *Controller) uploadFile(ctx context.Context, alias string, filename string, data []byte) (richPath ypath.Rich, err error) {
	path := c.artifactDir(alias).Child(filename)
	_, err = c.ytc.CreateNode(ctx, path, yt.NodeFile, &yt.CreateNodeOptions{IgnoreExisting: true})
	if err != nil {
		return
	}
	w, err := c.ytc.WriteFile(ctx, path, nil)
	if err != nil {
		return
	}
	_, err = w.Write(data)
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

func (c *Controller) uploadYsonFile(ctx context.Context, alias string, filename string, config any) (ypath.Rich, error) {
	configYson, err := yson.MarshalFormat(config, yson.FormatPretty)
	if err != nil {
		return ypath.Rich{}, err
	}
	return c.uploadFile(ctx, alias, filename, configYson)
}

func (c *Controller) artifactDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("artifacts")
}

func (c *Controller) sqlUDFDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("user_defined_sql_functions")
}

func (c *Controller) orchidDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("orchids")
}

func (c *Controller) systemLogTableRootDir(alias string) ypath.Path {
	return c.artifactDir(alias).Child("system_log_tables")
}

func (c *Controller) storageArtifactsDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("storage_artifacts")
}

const (
	defaultErrorLogFile = "./clickhouse.error.log"
	defaultDebugLogFile = "./clickhouse.debug.log"
	defaultInfoLogFile  = "./clickhouse.log"
)

func (c *Controller) getServerConfig(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet) (map[string]any, error) {
	clickhouseConfig, err := c.getPatchedClickHouseConfig(oplet, speclet)
	if err != nil {
		return nil, fmt.Errorf("invalid clickhouse config: %v", err)
	}
	ytConfig, err := c.getPatchedYtConfig(ctx, oplet, speclet)
	if err != nil {
		return nil, fmt.Errorf("invalid yt config: %v", err)
	}

	serverConfig := maps.Clone(c.config.ServerConfig)
	if serverConfig == nil {
		serverConfig = make(map[string]any)
	}

	serverConfig["clickhouse"] = clickhouseConfig
	serverConfig["yt"] = ytConfig
	serverConfig["cpu_limit"] = speclet.Resources.InstanceCPU
	serverConfig["memory"] = speclet.Resources.InstanceMemory.memoryConfig()
	serverConfig["cluster_connection"] = c.cachedClusterConnection

	serverConfig["logging"], err = c.getLoggingConfig(serverConfig["logging"], speclet)
	if err != nil {
		return nil, err
	}

	serverConfig["solomon_exporter"], err = c.getSolomonExporterConfig(serverConfig["solomon_exporter"], oplet.Alias())
	if err != nil {
		return nil, err
	}

	{
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

		if nativeAuthenticatorConfig != nil {
			serverConfig["native_authentication_manager"] = nativeAuthenticatorConfig
		}
	}
	// Do not override the default value.
	if mode := speclet.ClusterConnectionDynamicConfigModeOrDefault(c.config.DefaultClusterConnectionDynamicConfigModeOrDefault()); mode != ClusterConnectionFromStaticConfig {
		serverConfig["cluster_connection_dynamic_config_mode"] = mode
	} else {
		delete(serverConfig, "cluster_connection_dynamic_config_mode")
	}
	if c.config.AddressResolver != nil {
		serverConfig["address_resolver"] = c.config.AddressResolver
	}
	if c.config.BusServer != nil {
		serverConfig["bus_server"] = c.config.BusServer
	}

	return serverConfig, nil
}

func (c *Controller) appendConfigs(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet, filePaths *[]ypath.Rich) error {
	serverConfig, err := c.getServerConfig(ctx, oplet, speclet)
	if err != nil {
		return err
	}
	versionedConfigFilename := fmt.Sprintf("config-%d.yson", oplet.NextIncarnationIndex())
	ytServerClickHouseConfigPath, err := c.uploadYsonFile(ctx, oplet.Alias(), versionedConfigFilename, serverConfig)
	if err != nil {
		return err
	}
	runtimeSpec := speclet.RuntimeDataSpecOrDefault(c.root.Child(oplet.Alias()))
	if err := c.ytc.SetNode(ctx, ytServerClickHouseConfigPath.Path.Attr("expiration_timeout"), runtimeSpec.RuntimeDataExpirationTimeout, nil); err != nil {
		return err
	}
	latestConfigPath := c.artifactDir(oplet.Alias()).Child("config.yson")
	_, err = c.ytc.LinkNode(ctx, ytServerClickHouseConfigPath.Path, latestConfigPath, &yt.LinkNodeOptions{Force: true})
	if err != nil {
		return err
	}
	ytServerClickHouseConfigPath.Path = latestConfigPath
	ytServerClickHouseConfigPath.FileName = "config.yson"
	*filePaths = append(*filePaths, ytServerClickHouseConfigPath)

	return nil
}
