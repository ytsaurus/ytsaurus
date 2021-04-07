package chyt

import (
	"context"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

func (c *Controller) uploadConfig(alias string, filename string, config interface{}) (richPath ypath.Rich, err error) {
	configYson, err := yson.MarshalFormat(config, yson.FormatPretty)
	if err != nil {
		return
	}
	path := c.root.Child(alias).Child(filename)
	_, err = c.ytc.CreateNode(context.TODO(), path, yt.NodeFile, &yt.CreateNodeOptions{IgnoreExisting: true})
	if err != nil {
		return
	}
	w, err := c.ytc.WriteFile(context.TODO(), path, nil)
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

func (c *Controller) appendConfigs(alias string, speclet *Speclet, filePaths *[]ypath.Rich) (err error) {
	r := speclet.Resources
	ytServerClickHouseConfig := map[string]interface{}{
		"clickhouse": map[string]interface{}{
			"settings": map[string]interface{}{
				"max_threads": *r.InstanceCPU,
				// TODO(max42): move to cpp config.
				"queue_max_wait_ms": 30 * 1000,
			},
		},
		"profile_manager": map[string]interface{}{
			"global_tags": map[string]interface{}{
				"operation_alias": alias,
			},
		},
		"cluster_connection": c.clusterConnection,
		"yt": map[string]interface{}{
			// TODO(max42): put to preprocessor similarly to yt/cpu_limit.
			"worker_thread_count": r.InstanceCPU,
		},
		"cpu_limit": r.InstanceCPU,
		"memory":    r.InstanceMemory.memoryConfig(),
	}
	ytServerClickHouseConfigPath, err := c.uploadConfig(alias, "config.yson", ytServerClickHouseConfig)
	if err != nil {
		return
	}

	logTailerConfig := map[string]interface{}{
		"profile_manager": map[string]interface{}{
			"global_tags": map[string]interface{}{
				"operation_alias": alias,
			},
		},
		"cluster_connection": c.clusterConnection,
	}
	logTailerConfigPath, err := c.uploadConfig(alias, "log_tailer_config.yson", logTailerConfig)
	if err != nil {
		return
	}
	*filePaths = append(*filePaths, ytServerClickHouseConfigPath, logTailerConfigPath)

	return
}
