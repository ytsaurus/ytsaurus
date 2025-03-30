package chyt

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestConfig(t *testing.T) {
	env, c := PrepareAPI(t)
	alias := "test1"
	r := c.MakePostRequest("create", api.RequestParams{
		Params: map[string]any{"alias": alias},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)
	r = c.MakePostRequest("start", api.RequestParams{
		Params: map[string]any{
			"alias":     alias,
			"untracked": true,
		},
	})
	require.Equal(t, http.StatusOK, r.StatusCode)

	var config map[string]any
	configPath := env.StrawberryRoot.Child(alias).Child("artifacts").Child("config.yson")
	reader, err := env.YT.ReadFile(env.Ctx, configPath, nil)
	defer func() { _ = reader.Close() }()
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	env.L.Info(string(content))
	require.NoError(t, err)
	err = yson.Unmarshal(content, &config)
	require.NoError(t, err)

	expectedClickhouseConfig := map[string]any{
		"path_to_regions_hierarchy_file": "./geodata/regions_hierarchy.txt",
		"path_to_regions_names_files":    "./geodata/",
		"settings": map[string]any{
			"max_concurrent_queries_for_user": int64(10000),
			"max_temporary_non_const_columns": int64(1234),
			"max_threads":                     uint64(16),
			"queue_max_wait_ms":               int64(30000),
		},
	}
	require.Equal(t, expectedClickhouseConfig, config["clickhouse"])

	expectedYtConfig := map[string]any{
		"clique_alias":          alias,
		"clique_incarnation":    int64(1),
		"clique_instance_count": uint64(1),
		"discovery": map[string]any{
			"read_quorum":         int64(1),
			"server_addresses":    []any{},
			"transaction_timeout": int64(30000),
			"version":             int64(2),
			"write_quorum":        int64(1),
		},
		"enable_dynamic_tables": true,
		"health_checker": map[string]any{
			"period": int64(60000),
			"queries": []any{
				"select * from `//sys/clickhouse/sample_table`",
			},
		},
		"http_header_blacklist": "authentication|x-clickhouse-user",
		"system_log_table_exporters": map[string]any{
			"cypress_root_directory": env.StrawberryRoot.Child(alias).Child("artifacts").Child("system_log_tables").String(),
			"default": map[string]any{
				"enabled": false,
			},
		},
		"table_attribute_cache": map[string]any{
			"master_read_options": map[string]any{
				"read_from": "cache",
			},
		},
		"user_defined_sql_objects_storage": map[string]any{
			"enabled": true,
			"path":    env.StrawberryRoot.Child(alias).Child("user_defined_sql_functions").String(),
		},
		"worker_thread_count": uint64(16),
	}
	require.Equal(t, expectedYtConfig, config["yt"])
}
