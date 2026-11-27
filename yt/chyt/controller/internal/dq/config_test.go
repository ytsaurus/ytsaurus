package dq

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestOpaqueConfigAndTask(t *testing.T) {
	c := &Controller{cluster: "pythia"}
	parsed, err := c.ParseSpeclet(yson.RawValue(`{
        future_section={x="opaque";}; instance_count=5; cpu_limit=10; memory_limit=17179869184;
        backend={worker_capacity=10; new_setting={x=17;};use_local_job_proxy_for_table_reads=%true;};
        scheduler={keep_reserve_for_literal_requests=%false;future_setting=100;};
        service={file_cache_dir="cache";grpc_port=31003;}; use_tmpfs=%true;tmpfs_size=4294967296;
        extra_env={YQL_DETERMINISTIC_MODE="1";};
    }`))
	require.NoError(t, err)
	speclet := parsed.(Speclet)
	content, err := c.buildServiceConfig(speclet, "clique")
	require.NoError(t, err)
	var config map[string]any
	require.NoError(t, yson.Unmarshal(content, &config))
	require.Equal(t, int64(17), config["backend"].(map[string]any)["new_setting"].(map[string]any)["x"])
	require.Equal(t, int64(100), config["scheduler"].(map[string]any)["future_setting"])
	require.Equal(t, "clique", config["service"].(map[string]any)["clique_alias"])
	require.Equal(t, "opaque", config["future_section"].(map[string]any)["x"])
	task := c.buildTaskSpec(speclet, strawberry.Speclet{}, "clique", 1, "//bin/dq_service_process", []ypath.Rich{{Path: "//config", FileName: serviceConfigFile}})
	require.Equal(t, true, task["enable_rpc_proxy_in_job_proxy"])
	require.Equal(t, "cache", task["tmpfs_path"])
	require.Equal(t, uint64(10), task["cpu_limit"])
	require.Equal(t, uint64(17179869184), task["memory_limit"])
	require.Equal(t, map[string]string{"ACTOR_PORT": "31002", "ACTOR_NODE_ID": "1", "OPERATION_SIZE": "5", "YQL_DETERMINISTIC_MODE": "1"}, task["environment"])
}

func TestContainerCPULimitTaskOption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config string
		want   any
	}{
		{"unset", `{}`, nil},
		{"enabled", `{set_container_cpu_limit=%true;}`, true},
		{"disabled", `{set_container_cpu_limit=%false;}`, false},
		{"backend_only", `{backend={container_cpu_limit=%true;};}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Controller{}
			parsed, err := c.ParseSpeclet(yson.RawValue(tc.config))
			require.NoError(t, err)
			task := c.buildTaskSpec(parsed.(Speclet), strawberry.Speclet{}, "clique", 1, "//bin/dq_service_process", nil)
			if tc.want == nil {
				require.NotContains(t, task, "set_container_cpu_limit")
			} else {
				require.Equal(t, tc.want, task["set_container_cpu_limit"])
			}
		})
	}
}

func TestRejectsSecretsAndInvalidLauncherFields(t *testing.T) {
	for _, config := range []string{
		`{instance_count=0;}`,
		`{set_container_cpu_limit=1;}`,
		`{set_container_cpu_limit=1.0;}`,
		`{set_container_cpu_limit="true";}`,
		`{backend={use_local_job_proxy_for_table_reads="true";};}`,
		`{coordinator={token="secret";};}`,
		`{backend={token_file="/secret";};}`,
		`{yt_token_file="/secret";}`,
		`{secure_vault={YT_TOKEN="secret";};}`,
		`{backend={future_option=<token="secret";>{};};}`,
		`{disk_request={additional_spec_yson="{account=hidden;};";};}`,
		`{extra_env={YT_SECURE_VAULT_YT_TOKEN="secret";};}`,
		`{extra_env={ACTOR_NODE_ID="2";};}`,
		`{service={file_cache_dir=123;};}`,
	} {
		_, err := (&Controller{}).ParseSpeclet(yson.RawValue(config))
		require.Error(t, err, config)
	}
}

var _ strawberry.Controller = (*Controller)(nil)

type configWriter struct{ bytes.Buffer }

func (*configWriter) Close() error { return nil }

type artifactClient struct {
	yt.Client
	files map[ypath.YPath]*configWriter
}

func (*artifactClient) CreateNode(context.Context, ypath.YPath, yt.NodeType, *yt.CreateNodeOptions) (yt.NodeID, error) {
	return yt.NodeID{}, nil
}
func (c *artifactClient) WriteFile(_ context.Context, path ypath.YPath, _ *yt.WriteFileOptions) (io.WriteCloser, error) {
	writer := &configWriter{}
	c.files[path] = writer
	return writer, nil
}
func TestUploadsOneConfigArtifact(t *testing.T) {
	client := &artifactClient{files: map[ypath.YPath]*configWriter{}}
	c := &Controller{ytc: client, root: "//tmp/dq", cluster: "pythia"}
	parsed, err := c.ParseSpeclet(yson.RawValue(`{backend={worker_capacity=10;};scheduler={new_parameter=123;};}`))
	require.NoError(t, err)
	oplet := strawberry.NewOplet(strawberry.OpletOptions{Alias: "clique", Logger: &nop.Logger{}})
	files, err := c.appendOperationConfigs(context.Background(), oplet, parsed.(Speclet))
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, serviceConfigFile, files[0].FileName)
	require.Len(t, client.files, 1)
	var config map[string]any
	require.NoError(t, yson.Unmarshal(client.files[files[0].Path].Bytes(), &config))
	require.Equal(t, int64(123), config["scheduler"].(map[string]any)["new_parameter"])
}

func TestStrawberryControlsDoNotChangeTheDQConfig(t *testing.T) {
	c := &Controller{}
	base, err := c.ParseSpeclet(yson.RawValue(`{future_section={value=1;};}`))
	require.NoError(t, err)
	for _, option := range []string{
		`pool="new-pool"`, `resume_marker="new-marker"`, `active=%true`,
		`family="dq"`, `stage="test"`, `restart_on_speclet_change=%false`,
		`restart_on_controller_change=%false`, `min_speclet_revision=42`,
		`network_project="net"`, `pool_trees=["tree";]`,
		`disk_request={account="yql";disk_space=1024;}`,
	} {
		parsed, err := c.ParseSpeclet(yson.RawValue(`{future_section={value=1;};` + option + `;}`))
		require.NoError(t, err)
		require.Equal(t, base, parsed, option)
	}
	changed, err := c.ParseSpeclet(yson.RawValue(`{future_section={value=2;};}`))
	require.NoError(t, err)
	require.NotEqual(t, base, changed)
}

func TestDQOptionNamesMustUseSnakeCase(t *testing.T) {
	for _, tc := range []struct{ config, field string }{
		{`{Backend={};}`, "Backend"},
		{`{coordinator={User="alice";};}`, "coordinator.User"},
		{`{coordinator={ProxyAddress="custom";};}`, "coordinator.ProxyAddress"},
		{`{backend={UseLocalJobProxyForTableReads=%true;};}`, "backend.UseLocalJobProxyForTableReads"},
		{`{backend={UseLocalJobProxyForResourceDownloads=%true;};}`, "backend.UseLocalJobProxyForResourceDownloads"},
		{`{backend={worker__capacity=1;};}`, "backend.worker__capacity"},
		{`{scheduler={KeepReserveForLiteralRequests=%false;};}`, "scheduler.KeepReserveForLiteralRequests"},
		{`{service={GrpcPort=31003;};}`, "service.GrpcPort"},
	} {
		_, err := parseSpeclet(yson.RawValue(tc.config))
		require.ErrorContains(t, err, tc.field)
		require.ErrorContains(t, err, "snake_case")
	}
	_, err := parseSpeclet(yson.RawValue(`{
		extra_env={YQL_DETERMINISTIC_MODE="1";};
		documentation={DisplayName="opaque";};
		backend={new_lower_case_option={opaque="value";};};
	}`))
	require.NoError(t, err)
}

func TestSharedDiskRequestTaskOption(t *testing.T) {
	for _, tc := range []struct{ name, config string }{
		{"unset", `{}`},
		{"null", `{disk_request=#;}`},
		{"empty", `{disk_request={};}`},
		{"configured", `{disk_request={account="yql";disk_space=1024u;inode_count=10u;medium_name="ssd";};}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := yson.RawValue(tc.config)
			speclet, err := parseSpeclet(raw)
			require.NoError(t, err)
			var common strawberry.Speclet
			require.NoError(t, yson.Unmarshal(raw, &common))
			task := (&Controller{}).buildTaskSpec(speclet, common, "clique", 1, "//bin/dq_service_process", nil)
			if common.DiskRequest == nil {
				require.NotContains(t, task, "disk_request")
				return
			}
			encoded, err := yson.Marshal(task)
			require.NoError(t, err)
			var actual, expected map[string]any
			require.NoError(t, yson.Unmarshal(encoded, &actual))
			require.NoError(t, yson.Unmarshal(raw, &expected))
			require.Equal(t, expected["disk_request"], actual["disk_request"])
		})
	}
}
