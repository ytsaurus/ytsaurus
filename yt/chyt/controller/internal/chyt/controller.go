package chyt

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

type Controller struct {
	ytc               yt.Client
	l                 log.Logger
	clusterConnection map[string]interface{}
	root              ypath.Path
	cluster           string
	tvmSecret         string
}

func (c *Controller) prepareTvmSecret() {
	clusterName := strings.ReplaceAll(strings.ToUpper(c.cluster), "-", "_")
	envVar := "CHYT_TVM_SECRET_" + clusterName
	secret, ok := os.LookupEnv(envVar)
	if !ok {
		return
	}
	c.tvmSecret = strings.TrimSpace(secret)
}

func (c *Controller) getTvmID() (int64, bool) {
	rawTvmID, ok := c.clusterConnection["tvm_id"]
	if !ok {
		return 0, false
	}
	tvmID, ok := rawTvmID.(int64)
	if !ok {
		c.l.Warn("tvm id must have int64 type")
		return 0, false
	}
	return tvmID, true
}

func (c *Controller) getClusterConnection(ctx context.Context) (
	clusterConnection map[string]interface{}, err error) {
	err = c.ytc.GetNode(ctx, ypath.Path("//sys/@cluster_connection"), &clusterConnection, nil)
	if err != nil {
		return
	}
	if _, ok := c.clusterConnection["block_cache"]; ok {
		err = fmt.Errorf("chyt: cluster connection contains block_cache section; looks like a misconfiguration")
		return
	}
	return
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]interface{}, description map[string]interface{}, annotations map[string]interface{}, err error) {
	alias := oplet.Alias()

	description = buildDescription(c.cluster, alias)
	speclet := oplet.ControllerSpeclet().(Speclet)

	var filePaths []ypath.Rich

	// Populate resources.
	err = c.populateResources(&speclet)
	if err != nil {
		return
	}

	// Build artifacts.
	err = c.appendArtifacts(ctx, &speclet, &filePaths, &description)
	if err != nil {
		return
	}

	// Build configs.
	err = c.appendConfigs(ctx, oplet, &speclet, &filePaths)
	if err != nil {
		return
	}

	// Prepare runtime stuff: stderr/core-table, etc.
	runtimePaths, err := c.prepareRuntime(ctx, speclet.RuntimeDataPathOrDefault().Child(alias), alias, oplet.NextIncarnationIndex())
	if err != nil {
		return
	}

	// Build command.
	var args []string
	args = append(args, "./clickhouse-trampoline", "./ytserver-clickhouse")
	args = append(args, "--monitoring-port", "10142", "--log-tailer-monitoring-port", "10242")
	if speclet.EnableGeoDataOrDefault() {
		args = append(args, "--prepare-geodata")
	}
	args = append(args, "--log-tailer-bin", "./ytserver-log-tailer")
	command := strings.Join(args, " ")

	spec = map[string]interface{}{
		"tasks": map[string]interface{}{
			"instances": map[string]interface{}{
				"command":                            command,
				"job_count":                          speclet.Resources.InstanceCount,
				"file_paths":                         filePaths,
				"memory_limit":                       speclet.Resources.InstanceMemory.totalMemory(),
				"cpu_limit":                          speclet.Resources.InstanceCPU,
				"port_count":                         5,
				"max_stderr_size":                    1024 * 1024 * 1024,
				"user_job_memory_digest_lower_bound": 1.0,
				"restart_completed_jobs":             true,
				"interruption_signal":                "SIGINT",
			},
		},
		"max_failed_job_count": 10 * 1000,
		"max_stderr_count":     150,
		"stderr_table_path":    runtimePaths.StderrTable,
		"core_table_path":      runtimePaths.CoreTable,
		"title":                "CHYT clique *" + alias,
	}
	annotations = map[string]interface{}{
		"is_clique": true,
		"expose":    true,
	}

	if c.tvmSecret != "" {
		spec["secure_vault"] = map[string]interface{}{
			"TVM_SECRET": c.tvmSecret,
		}
	}

	return
}

func (c *Controller) Family() string {
	return "chyt"
}

func (c *Controller) ParseSpeclet(specletYson yson.RawValue) (any, error) {
	var speclet Speclet
	err := yson.Unmarshal(specletYson, &speclet)
	if err != nil {
		return nil, yterrors.Err("failed to parse speclet", err)
	}
	return speclet, nil
}

func (c *Controller) TryUpdate() (bool, error) {
	newClusterConnection, err := c.getClusterConnection(context.Background())
	if err != nil {
		c.l.Error("error getting new cluster connection", log.Error(err))
		return false, err
	}
	if reflect.DeepEqual(c.clusterConnection, newClusterConnection) {
		c.l.Debug("cluster connection is the same")
		return false, nil
	}
	c.clusterConnection = newClusterConnection
	c.l.Debug("changed cluster connection")
	return true, nil
}

func NewController(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, config yson.RawValue) strawberry.Controller {
	c := &Controller{
		l:       l,
		ytc:     ytc,
		root:    root,
		cluster: cluster,
	}
	clusterConnection, err := c.getClusterConnection(context.Background())
	if err != nil {
		panic(err)
	}
	c.clusterConnection = clusterConnection
	c.prepareTvmSecret()
	return c
}
