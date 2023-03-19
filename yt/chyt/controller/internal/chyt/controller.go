package chyt

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Config struct {
	// LocalBinariesDir is set if we want to execute local binaries on the clique.
	// This directory should contain trampoline, chyt and log-tailer binaries.
	LocalBinariesDir *string        `yson:"local_binaries_dir"`
	EnableLogTailer  *bool          `yson:"enable_log_tailer"`
	AddressResolver  map[string]any `yson:"address_resolver"`
}

const (
	DefaultEnableLogTailer = true
)

func (c *Config) EnableLogTailerOrDefault() bool {
	if c.EnableLogTailer != nil {
		return *c.EnableLogTailer
	}
	return DefaultEnableLogTailer
}

type Controller struct {
	ytc               yt.Client
	l                 log.Logger
	clusterConnection map[string]interface{}
	root              ypath.Path
	cluster           string
	tvmSecret         string
	config            Config
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

func (c *Controller) buildCommand(speclet *Speclet) string {
	binariesDir := "./"
	if c.config.LocalBinariesDir != nil {
		binariesDir = *c.config.LocalBinariesDir + "/"
	}
	trampolinePath := binariesDir + "clickhouse-trampoline"
	chytPath := binariesDir + "ytserver-clickhouse"
	logTailerPath := binariesDir + "ytserver-log-tailer"

	var args []string
	args = append(args, trampolinePath, chytPath)
	args = append(args, "--monitoring-port", "10142", "--log-tailer-monitoring-port", "10242")
	if speclet.EnableGeoDataOrDefault() {
		args = append(args, "--prepare-geodata")
	}
	if c.config.EnableLogTailerOrDefault() {
		args = append(args, "--log-tailer-bin", logTailerPath)
	}
	return strings.Join(args, " ")
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

	// Build artifacts if there are no local binaries.
	if c.config.LocalBinariesDir == nil {
		err = c.appendArtifacts(ctx, &speclet, &filePaths, &description)
		if err != nil {
			return
		}
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
	command := c.buildCommand(&speclet)

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

func parseConfig(rawConfig yson.RawValue) Config {
	var controllerConfig Config
	if rawConfig != nil {
		if err := yson.Unmarshal(rawConfig, &controllerConfig); err != nil {
			panic(err)
		}
	}
	return controllerConfig
}

func NewController(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, rawConfig yson.RawValue) strawberry.Controller {
	c := &Controller{
		l:       l,
		ytc:     ytc,
		root:    root,
		cluster: cluster,
		config:  parseConfig(rawConfig),
	}
	clusterConnection, err := c.getClusterConnection(context.Background())
	if err != nil {
		panic(err)
	}
	c.clusterConnection = clusterConnection
	c.prepareTvmSecret()
	return c
}
