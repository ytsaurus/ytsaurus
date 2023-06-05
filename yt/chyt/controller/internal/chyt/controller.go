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
	LocalBinariesDir          *string        `yson:"local_binaries_dir"`
	EnableLogTailer           *bool          `yson:"enable_log_tailer"`
	AddressResolver           map[string]any `yson:"address_resolver"`
	EnableYandexSpecificLinks *bool          `yson:"enable_yandex_specific_links"`
}

const (
	DefaultEnableLogTailer           = true
	DefaultEnableYandexSpecificLinks = false
)

func (c *Config) EnableLogTailerOrDefault() bool {
	if c.EnableLogTailer != nil {
		return *c.EnableLogTailer
	}
	return DefaultEnableLogTailer
}

func (c *Config) EnableYandexSpecificLinksOrDefault() bool {
	if c.EnableYandexSpecificLinks != nil {
		return *c.EnableYandexSpecificLinks
	}
	return DefaultEnableYandexSpecificLinks
}

type Controller struct {
	ytc                     yt.Client
	l                       log.Logger
	cachedClusterConnection map[string]interface{}
	root                    ypath.Path
	cluster                 string
	tvmSecret               string
	config                  Config
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
	rawTvmID, ok := c.cachedClusterConnection["tvm_id"]
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

func (c *Controller) updateClusterConnection(ctx context.Context) (changed bool, err error) {
	var clusterConnection map[string]interface{}
	err = c.ytc.GetNode(ctx, ypath.Path("//sys/@cluster_connection"), &clusterConnection, nil)
	if err != nil {
		c.l.Error("failed to update cluster connection", log.Error(err))
		return false, err
	}
	if _, ok := clusterConnection["block_cache"]; ok {
		c.l.Error("failed to update cluster connection: cluster connection contains block_cache section")
		return false, fmt.Errorf("chyt: cluster connection contains block_cache section; looks like a misconfiguration")
	}
	if !reflect.DeepEqual(clusterConnection, c.cachedClusterConnection) {
		c.cachedClusterConnection = clusterConnection
		changed = true
	}
	c.l.Error("cluster connection updated", log.Bool("changed", changed))
	return changed, nil
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
	args = append(args, "--monitoring-port", "10142")
	if speclet.EnableGeoDataOrDefault() {
		args = append(args, "--prepare-geodata")
	}
	if c.config.EnableLogTailerOrDefault() {
		args = append(args, "--log-tailer-bin", logTailerPath, "--log-tailer-monitoring-port", "10242")
	}
	return strings.Join(args, " ")
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]interface{}, description map[string]interface{}, annotations map[string]interface{}, err error) {
	alias := oplet.Alias()

	description = buildDescription(c.cluster, alias, c.config.EnableYandexSpecificLinksOrDefault())
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

func (c *Controller) UpdateState() (changed bool, err error) {
	return c.updateClusterConnection(context.Background())
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
	_, err := c.updateClusterConnection(context.Background())
	if err != nil {
		panic(err)
	}
	c.prepareTvmSecret()
	return c
}
