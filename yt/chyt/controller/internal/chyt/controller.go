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

type LogRotationModeType string

const (
	LogRotationModeDisabled LogRotationModeType = "disabled"
	LogRotationModeBuiltin  LogRotationModeType = "builtin"

	DefaultLogRotationMode = LogRotationModeBuiltin
)

type Config struct {
	// LocalBinariesDir is set if we want to execute local binaries on the clique.
	// This directory should contain trampoline and chyt binaries.
	LocalBinariesDir          *string              `yson:"local_binaries_dir"`
	LogRotationMode           *LogRotationModeType `yson:"log_rotation_mode"`
	AddressResolver           map[string]any       `yson:"address_resolver"`
	EnableYandexSpecificLinks *bool                `yson:"enable_yandex_specific_links"`
}

const (
	DefaultEnableYandexSpecificLinks = false
)

func (c *Config) LogRotationModeOrDefault() LogRotationModeType {
	if c.LogRotationMode != nil {
		return *c.LogRotationMode
	}
	return DefaultLogRotationMode
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
	cachedClusterConnection map[string]any
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

var (
	clusterConnectionFields = []string{
		"discovery_connection",
		"master_cache",
		"primary_master",
		"secondary_masters",
		"timestamp_provider",
		"tvm_id",
		"chyt",
	}
)

func (c *Controller) updateClusterConnection(ctx context.Context) (changed bool, err error) {
	var clusterConnection map[string]any
	err = c.ytc.GetNode(ctx, ypath.Path("//sys/@cluster_connection"), &clusterConnection, nil)
	if err != nil {
		c.l.Error("failed to update cluster connection", log.Error(err))
		return false, err
	}
	if _, ok := clusterConnection["block_cache"]; ok {
		c.l.Error("failed to update cluster connection: cluster connection contains block_cache section")
		return false, fmt.Errorf("chyt: cluster connection contains block_cache section; looks like a misconfiguration")
	}
	for _, field := range clusterConnectionFields {
		newValue, newValueExists := clusterConnection[field]
		cachedValue, cachedValueExists := c.cachedClusterConnection[field]
		if newValueExists != cachedValueExists ||
			newValueExists && cachedValueExists && !reflect.DeepEqual(newValue, cachedValue) {
			c.cachedClusterConnection = clusterConnection
			changed = true
			break
		}
	}
	c.l.Info("cluster connection updated", log.Bool("changed", changed))
	return changed, nil
}

func (c *Controller) buildCommand(speclet *Speclet) string {
	binariesDir := "./"
	if c.config.LocalBinariesDir != nil {
		binariesDir = *c.config.LocalBinariesDir + "/"
	}
	trampolinePath := binariesDir + "clickhouse-trampoline"
	chytPath := binariesDir + "ytserver-clickhouse"

	var args []string
	args = append(args, trampolinePath, chytPath)
	if speclet.EnableGeoDataOrDefault() {
		args = append(args, "--prepare-geodata")
	}
	return strings.Join(args, " ")
}

func (c *Controller) Root() ypath.Path {
	return c.root
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]any, description map[string]any, annotations map[string]any, err error) {
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

	spec = map[string]any{
		"tasks": map[string]any{
			"instances": map[string]any{
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
	annotations = map[string]any{
		"is_clique": true,
		"expose":    true,
	}

	if c.tvmSecret != "" {
		spec["secure_vault"] = map[string]any{
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

func (c *Controller) DescribeOptions(parsedSpeclet any) []strawberry.OptionGroupDescriptor {
	speclet := parsedSpeclet.(Speclet)

	return []strawberry.OptionGroupDescriptor{
		{
			Title: "Resources",
			Options: []strawberry.OptionDescriptor{
				{
					Title:        "Instance count",
					Name:         "instance_count",
					Type:         strawberry.TypeInt64,
					CurrentValue: speclet.InstanceCount,
					DefaultValue: defaultInstanceCount,
					MinValue:     1,
					MaxValue:     100,
					Description:  "Clique instance count.",
				},
				{
					Title:        "Instance CPU",
					Name:         "instance_cpu",
					Type:         strawberry.TypeInt64,
					CurrentValue: speclet.InstanceCPU,
					DefaultValue: defaultInstanceCPU,
					MinValue:     1,
					MaxValue:     100,
					Description:  "Number of CPU cores per instance.",
				},
				{
					Title:        "Instance total memory",
					Name:         "instance_total_memory",
					Type:         strawberry.TypeByteCount,
					CurrentValue: speclet.InstanceTotalMemory,
					DefaultValue: (&InstanceMemory{}).totalMemory(),
					MinValue:     1024 * 1024 * 1024,
					MaxValue:     300 * 1024 * 1024 * 1024,
					Description:  "Amount of RAM per instance in bytes.",
				},
			},
		},
		{
			Title:  "Advanced",
			Hidden: true,
			Options: []strawberry.OptionDescriptor{
				{
					Title:        "CHYT version",
					Name:         "chyt_version",
					Type:         strawberry.TypeString,
					CurrentValue: speclet.CHYTVersion,
				},
				{
					Title:        "Enable geodata",
					Name:         "enable_geodata",
					Type:         strawberry.TypeBool,
					CurrentValue: speclet.EnableGeoData,
					DefaultValue: DefaultEnableGeoData,
					Description:  "If true, system dictionaries for geo-functions are set up automatically.",
				},
				{
					Title:        "Geodata path",
					Name:         "geodata_path",
					Type:         strawberry.TypePath,
					CurrentValue: speclet.GeoDataPath,
					DefaultValue: DefaultGeoDataPath,
				},
				{
					Title:        "Query settings",
					Name:         "query_settings",
					Type:         strawberry.TypeYson,
					CurrentValue: speclet.QuerySettings,
					DefaultValue: map[string]any{},
					Description:  "Map with default query settings.",
				},
				{
					Title:        "ClickHouse config",
					Name:         "clickhouse_config",
					Type:         strawberry.TypeYson,
					CurrentValue: speclet.ClickHouseConfig,
					DefaultValue: map[string]any{},
				},
				{
					Title:        "YT config",
					Name:         "yt_config",
					Type:         strawberry.TypeYson,
					CurrentValue: speclet.YTConfig,
					DefaultValue: map[string]any{},
				},
				{
					Title:        "Instance memory",
					Name:         "instance_memory",
					Type:         strawberry.TypeYson,
					CurrentValue: speclet.InstanceMemory,
					DefaultValue: nil,
				},
			},
		},
	}
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any) map[string]any {
	speclet := parsedSpeclet.(Speclet)
	var instanceCount, totalCPU, totalMemory any
	if err := c.populateResources(&speclet); err == nil {
		instanceCount = speclet.InstanceCount
		totalCPU = speclet.CliqueCPU
		totalMemory = speclet.CliqueMemory
	}
	return map[string]any{
		"instance_count": instanceCount,
		"total_cpu":      totalCPU,
		"total_memory":   totalMemory,
	}
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
