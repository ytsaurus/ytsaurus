package chyt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/ptr"
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

type ResourcesConfig struct {
	DefaultInstanceCPU    *uint64 `yson:"default_instance_cpu"`
	DefaultInstanceMemory *uint64 `yson:"default_instance_memory"`
}

type Config struct {
	// LocalBinariesDir is set if we want to execute local binaries on the clique.
	// This directory should contain trampoline and chyt binaries.
	LocalBinariesDir           *string                 `yson:"local_binaries_dir"`
	LogRotationMode            *LogRotationModeType    `yson:"log_rotation_mode"`
	AddressResolver            map[string]any          `yson:"address_resolver"`
	BusServer                  map[string]any          `yson:"bus_server"`
	EnableYandexSpecificLinks  *bool                   `yson:"enable_yandex_specific_links"`
	ExportSystemLogTables      *bool                   `yson:"export_system_log_tables"`
	EnableGeodata              *bool                   `yson:"enable_geodata"`
	EnableRuntimeData          *bool                   `yson:"enable_runtime_data"`
	ResourcesConfig            *ResourcesConfig        `yson:"resources_config"`
	SecureVaultFiles           map[string]string       `yson:"secure_vault_files"`
	DefaultSpeclet             *Speclet                `yson:"default_speclet"`
	SpecletConfigExclusionTree map[string]any          `yson:"speclet_config_exclusion_tree"`
	DefaultOpletHealth         *strawberry.OpletHealth `yson:"default_oplet_health"`
}

type controllerSnapshot struct {
	ClusterConnectionHash string `yson:"cluster_connection_hash,omitempty"`
	SecureVaultFilesHash  string `yson:"secure_vault_files_hash,omitempty"`
}

const (
	DefaultEnableYandexSpecificLinks                        = false
	DefaultExportSystemLogTables                            = false
	DefaultEnableGeodata                                    = false
	DefaultEnableRuntimeData                                = false
	DefaultDefaultOpletHealth        strawberry.OpletHealth = strawberry.OpletHealthUnknown
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

func (c *Config) ExportSystemLogTablesOrDefault() bool {
	if c.ExportSystemLogTables != nil {
		return *c.ExportSystemLogTables
	}
	return DefaultExportSystemLogTables
}

func (c *Config) EnableGeodataOrDefault() bool {
	if c.EnableGeodata != nil {
		return *c.EnableGeodata
	}
	return DefaultEnableGeodata
}

func (c *Config) EnableRuntimeDataOrDefault() bool {
	if c.EnableRuntimeData != nil {
		return *c.EnableRuntimeData
	}
	return DefaultEnableRuntimeData
}

func (c *Config) DefaultOpletHealthOrDefault() strawberry.OpletHealth {
	if c.DefaultOpletHealth != nil {
		return *c.DefaultOpletHealth
	}
	return DefaultDefaultOpletHealth
}

func (c *Config) getDefaultInstanceCPU() uint64 {
	if c.ResourcesConfig != nil && c.ResourcesConfig.DefaultInstanceCPU != nil {
		return *c.ResourcesConfig.DefaultInstanceCPU
	}
	return defaultInstanceCPU
}

func (c *Config) getDefaultMemory() uint64 {
	if c.ResourcesConfig != nil && c.ResourcesConfig.DefaultInstanceMemory != nil {
		return *c.ResourcesConfig.DefaultInstanceMemory
	}
	return (&InstanceMemory{}).totalMemory()
}

type chytOpletInfo struct {
	CHYTRunningVersion     string `yson:"chyt_running_version"`
	CHYTRunningVersionPath string `yson:"chyt_running_version_path"`
}

type Controller struct {
	ytc                     yt.Client
	dc                      yt.DiscoveryClient
	l                       log.Logger
	cachedClusterConnection map[string]any
	root                    ypath.Path
	cluster                 string
	tvmSecret               string
	config                  Config
	secrets                 map[string][]byte
	snapshot                controllerSnapshot
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
		"bus_client",
		"discovery_connection",
		"master_cache",
		"primary_master",
		"secondary_masters",
		"timestamp_provider",
		"tvm_id",
		"chyt",
	}
)

type ysonHashEncoder struct {
	*yson.Encoder
	hasher hash.Hash
}

func (encoder *ysonHashEncoder) Sum() string {
	return hex.EncodeToString(encoder.hasher.Sum(nil))
}

func newYsonHashEncoder() ysonHashEncoder {
	hasher := sha256.New()
	return ysonHashEncoder{
		Encoder: yson.NewEncoderWriter(
			yson.NewWriterConfig(
				hasher,
				yson.WriterConfig{
					Format: yson.FormatBinary,
					Kind:   yson.StreamListFragment,
				},
			),
		),
		hasher: hasher,
	}
}

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
	encoder := newYsonHashEncoder()
	for _, field := range clusterConnectionFields {
		newValue, newValueExists := clusterConnection[field]
		cachedValue, cachedValueExists := c.cachedClusterConnection[field]
		if newValueExists != cachedValueExists ||
			newValueExists && cachedValueExists && !reflect.DeepEqual(newValue, cachedValue) {
			c.cachedClusterConnection = clusterConnection
			changed = true
		}
		if newValueExists {
			if err = encoder.Encode(field); err != nil {
				return false, err
			}
			if err = encoder.Encode(newValue); err != nil {
				return false, err
			}
		}
	}
	c.snapshot.ClusterConnectionHash = encoder.Sum()
	c.l.Info("cluster connection updated", log.Bool("changed", changed), log.String("hash", c.snapshot.ClusterConnectionHash))
	return changed, nil
}

func (c *Controller) updateSecrets() (changed bool, err error) {
	encoder := newYsonHashEncoder()
	for _, secret := range slices.Sorted(maps.Keys(c.config.SecureVaultFiles)) {
		var value []byte
		if value, err = os.ReadFile(c.config.SecureVaultFiles[secret]); err != nil {
			c.l.Error("failed to read secret", log.Error(err))
			return false, err
		}
		if !bytes.Equal(c.secrets[secret], value) {
			c.secrets[secret] = value
			changed = true
		}
		if err := encoder.Encode(secret); err != nil {
			return false, err
		}
		if err := encoder.Encode(value); err != nil {
			return false, err
		}
	}
	c.snapshot.SecureVaultFilesHash = encoder.Sum()
	c.l.Info("secrets updated", log.Bool("changed", changed), log.String("hash", c.snapshot.SecureVaultFilesHash))
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
	if speclet.EnableGeodataOrDefault(c.config.EnableGeodataOrDefault()) {
		args = append(args, "--prepare-geodata")
	}
	return strings.Join(args, " ")
}

func (c *Controller) Root() ypath.Path {
	return c.root
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]any, description map[string]any, annotations map[string]any, runAsUser bool, err error) {
	alias := oplet.Alias()

	description = buildDescription(c.cluster, alias, c.config.EnableYandexSpecificLinksOrDefault())
	speclet := oplet.ControllerSpeclet().(Speclet)

	var opletInfo chytOpletInfo
	var filePaths []ypath.Rich

	// Populate resources.
	err = c.populateResources(&speclet)
	if err != nil {
		return
	}

	err = c.prepareCypressDirectories(ctx, oplet.Alias())
	if err != nil {
		return
	}

	// Build artifacts if there are no local binaries.
	if c.config.LocalBinariesDir == nil {
		err = c.appendOpArtifacts(ctx, &speclet, &filePaths, &description, &opletInfo)
		if err != nil {
			return
		}
	} else {
		opletInfo.CHYTRunningVersion = "LocalVersion"
		opletInfo.CHYTRunningVersionPath = filepath.Join(*c.config.LocalBinariesDir, "ytserver-clickhouse")
	}
	oplet.SetOpletInfo(opletInfo)

	// Build configs.
	err = c.appendConfigs(ctx, oplet, &speclet, &filePaths)
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
		"title":                "CHYT clique *" + alias,
	}
	annotations = map[string]any{
		"is_clique": true,
		"expose":    true,
	}

	if c.tvmSecret != "" {
		oplet.SetSecret("TVM_SECRET", c.tvmSecret)
	}

	for secret, value := range c.secrets {
		oplet.SetSecret(secret, value)
	}

	// Prepare runtime stuff if need: stderr/core-table, etc.
	if speclet.EnableRuntimeDataOrDefault(c.config.EnableRuntimeDataOrDefault()) {
		var runtimePaths runtimePaths
		runtimePaths, err = c.prepareRuntime(ctx, speclet.RuntimeDataSpecOrDefault(), alias, oplet.NextIncarnationIndex())
		if err != nil {
			return
		}
		spec["stderr_table_path"] = runtimePaths.StderrTable
		spec["core_table_path"] = runtimePaths.CoreTable
	}

	runAsUser = false

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

func (c *Controller) needsRestart(ctx context.Context, oplet *strawberry.Oplet) (bool, error) {
	speclet := oplet.ControllerSpeclet().(Speclet)
	if !speclet.RestartOnVersionDriftOrDefault() {
		return false, nil
	}

	cypressVersionPath, err := c.resolveSymlink(ctx, CHYTBinaryDirectory.Child(speclet.CHYTVersionOrDefault()))
	if err != nil {
		return false, err
	}
	specifiedVersionPath := filepath.Base(cypressVersionPath.String())

	briefInfo := oplet.GetBriefInfo()
	var controllerInfo chytOpletInfo
	err = yson.Unmarshal(briefInfo.OpletInfo, &controllerInfo)
	if err != nil {
		return false, err
	}

	if controllerInfo.CHYTRunningVersionPath != specifiedVersionPath {
		return true, nil
	}
	return false, nil
}

func (c *Controller) checkHealth(ctx context.Context, oplet *strawberry.Oplet) (strawberry.OpletHealth, string) {
	// If discovery client hasn't been initialized for any reason, we consider this as the default case.
	if c.dc == nil {
		return c.config.DefaultOpletHealthOrDefault(), ""
	}

	group := "/chyt/" + oplet.Alias()
	// NB: Strawberry doesn't limit the number of instances, but ListMember requires an explicit limit.
	members, err := c.dc.ListMembers(ctx, group, &yt.ListMembersOptions{Limit: ptr.Int32(math.MaxInt32)})
	if err != nil && !yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchGroup) {
		return strawberry.OpletHealthUnknown, fmt.Sprintf("failed to discover instances: %s", err)
	}

	if yterrors.ContainsErrorCode(err, yterrors.CodeNoSuchGroup) || len(members) == 0 {
		return strawberry.OpletHealthPending, "There are no active instances yet."
	}

	return strawberry.OpletHealthGood, ""
}

const (
	versionDriftRestartReason = "Current clique version and version from speclet are differ. The restart was scheduled."
)

func (c *Controller) CheckState(ctx context.Context, oplet *strawberry.Oplet) (state strawberry.ControllerOpletState, err error) {
	state.NeedsRestart, err = c.needsRestart(ctx, oplet)
	if err != nil {
		return
	}
	if state.NeedsRestart {
		state.Health = strawberry.OpletHealthPending
		state.Reason = versionDriftRestartReason
		return
	}

	state.Health, state.Reason = c.checkHealth(ctx, oplet)
	return
}

func (c *Controller) UpdateState() (changed bool, err error) {
	connectionChanged, err := c.updateClusterConnection(context.Background())
	if err != nil {
		return false, err
	}
	secretsChanged, err := c.updateSecrets()
	if err != nil {
		return false, err
	}
	return connectionChanged || secretsChanged, nil
}

func (c *Controller) GetControllerSnapshot() (yson.RawValue, error) {
	// NOTE: Must use same yson format as cypress node get response to match.
	return yson.MarshalFormat(c.snapshot, yson.FormatBinary)
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
					MinValue:     memNonElastic,
					MaxValue:     300 * gib,
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
					Title:        "Restart on version drift",
					Name:         "restart_on_version_drift",
					Type:         strawberry.TypeBool,
					CurrentValue: speclet.RestartOnVersionDrift,
					DefaultValue: speclet.RestartOnVersionDriftOrDefault(),
				},
				{
					Title:        "Enable geodata",
					Name:         "enable_geodata",
					Type:         strawberry.TypeBool,
					CurrentValue: speclet.EnableGeodata,
					DefaultValue: c.config.EnableGeodataOrDefault(),
					Description:  "If true, system dictionaries for geo-functions are set up automatically.",
				},
				{
					Title:        "Geodata path",
					Name:         "geodata_path",
					Type:         strawberry.TypePath,
					CurrentValue: speclet.GeodataPath,
					DefaultValue: DefaultGeodataPath,
				},
				{
					Title:        "Export system log tables",
					Name:         "export_system_log_tables",
					Type:         strawberry.TypeBool,
					CurrentValue: speclet.ExportSystemLogTables,
					DefaultValue: c.config.ExportSystemLogTablesOrDefault(),
					Description:  "If true, system log tables (e.g. system.query_log) configured with SystemLogTableExporter engine are exported to cypress.",
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
				{
					Title:        "Enable sticky query distribution",
					Name:         "enable_sticky_query_distribution",
					Type:         strawberry.TypeBool,
					CurrentValue: speclet.EnableStickyQueryDistribution,
					DefaultValue: false,
					Description:  "Enables query distribution based on query hash for better query cache hit rate.",
				},
				{
					Title:        "Query sticky group size",
					Name:         "query_sticky_group_size",
					Type:         strawberry.TypeInt64,
					CurrentValue: speclet.QueryStickyGroupSize,
					DefaultValue: DefaultQueryStickyGroupSize,
					MinValue:     1,
					MaxValue:     100,
					Description:  "Identical queries are distributed uniformly among the group of |QueryStickyGroupSize|-instances, different queries correspond to different groups. This has effect only if sticky query distribution is enabled.",
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

func (c *Controller) GetScalerTarget(ctx context.Context, opletInfo strawberry.OpletInfoForScaler) (*strawberry.ScalerTarget, error) {
	return nil, nil
}

func (c *Controller) GetOpletInfo(ctx context.Context, oplet *strawberry.Oplet) (any, error) {
	opID := oplet.GetBriefInfo().YTOperation.ID
	if opID == yt.NullOperationID {
		return chytOpletInfo{}, nil
	}

	opInfo, err := c.ytc.GetOperation(
		ctx,
		opID,
		&yt.GetOperationOptions{
			Attributes: []string{"full_spec"},
			MasterReadOptions: &yt.MasterReadOptions{
				ReadFrom: yt.ReadFromFollower,
			},
		})
	if err != nil {
		return nil, err
	}

	var s struct {
		Description struct {
			Artifacts struct {
				CH struct {
					Version string `yson:"version"`
				} `yson:"ytserver-clickhouse"`
			} `yson:"artifacts"`
		} `yson:"description"`

		Tasks struct {
			Instances struct {
				FilePaths []ypath.Rich `yson:"file_paths"`
			} `yson:"instances"`
		} `yson:"tasks"`
	}
	err = yson.Unmarshal(opInfo.FullSpec, &s)
	if err != nil {
		return nil, err
	}

	opletInfo := chytOpletInfo{CHYTRunningVersion: s.Description.Artifacts.CH.Version}
	for _, fp := range s.Tasks.Instances.FilePaths {
		if fp.FileName == "ytserver-clickhouse" {
			opletInfo.CHYTRunningVersionPath = fp.Path.String()
			break
		}
	}
	return opletInfo, nil
}

func (c *Controller) RunAsUser() bool {
	return false
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

func createDiscoveryClient(ctx context.Context, ytc yt.Client) (yt.DiscoveryClient, error) {
	var discoveryConnection struct {
		Addresses *[]string `yson:"addresses"`
		Endpoints *struct {
			EndpointSetId string   `yson:"endpoint_set_id"`
			Clusters      []string `yson:"clusters"`
		} `yson:"endpoints"`
	}

	err := ytc.GetNode(ctx, ypath.Path("//sys/@cluster_connection/discovery_connection"), &discoveryConnection, nil)
	if err != nil {
		return nil, err
	}

	var cfg yt.DiscoveryConfig
	if discoveryConnection.Addresses != nil {
		cfg.DiscoveryServers = *discoveryConnection.Addresses
	}
	if discoveryConnection.Endpoints != nil {
		cfg.EndpointSet = discoveryConnection.Endpoints.EndpointSetId
		cfg.YPClusters = discoveryConnection.Endpoints.Clusters
	}

	return newDiscoveryClient(cfg)
}

func NewController(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, rawConfig yson.RawValue) strawberry.Controller {
	dc, err := createDiscoveryClient(context.Background(), ytc)
	if err != nil {
		panic(err)
	}

	c := &Controller{
		l:       l,
		ytc:     ytc,
		dc:      dc,
		root:    root,
		cluster: cluster,
		secrets: make(map[string][]byte),
		config:  parseConfig(rawConfig),
	}
	c.prepareTvmSecret()
	return c
}
