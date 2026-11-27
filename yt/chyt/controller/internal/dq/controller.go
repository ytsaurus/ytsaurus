package dq

import (
	"bytes"
	"context"
	"fmt"
	"path"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/discovery"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	envActorPort     = "ACTOR_PORT"
	envActorNodeID   = "ACTOR_NODE_ID"
	envOperationSize = "OPERATION_SIZE"
)

type Controller struct {
	ytc     yt.Client
	dc      yt.DiscoveryClient
	l       log.Logger
	root    ypath.Path
	cluster string
	config  Config
}

func (c *Controller) UpdateState() (bool, error) {
	return false, nil
}

func (c *Controller) GetControllerSnapshot() (yson.RawValue, error) {
	// An empty byte slice is omitted from persistent state, causing Strawberry
	// to add it and flush the state on every pass instead of tracking operation status.
	return yson.Marshal(struct{}{})
}

func (c *Controller) IsControllerSnapshotOutdated(snapshot yson.RawValue, _ *strawberry.Oplet) (bool, error) {
	current, err := c.GetControllerSnapshot()
	if err != nil {
		return false, err
	}
	return !bytes.Equal(snapshot, current), nil
}

// Application settings are passed through as YSON, without Go protobuf types.
func (c *Controller) appendOperationConfigs(ctx context.Context, oplet *strawberry.Oplet, speclet Speclet) ([]ypath.Rich, error) {
	if err := c.ensureArtifactDir(ctx, oplet.Alias()); err != nil {
		return nil, err
	}
	config, err := c.buildServiceConfig(speclet, oplet.Alias())
	if err != nil {
		return nil, err
	}
	uploaded, err := c.uploadTextArtifact(ctx, oplet.Alias(), serviceConfigFile, string(config))
	if err != nil {
		return nil, err
	}
	return []ypath.Rich{uploaded}, nil
}

func (c *Controller) buildServiceConfig(speclet Speclet, alias string) ([]byte, error) {
	// Copy the application section before filling launcher-owned defaults.
	service := make(map[string]any, len(speclet.Service)+1)
	for key, value := range speclet.Service {
		service[key] = value
	}
	service["clique_alias"] = alias
	coordinator := make(map[string]any, len(speclet.Coordinator)+3)
	for key, value := range speclet.Coordinator {
		coordinator[key] = value
	}
	for key, fallback := range map[string]string{
		"cluster_name": c.cluster, "proxy_address": c.cluster,
		"user":   c.config.DefaultYTUserOrDefault(),
		"prefix": c.config.DefaultYTPrefixOrDefault().String(),
	} {
		if _, ok := coordinator[key]; !ok {
			coordinator[key] = fallback
		}
	}
	config := make(map[string]any, len(speclet.RawConfig)+2)
	for key, value := range speclet.RawConfig {
		config[key] = value
	}
	config["service"] = service
	config["coordinator"] = coordinator
	return yson.MarshalFormat(config, yson.FormatText)
}

func buildBinaryFilePath(cypressPath string) ypath.Rich {
	return ypath.Rich{
		Path:     ypath.Path(cypressPath),
		FileName: "dq_service_process",
	}
}

func (c *Controller) buildTaskSpec(speclet Speclet, strawberrySpeclet strawberry.Speclet, alias string, nodeID int, binaryPath string, configPaths []ypath.Rich) map[string]any {
	interconnectPort := speclet.InterconnectPortOrDefault()
	if !speclet.SameActorPortsOrDefault() {
		interconnectPort += nodeID - speclet.MinNodeIDOrDefault()
	}

	environment := map[string]string{
		envActorPort:     fmt.Sprintf("%d", interconnectPort),
		envActorNodeID:   fmt.Sprintf("%d", nodeID),
		envOperationSize: fmt.Sprintf("%d", speclet.InstanceCountOrDefault()),
	}
	for key, value := range speclet.ExtraEnv {
		environment[key] = value
	}

	filePaths := make([]ypath.Rich, 0, 1+len(configPaths))
	filePaths = append(filePaths, buildBinaryFilePath(binaryPath))
	filePaths = append(filePaths, configPaths...)
	for _, layer := range speclet.PortoLayers {
		filePaths = append(filePaths, ypath.Rich{Path: ypath.Path(layer), FileName: path.Base(layer)})
	}

	task := map[string]any{
		"command":      "./dq_service_process",
		"job_count":    1,
		"file_paths":   filePaths,
		"cpu_limit":    speclet.CPULimitOrDefault(),
		"memory_limit": speclet.MemoryLimitOrDefault(),
		"environment":  environment,
		"port_count":   1,
	}

	if len(speclet.OperationLayers) > 0 {
		task["layer_paths"] = speclet.OperationLayers
	}
	if speclet.ContainerCPULimit != nil {
		task["set_container_cpu_limit"] = *speclet.ContainerCPULimit
	}
	if speclet.UseTmpfs != nil && *speclet.UseTmpfs {
		task["tmpfs_path"] = speclet.FileCacheDirOrDefault()
		if speclet.TmpfsSize != nil {
			task["tmpfs_size"] = *speclet.TmpfsSize
		}
	}

	extraSpec := map[string]any{}

	if enabled, _ := speclet.needsRPCProxy(); enabled {
		extraSpec["enable_rpc_proxy_in_job_proxy"] = true
	}
	if speclet.EnablePorto != nil {
		extraSpec["enable_porto"] = *speclet.EnablePorto
	}
	if strawberrySpeclet.DiskRequest != nil {
		extraSpec["disk_request"] = strawberrySpeclet.DiskRequest
	}
	if len(extraSpec) > 0 {
		for k, v := range extraSpec {
			task[k] = v
		}
	}

	return task
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (*strawberry.PrepareResult, error) {
	result := &strawberry.PrepareResult{}
	alias := oplet.Alias()
	speclet := oplet.ControllerSpeclet().(Speclet)
	strawberrySpeclet := oplet.StrawberrySpeclet()

	binaryPath := speclet.BinaryCypressPathOrDefault(c.config.DefaultBinaryCypressPathOrDefault().String())

	configPaths, err := c.appendOperationConfigs(ctx, oplet, speclet)
	if err != nil {
		return nil, err
	}

	instanceCount := speclet.InstanceCountOrDefault()
	minNodeID := speclet.MinNodeIDOrDefault()

	tasks := make(map[string]any, instanceCount)
	for i := 0; i < instanceCount; i++ {
		nodeID := minNodeID + i
		taskName := fmt.Sprintf("yql_worker_%d", nodeID)
		tasks[taskName] = c.buildTaskSpec(speclet, strawberrySpeclet, alias, nodeID, binaryPath, configPaths)
	}

	result.Spec = map[string]any{
		"tasks":                      tasks,
		"try_avoid_duplicating_jobs": true,
		"max_failed_job_count":       5,
		"title":                      "DQ clique *" + alias,
	}

	if speclet.SchedulingTagFilter != nil {
		result.Spec["scheduling_tag_filter"] = *speclet.SchedulingTagFilter
	}
	if speclet.EnableRuntimeDataOrDefault(c.config.EnableRuntimeDataOrDefault()) {
		runtimePaths, prepareErr := c.prepareRuntime(
			ctx,
			speclet.RuntimeDataSpecOrDefault(c.config.DefaultRuntimeDataPathOrDefault(c.root).String()),
			alias,
			oplet.NextIncarnationIndex(),
		)
		if prepareErr != nil {
			return nil, prepareErr
		}
		result.Spec["stderr_table_path"] = runtimePaths.StderrTable
		result.Spec["core_table_path"] = runtimePaths.CoreTable
	}

	result.Description = map[string]any{
		"dq_clique_alias": alias,
		"instance_count":  instanceCount,
		"binary_path":     binaryPath,
	}
	result.Annotations = map[string]any{
		"is_clique": true,
		"expose":    true,
	}

	result.RunAsUser = false
	return result, nil
}

func (c *Controller) Family() string {
	return "dq"
}

func (c *Controller) Root() ypath.Path {
	return c.root
}

func (c *Controller) ParseSpeclet(specletYson yson.RawValue) (any, error) {
	speclet, err := parseSpeclet(specletYson)
	if err != nil {
		return nil, yterrors.Err("failed to parse dq speclet", err)
	}
	return speclet, nil
}

func (c *Controller) checkHealth(ctx context.Context, oplet *strawberry.Oplet) (strawberry.OpletHealth, string) {
	if !oplet.Active() || !c.config.EnableDiscoveryHealthCheckOrDefault() {
		return strawberry.OpletHealthGood, ""
	}
	if c.dc == nil {
		return strawberry.OpletHealthUnknown, "discovery client is not initialized"
	}

	members, err := listDiscoveryMembers(ctx, c.dc, oplet.Alias())
	if err != nil {
		return strawberry.OpletHealthUnknown, fmt.Sprintf("failed to discover instances: %s", err)
	}
	if len(members) == 0 {
		return strawberry.OpletHealthPending, "There are no active instances yet."
	}

	speclet := oplet.ControllerSpeclet().(Speclet)
	expected := speclet.InstanceCountOrDefault()
	if len(members) < expected {
		return strawberry.OpletHealthPending, fmt.Sprintf(
			"discovered %d instances, expected %d",
			len(members),
			expected,
		)
	}

	return strawberry.OpletHealthGood, ""
}

func (c *Controller) CheckState(ctx context.Context, oplet *strawberry.Oplet) (state strawberry.ControllerOpletState, err error) {
	state.Health, state.Reason = c.checkHealth(ctx, oplet)
	return
}

func (c *Controller) ListDiscoveryInstances(ctx context.Context, alias string) ([]map[string]any, error) {
	members, err := listDiscoveryMembers(ctx, c.dc, alias)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(members))
	for _, member := range members {
		entry := map[string]any{
			"id":       member.ID,
			"priority": member.Priority,
			"revision": member.Revision,
		}
		for _, attr := range member.Attributes {
			var decoded any
			if err := yson.Unmarshal(attr.Value, &decoded); err == nil {
				entry[attr.Key] = decoded
			} else {
				entry[attr.Key] = string(attr.Value)
			}
		}
		result = append(result, entry)
	}
	return result, nil
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
					DefaultValue: DefaultInstanceCount,
					MinValue:     1,
					MaxValue:     100,
					Description:  "Number of dq_service_process instances.",
				},
				{
					Title:        "CPU limit",
					Name:         "cpu_limit",
					Type:         strawberry.TypeInt64,
					CurrentValue: speclet.CPULimit,
					DefaultValue: DefaultCPULimit,
					Description:  "CPU limit per instance.",
				},
				{
					Title:        "Memory limit",
					Name:         "memory_limit",
					Type:         strawberry.TypeByteCount,
					CurrentValue: speclet.MemoryLimit,
					DefaultValue: DefaultMemoryLimit,
					MinValue:     1 * gib,
					MaxValue:     300 * gib,
					Description:  "Memory limit per instance in bytes.",
				},
			},
		},
		{
			Title: "DQ runtime",
			Options: []strawberry.OptionDescriptor{
				{
					Title:        "Binary Cypress path",
					Name:         "binary_cypress_path",
					Type:         strawberry.TypePath,
					CurrentValue: speclet.BinaryCypressPath,
					DefaultValue: c.config.DefaultBinaryCypressPathOrDefault().String(),
				},
				{
					Title:        "Interconnect port",
					Name:         "interconnect_port",
					Type:         strawberry.TypeInt64,
					CurrentValue: speclet.InterconnectPort,
					DefaultValue: DefaultInterconnectPort,
				},
				{
					Title:        "Same actor ports",
					Name:         "same_actor_ports",
					Type:         strawberry.TypeBool,
					CurrentValue: speclet.SameActorPorts,
					DefaultValue: DefaultSameActorPorts,
				},
			},
		},
	}
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any, opletInfo yson.RawValue) map[string]any {
	speclet := parsedSpeclet.(Speclet)
	return map[string]any{
		"instance_count": speclet.InstanceCountOrDefault(),
		"cpu_limit":      speclet.CPULimitOrDefault(),
		"memory_limit":   speclet.MemoryLimitOrDefault(),
	}
}

func (c *Controller) GetScalerTarget(ctx context.Context, opletInfo strawberry.OpletInfoForScaler) (*strawberry.ScalerTarget, error) {
	return nil, nil
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
	config := parseConfig(rawConfig)

	dc, err := discovery.CreateClient(context.Background(), ytc)
	if err != nil {
		l.Warn("failed to create discovery client", log.Error(err))
	}

	return &Controller{
		l:       l,
		ytc:     ytc,
		dc:      dc,
		root:    root,
		cluster: cluster,
		config:  config,
	}
}
