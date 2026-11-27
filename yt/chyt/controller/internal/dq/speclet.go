package dq

import (
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"strings"
	"time"

	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/yson"
)

const (
	gib = 1024 * 1024 * 1024

	DefaultInstanceCount    = 1
	DefaultMinNodeID        = 1
	DefaultCPULimit         = 4
	DefaultMemoryLimit      = 16 * gib
	DefaultInterconnectPort = 31002
	DefaultFileCacheDir     = "file_cache2"
	DefaultSameActorPorts   = true

	DefaultRuntimeDataExpirationTimeout = 12 * 7 * (24 * time.Hour)
)

// Only operation-launch settings have a schema in the controller. Application
// sections remain YSON maps and are interpreted by dq_service_process.
type Speclet struct {
	InstanceCount *int    `yson:"instance_count"`
	MinNodeID     *int    `yson:"min_node_id"`
	CPULimit      *uint64 `yson:"cpu_limit"`
	MemoryLimit   *uint64 `yson:"memory_limit"`

	BinaryCypressPath *string `yson:"binary_cypress_path"`

	InterconnectPort *int  `yson:"interconnect_port"`
	SameActorPorts   *bool `yson:"same_actor_ports"`

	SchedulingTagFilter *string `yson:"scheduling_tag_filter"`

	EnablePorto       *string  `yson:"enable_porto"`
	PortoLayers       []string `yson:"porto_layers"`
	OperationLayers   []string `yson:"operation_layers"`
	ContainerCPULimit *bool    `yson:"set_container_cpu_limit"`
	UseTmpfs          *bool    `yson:"use_tmpfs"`
	TmpfsSize         *uint64  `yson:"tmpfs_size"`

	EnableRuntimeData            *bool   `yson:"enable_runtime_data"`
	RuntimeDataPath              *string `yson:"runtime_data_path"`
	RuntimeDataExpirationTimeout *uint64 `yson:"runtime_data_expiration_timeout"`

	ExtraEnv map[string]string `yson:"extra_env"`

	// Preserve future application sections and restart when opaque settings change.
	RawConfig   map[string]any `yson:"-"`
	Service     map[string]any `yson:"service"`
	Coordinator map[string]any `yson:"coordinator"`
	Backend     map[string]any `yson:"backend"`
	Scheduler   map[string]any `yson:"scheduler"`
}

type runtimeDataSpec struct {
	RuntimeDataPath              string
	RuntimeDataExpirationTimeout uint64
}

func (s *Speclet) EnableRuntimeDataOrDefault(defaultValue bool) bool {
	if s.EnableRuntimeData != nil {
		return *s.EnableRuntimeData
	}
	return defaultValue
}

func (s *Speclet) RuntimeDataSpecOrDefault(defaultPath string) runtimeDataSpec {
	spec := runtimeDataSpec{
		RuntimeDataPath:              defaultPath,
		RuntimeDataExpirationTimeout: uint64(DefaultRuntimeDataExpirationTimeout.Milliseconds()),
	}
	if s.RuntimeDataPath != nil {
		spec.RuntimeDataPath = *s.RuntimeDataPath
	}
	if s.RuntimeDataExpirationTimeout != nil {
		spec.RuntimeDataExpirationTimeout = *s.RuntimeDataExpirationTimeout
	}
	return spec
}

func (s *Speclet) InstanceCountOrDefault() int {
	if s.InstanceCount != nil {
		return *s.InstanceCount
	}
	return DefaultInstanceCount
}

func (s *Speclet) MinNodeIDOrDefault() int {
	if s.MinNodeID != nil {
		return *s.MinNodeID
	}
	return DefaultMinNodeID
}

func (s *Speclet) CPULimitOrDefault() uint64 {
	if s.CPULimit != nil {
		return *s.CPULimit
	}
	return DefaultCPULimit
}

func (s *Speclet) MemoryLimitOrDefault() uint64 {
	if s.MemoryLimit != nil {
		return *s.MemoryLimit
	}
	return DefaultMemoryLimit
}

func (s *Speclet) InterconnectPortOrDefault() int {
	if s.InterconnectPort != nil {
		return *s.InterconnectPort
	}
	return DefaultInterconnectPort
}

func (s *Speclet) SameActorPortsOrDefault() bool {
	if s.SameActorPorts != nil {
		return *s.SameActorPorts
	}
	return DefaultSameActorPorts
}

func (s *Speclet) FileCacheDirOrDefault() string {
	value, _ := s.fileCacheDir() // ParseSpeclet has already validated it.
	return value
}

func (s *Speclet) BinaryCypressPathOrDefault(defaultPath string) string {
	if s.BinaryCypressPath != nil {
		return *s.BinaryCypressPath
	}
	return defaultPath
}

func (s *Speclet) fileCacheDir() (string, error) {
	if value, ok := s.Service["file_cache_dir"]; ok {
		result, ok := value.(string)
		if !ok || result == "" {
			return "", fmt.Errorf("service.file_cache_dir must be a nonempty string")
		}
		return result, nil
	}
	return DefaultFileCacheDir, nil
}

func (s *Speclet) needsRPCProxy() (bool, error) {
	result := false
	for _, key := range []string{"use_local_job_proxy_for_table_reads", "use_local_job_proxy_for_resource_downloads"} {
		if value, ok := s.Backend[key]; ok {
			flag, ok := value.(bool)
			if !ok {
				return false, fmt.Errorf("backend.%s must be a boolean", key)
			}
			result = result || flag
		}
	}
	return result, nil
}

func rejectSecrets(value any) error {
	switch value := value.(type) {
	case *yson.ValueWithAttrs:
		if err := rejectSecrets(value.Attrs); err != nil {
			return err
		}
		return rejectSecrets(value.Value)
	case map[string]any:
		for key, item := range value {
			normalized := strings.ToLower(strings.ReplaceAll(key, "_", ""))
			if normalized == "token" || normalized == "tokenfile" || normalized == "vaultenv" || normalized == "yttoken" || normalized == "yttokenfile" || normalized == "vaultfiles" || normalized == "securevault" || normalized == "secrets" {
				return fmt.Errorf("secrets and token source paths must be supplied through secure vault")
			}
			if err := rejectSecrets(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := rejectSecrets(item); err != nil {
				return err
			}
		}
	}
	return nil
}

var optionNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

func validateOptionNames(fields map[string]any, prefix string) error {
	for key := range fields {
		if !optionNamePattern.MatchString(key) {
			return fmt.Errorf("%s%s: option names must use snake_case", prefix, key)
		}
	}
	return nil
}

func validateConfigOptionNames(data map[string]any) error {
	if err := validateOptionNames(data, ""); err != nil {
		return err
	}
	// These map keys name options. Other maps, such as extra_env, contain
	// user data. Nested protobuf field names are checked by dq_service_process.
	for _, section := range []string{"service", "coordinator", "backend", "scheduler", "disk_request"} {
		value := data[section]
		if attributed, ok := value.(*yson.ValueWithAttrs); ok {
			value = attributed.Value
		}
		if fields, ok := value.(map[string]any); ok {
			if err := validateOptionNames(fields, section+"."); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseSpeclet(raw yson.RawValue) (Speclet, error) {
	var s Speclet
	if err := yson.Unmarshal(raw, &s); err != nil {
		return s, err
	}
	var data map[string]any
	if err := yson.Unmarshal(raw, &data); err != nil {
		return s, err
	}
	// Strawberry handles its own control fields, including live pool updates and
	// resume markers. Keep them out of the application config and restart comparison.
	s.RawConfig = maps.Clone(data)
	for _, field := range reflect.VisibleFields(reflect.TypeFor[strawberry.Speclet]()) {
		if !field.Anonymous && field.IsExported() {
			delete(s.RawConfig, strings.Split(field.Tag.Get("yson"), ",")[0])
		}
	}
	if len(data) == 0 {
		return s, nil
	} // Default option descriptors.
	if err := rejectSecrets(data); err != nil {
		return s, err
	}
	if err := validateConfigOptionNames(data); err != nil {
		return s, err
	}
	if s.InstanceCountOrDefault() <= 0 || s.MinNodeIDOrDefault() <= 0 || s.CPULimitOrDefault() <= 0 || s.MemoryLimitOrDefault() == 0 {
		return s, fmt.Errorf("instance_count, min_node_id, cpu_limit and memory_limit must be positive")
	}
	lastPort := s.InterconnectPortOrDefault()
	if !s.SameActorPortsOrDefault() {
		lastPort += s.InstanceCountOrDefault() - 1
	}
	if s.InterconnectPortOrDefault() < 1 || lastPort > 65535 {
		return s, fmt.Errorf("invalid interconnect port range")
	}
	if _, err := s.fileCacheDir(); err != nil {
		return s, err
	}
	if _, err := s.needsRPCProxy(); err != nil {
		return s, err
	}
	if disk, ok := data["disk_request"].(map[string]any); ok {
		for key := range disk {
			switch key {
			case "disk_space", "inode_count", "account", "medium_name":
			default:
				return s, fmt.Errorf("unsupported disk_request option %q", key)
			}
		}
	}
	for key, value := range s.ExtraEnv {
		switch key {
		case "ACTOR_PORT", "ACTOR_NODE_ID", "OPERATION_SIZE", "YT_OPERATION_ID", "YT_JOB_ID", "YT_JOB_PROXY_SOCKET_PATH", "YT_TOKEN":
			return s, fmt.Errorf("extra_env cannot override %s", key)
		}
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, '\x00') || strings.HasPrefix(key, "YT_SECURE_VAULT_") {
			return s, fmt.Errorf("invalid or reserved extra_env key %q", key)
		}
	}
	return s, nil
}
