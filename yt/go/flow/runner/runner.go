// Package runner prepares and launches a Go companion pipeline.
package runner

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

// CompanionFileName is the pipeline binary name in the job sandbox.
const CompanionFileName = "go_companion"

// FlowBinEnvVar names flow_server when --flow-bin is not given.
const FlowBinEnvVar = "YT_FLOW_BIN"

const shippedExecutable = "./" + CompanionFileName

const (
	companionManagerClass    = "NYT::NFlow::NCompanion::TCompanionManager"
	companionWorkerPortCount = 3
)

var (
	// ErrMissingConfig reports a launch that did not name a pipeline config.
	ErrMissingConfig = xerrors.NewSentinel("--config <pipeline.yson> is required")

	// ErrMissingFlowBin reports a launch that named no flow_server binary.
	ErrMissingFlowBin = xerrors.NewSentinel(
		"flow_server is not given: pass --flow-bin <path to flow_server> or set " + FlowBinEnvVar)

	// ErrMalformedConfig reports a pipeline config that is not a YSON map.
	ErrMalformedConfig = xerrors.NewSentinel("malformed pipeline config")

	// ErrStreamSchemaConflict reports a registered schema that disagrees with the config.
	ErrStreamSchemaConflict = xerrors.NewSentinel("registered stream schema conflicts with pipeline config")
)

// Args is a launcher command line.
type Args struct {
	ConfigPath string
	// FlowBin is empty when --flow-bin is not given; Launch then resolves it by ResolveFlowBin.
	FlowBin string
}

// ParseArgs reads launcher flags from argv.
func ParseArgs(argv []string) (Args, error) {
	var args Args

	rest := argv
	if len(rest) > 0 {
		rest = rest[1:]
	}

	for i := 0; i < len(rest); i++ {
		name, value, hasValue := strings.Cut(rest[i], "=")

		var target *string
		switch name {
		case "--config", "-config":
			target = &args.ConfigPath
		case "--flow-bin", "-flow-bin":
			target = &args.FlowBin
		default:
			continue
		}

		if !hasValue {
			if i+1 == len(rest) {
				return Args{}, xerrors.Errorf("flow/runner: %s expects a value", name)
			}
			i++
			value = rest[i]
		}
		*target = value
	}

	if args.ConfigPath == "" {
		return Args{}, xerrors.Errorf("flow/runner: %w", ErrMissingConfig)
	}
	return args, nil
}

// ResolveFlowBin returns the absolute path of flow_server: flowBin if not empty, else
// $YT_FLOW_BIN.
func ResolveFlowBin(flowBin string) (string, error) {
	if flowBin == "" {
		flowBin = os.Getenv(FlowBinEnvVar)
	}
	if flowBin == "" {
		return "", xerrors.Errorf("flow/runner: %w", ErrMissingFlowBin)
	}

	abs, err := filepath.Abs(flowBin)
	if err != nil {
		return "", xerrors.Errorf("flow/runner: resolve %q: %w", flowBin, err)
	}
	return abs, nil
}

// Launch enriches the config and replaces the process with flow_server.
func Launch(args Args, streamSchemas map[string]schema.Schema) error {
	flowBin, err := ResolveFlowBin(args.FlowBin)
	if err != nil {
		return err
	}

	pipelineConfig, err := os.ReadFile(args.ConfigPath)
	if err != nil {
		return xerrors.Errorf("flow/runner: read pipeline config: %w", err)
	}

	// argv[0] need not identify the running binary.
	companionPath, err := os.Executable()
	if err != nil {
		return xerrors.Errorf("flow/runner: locate pipeline binary: %w", err)
	}

	extended, err := Enrich(pipelineConfig, companionPath, streamSchemas)
	if err != nil {
		return err
	}

	extendedPath, err := writeExtendedConfig(extended)
	if err != nil {
		return err
	}

	if err := syscall.Exec(flowBin, []string{flowBin, "--config", extendedPath}, os.Environ()); err != nil {
		return xerrors.Errorf("flow/runner: exec %s: %w", flowBin, err)
	}
	return nil
}

// Enrich configures vanilla workers to run companionPath. A companion resource that declares its
// entrypoint keeps it, and when every one does, companionPath is not shipped: the job environment,
// e.g. the docker image, provides the companion.
func Enrich(pipelineConfig []byte, companionPath string, streamSchemas map[string]schema.Schema) ([]byte, error) {
	var config any
	if err := yson.Unmarshal(pipelineConfig, &config); err != nil {
		return nil, xerrors.Errorf("flow/runner: parse pipeline config: %w", err)
	}

	root, ok := asMap(config)
	if !ok {
		return nil, xerrors.Errorf("flow/runner: %w: root is not a map", ErrMalformedConfig)
	}

	spec, _ := asMap(root["spec"])
	if err := patchStreamSchemas(spec, streamSchemas); err != nil {
		return nil, err
	}
	if vanilla, ok := asMap(root["vanilla"]); ok && enabled(vanilla) {
		// One shipment serves every resource without an entrypoint; a resource that declares one
		// keeps it, so a mixed spec still ships.
		if patchCompanionResources(spec) {
			addLocalFile(vanilla, CompanionFileName, companionPath)
		} else {
			log.Print("flow/runner: every companion resource declares its entrypoint: shipping no companion binary, the job environment provides it")
		}
		// The worker talks to the companion over a port either way.
		ensureCompanionPortCount(vanilla)
	}

	extended, err := yson.MarshalFormat(config, yson.FormatPretty)
	if err != nil {
		return nil, xerrors.Errorf("flow/runner: serialize pipeline config: %w", err)
	}
	return extended, nil
}

func patchStreamSchemas(spec map[string]any, schemas map[string]schema.Schema) error {
	if spec == nil || len(schemas) == 0 {
		return nil
	}
	streams, ok := asMap(spec["streams"])
	if !ok {
		streams = map[string]any{}
		spec["streams"] = streams
	}
	for id, table := range schemas {
		definition, ok := asMap(streams[id])
		if !ok {
			definition = map[string]any{}
			streams[id] = definition
		}
		if configured, ok := definition["schema"]; ok {
			raw, err := yson.Marshal(configured)
			if err != nil {
				return xerrors.Errorf("flow/runner: stream %q schema: %w", id, err)
			}
			var existing schema.Schema
			if err := yson.Unmarshal(raw, &existing); err != nil {
				return xerrors.Errorf("flow/runner: stream %q schema: %w", id, err)
			}
			if !existing.Equal(table) {
				return xerrors.Errorf("flow/runner: stream %q: %w", id, ErrStreamSchemaConflict)
			}
			continue
		}
		definition["schema"] = table
	}
	return nil
}

// patchCompanionResources points every companion resource without a declared entrypoint at the
// shipped binary and reports whether some resource needs it.
func patchCompanionResources(spec map[string]any) bool {
	resources, ok := asMap(spec["resources"])
	if !ok {
		return false
	}

	needsShippedBinary := false
	for id, definition := range resources {
		resource, ok := asMap(definition)
		if !ok {
			continue
		}
		if className, _ := yson.ValueOf(resource["resource_class_name"]).(string); className != companionManagerClass {
			continue
		}

		parameters, ok := asMap(resource["parameters"])
		if !ok {
			parameters = map[string]any{}
			resource["parameters"] = parameters
		}
		parameters["run_process"] = true
		if executable := declaredExecutable(parameters); executable != "" {
			log.Printf("flow/runner: companion resource %s: entrypoint %s from the job environment, as declared", id, executable)
			continue
		}
		parameters["entrypoint"] = map[string]any{"executable": shippedExecutable}
		needsShippedBinary = true
		log.Printf("flow/runner: companion resource %s: entrypoint %s of the shipped binary", id, shippedExecutable)
	}
	return needsShippedBinary
}

// declaredExecutable returns the declared entrypoint executable, or "" when the spec leaves it out
// or blank. The runner's own ./go_companion names the shipped binary, so it counts as not declared.
func declaredExecutable(parameters map[string]any) string {
	entrypoint, ok := asMap(parameters["entrypoint"])
	if !ok {
		return ""
	}
	executable, _ := yson.ValueOf(entrypoint["executable"]).(string)
	executable = strings.TrimSpace(executable)
	if executable == shippedExecutable {
		return ""
	}
	return executable
}

func addLocalFile(vanilla map[string]any, name, path string) {
	worker, ok := asMap(vanilla["worker"])
	if !ok {
		worker = map[string]any{}
		vanilla["worker"] = worker
	}

	localFiles, ok := asMap(worker["local_files"])
	if !ok {
		localFiles = map[string]any{}
		worker["local_files"] = localFiles
	}
	localFiles[name] = path
}

func ensureCompanionPortCount(vanilla map[string]any) {
	worker, ok := asMap(vanilla["worker"])
	if !ok {
		worker = map[string]any{}
		vanilla["worker"] = worker
	}

	switch portCount := yson.ValueOf(worker["port_count"]).(type) {
	case int64:
		if portCount >= companionWorkerPortCount {
			return
		}
	case uint64:
		if portCount >= companionWorkerPortCount {
			return
		}
	case nil:
	default:
		return
	}
	worker["port_count"] = companionWorkerPortCount
}

func enabled(vanilla map[string]any) bool {
	enable, _ := yson.ValueOf(vanilla["enable"]).(bool)
	return enable
}

func asMap(node any) (map[string]any, bool) {
	m, ok := yson.ValueOf(node).(map[string]any)
	return m, ok
}

// The config must outlive exec, so it stays in an owner-only temporary directory.
func writeExtendedConfig(extended []byte) (string, error) {
	dir, err := os.MkdirTemp("", "flow_runner_")
	if err != nil {
		return "", xerrors.Errorf("flow/runner: create temporary directory: %w", err)
	}

	path := filepath.Join(dir, "extended-pipeline.yson")
	if err := os.WriteFile(path, extended, 0o600); err != nil {
		return "", xerrors.Errorf("flow/runner: write extended pipeline config: %w", err)
	}
	return path, nil
}
