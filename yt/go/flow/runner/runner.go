// Package runner prepares and launches a Go companion pipeline.
package runner

import (
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

const (
	companionManagerClass    = "NYT::NFlow::NCompanion::TCompanionManager"
	companionWorkerPortCount = 3
)

var (
	// ErrMissingConfig reports a launch that did not name a pipeline config.
	ErrMissingConfig = xerrors.NewSentinel("--config <pipeline.yson> is required")

	// ErrMissingFlowBin reports a launch that did not name a flow_server binary.
	ErrMissingFlowBin = xerrors.NewSentinel("--flow-bin <path to flow_server> is required")

	// ErrMalformedConfig reports a pipeline config that is not a YSON map.
	ErrMalformedConfig = xerrors.NewSentinel("malformed pipeline config")

	// ErrStreamSchemaConflict reports a registered schema that disagrees with the config.
	ErrStreamSchemaConflict = xerrors.NewSentinel("registered stream schema conflicts with pipeline config")
)

// Args is a launcher command line.
type Args struct {
	ConfigPath string
	FlowBin    string
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
	if args.FlowBin == "" {
		return Args{}, xerrors.Errorf("flow/runner: %w", ErrMissingFlowBin)
	}
	return args, nil
}

// Launch enriches the config and replaces the process with flow_server.
func Launch(args Args, streamSchemas map[string]schema.Schema) error {
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

	flowBin, err := filepath.Abs(args.FlowBin)
	if err != nil {
		return xerrors.Errorf("flow/runner: resolve %q: %w", args.FlowBin, err)
	}

	if err := syscall.Exec(flowBin, []string{flowBin, "--config", extendedPath}, os.Environ()); err != nil {
		return xerrors.Errorf("flow/runner: exec %s: %w", flowBin, err)
	}
	return nil
}

// Enrich configures vanilla workers to run companionPath.
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
		patchCompanionResources(spec)
		addLocalFile(vanilla, CompanionFileName, companionPath)
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

func patchCompanionResources(spec map[string]any) {
	resources, ok := asMap(spec["resources"])
	if !ok {
		return
	}

	for _, definition := range resources {
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
		parameters["entrypoint"] = map[string]any{"executable": "./" + CompanionFileName}
		parameters["run_process"] = true
	}
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
		return
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
