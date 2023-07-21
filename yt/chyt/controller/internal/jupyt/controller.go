package jupyt

import (
	"context"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Config struct {
}

type Controller struct {
	ytc     yt.Client
	l       log.Logger
	root    ypath.Path
	cluster string
	config  Config
}

func (c *Controller) UpdateState() (changed bool, err error) {
	return false, nil
}

func (c *Controller) buildCommand(speclet *Speclet) (command string, env map[string]string) {
	return "./jupyter-trampoline.sh", map[string]string{
		"PYTHON": "python" + speclet.PythonVersion,
	}
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]any, description map[string]any, annotations map[string]any, err error) {
	alias := oplet.Alias()

	// description = buildDescription(c.cluster, alias, c.config.EnableYandexSpecificLinksOrDefault())
	speclet := oplet.ControllerSpeclet().(Speclet)

	var filePaths []ypath.Rich

	description = map[string]any{}
	err = c.appendArtifacts(ctx, &speclet, &filePaths, &description)
	if err != nil {
		return
	}

	// Build command.
	command, env := c.buildCommand(&speclet)

	spec = map[string]any{
		"tasks": map[string]any{
			"jupyter": map[string]any{
				"command":                            command,
				"job_count":                          1,
				"file_paths":                         filePaths,
				"memory_limit":                       speclet.MemoryOrDefault(),
				"cpu_limit":                          speclet.CPUOrDefault(),
				"port_count":                         1,
				"max_stderr_size":                    1024 * 1024 * 1024,
				"user_job_memory_digest_lower_bound": 1.0,
				"environment":                        env,
			},
		},
		"max_failed_job_count": 10 * 1000,
		"max_stderr_count":     150,
		"title":                "JUPYT notebook *" + alias,
	}
	annotations = map[string]any{
		"is_notebook": true,
		"expose":      true,
	}

	return
}

func (c *Controller) Family() string {
	return "jupyt"
}

func (c *Controller) ParseSpeclet(specletYson yson.RawValue) (any, error) {
	var speclet Speclet
	err := yson.Unmarshal(specletYson, &speclet)
	if err != nil {
		return nil, yterrors.Err("failed to parse speclet", err)
	}
	return speclet, nil
}

func (c *Controller) DescribeOptions(parsedSpeclet any) []strawberry.OptionGroupDescriptor {
	return []strawberry.OptionGroupDescriptor{}
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
	return c
}
