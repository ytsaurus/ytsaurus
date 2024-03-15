package jupyt

import (
	"context"
	"fmt"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	JupytPort = 27042
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
	// TODO(max): take port from YT_PORT_0.
	// TODO(max): come up with a solution how to pass secrets (token or password) without exposing them in the
	// strawberry attributes.
	cmd := fmt.Sprintf(
		"bash -x start.sh /opt/conda/bin/jupyter lab --ip '*' --port %v --LabApp.token='' --allow-root >&2", JupytPort)
	return cmd, map[string]string{
		"NB_GID":  "0",
		"NB_UID":  "0",
		"NB_USER": "root",
	}
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]any, description map[string]any, annotations map[string]any, err error) {
	alias := oplet.Alias()

	// description = buildDescription(c.cluster, alias, c.config.EnableYandexSpecificLinksOrDefault())
	speclet := oplet.ControllerSpeclet().(Speclet)

	description = map[string]any{}

	// Build command.
	command, env := c.buildCommand(&speclet)

	spec = map[string]any{
		"tasks": map[string]any{
			"jupyter": map[string]any{
				"command":                            command,
				"job_count":                          1,
				"docker_image":                       speclet.JupyterDockerImage,
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

func (c *Controller) Root() ypath.Path {
	return c.root
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
	return nil
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any) map[string]any {
	return nil
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
