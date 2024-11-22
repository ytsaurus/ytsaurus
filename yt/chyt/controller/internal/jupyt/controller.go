package jupyt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Config struct {
	YTAuthCookieName    *string           `yson:"yt_auth_cookie_name"`
	YTProxy             *string           `yson:"yt_proxy"`
	ExtraEnvVars        map[string]string `yson:"extra_env_vars"`
	LastActivityURLPath *string           `yson:"last_activity_url_path"`
	Command             *string           `yson:"command"`
	AdditionalFiles     []string          `yson:"additional_files"`
}

const (
	DefaultYTAuthCookieName    = ""
	DefaultLastActivityURLPath = "api/status"
	DefaultCommand             = "bash -x start.sh /opt/conda/bin/jupyter lab --ip '*' --port $YT_PORT_0 --LabApp.token='' --allow-root >&2"
)

func (c *Config) YTAuthCookieNameOrDefault() string {
	if c.YTAuthCookieName != nil {
		return *c.YTAuthCookieName
	}
	return DefaultYTAuthCookieName
}

func (c *Config) LastActivityURLPathOrDefault() string {
	if c.LastActivityURLPath != nil {
		return *c.LastActivityURLPath
	}
	return DefaultLastActivityURLPath
}

func (c *Config) CommandOrDefault() string {
	if c.Command != nil {
		return *c.Command
	}
	return DefaultCommand
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
	cmd := c.config.CommandOrDefault()
	jupyterEnv := map[string]string{
		"NB_GID":  "0",
		"NB_UID":  "0",
		"NB_USER": "root",
		// Force ytsaurus-client to use the Docker image from this Jupyter kernel as the default Docker image for all operations.
		// ytsaurus-client uses pickle, which works reliably only when pickle and unpickle are performed on identical systems
		// (like the same docker image).
		"YT_BASE_LAYER": speclet.JupyterDockerImage,
	}
	for key, value := range c.config.ExtraEnvVars {
		jupyterEnv[key] = value
	}
	return cmd, jupyterEnv
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]any, description map[string]any, annotations map[string]any, err error) {
	alias := oplet.Alias()

	// description = buildDescription(c.cluster, alias, c.config.EnableYandexSpecificLinksOrDefault())
	speclet := oplet.ControllerSpeclet().(Speclet)

	description = map[string]any{}

	var filePaths []ypath.Rich

	err = c.prepareCypressDirectories(ctx, oplet.Alias())
	if err != nil {
		return
	}

	err = c.appendConfigs(ctx, oplet, &speclet, &filePaths)
	if err != nil {
		return
	}

	err = c.appendAdditionalFiles(&filePaths)
	if err != nil {
		return
	}

	command, env := c.buildCommand(&speclet)

	spec = map[string]any{
		"tasks": map[string]any{
			"jupyter": map[string]any{
				"command":                            command,
				"job_count":                          1,
				"docker_image":                       speclet.JupyterDockerImage,
				"memory_limit":                       speclet.MemoryOrDefault(),
				"cpu_limit":                          speclet.CPUOrDefault(),
				"gpu_limit":                          speclet.GPUOrDefault(),
				"file_paths":                         filePaths,
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

func (c *Controller) YTProxy() string {
	if c.config.YTProxy != nil {
		return *c.config.YTProxy
	}
	return c.cluster
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
	speclet := parsedSpeclet.(Speclet)

	return []strawberry.OptionGroupDescriptor{
		{
			Title: "Jupyt params",
			Options: []strawberry.OptionDescriptor{
				{
					Title:        "Docker image",
					Name:         "jupyter_docker_image",
					Type:         strawberry.TypeString,
					CurrentValue: speclet.JupyterDockerImage,
					Description:  "A docker image containing jupyt and required stuff.",
				},
				{
					Title:        "Enable idle timeout suspension",
					Name:         "enable_idle_suspension",
					Type:         strawberry.TypeBool,
					CurrentValue: speclet.EnableIdleSuspension,
					DefaultValue: false,
					Description:  "Jupyt operation will be suspended in case of no activity if enabled.",
				},
				{
					Title:        "Idle timeout",
					Name:         "idle_timeout",
					Type:         strawberry.TypeDuration,
					CurrentValue: speclet.IdleTimeout,
					DefaultValue: DefaultIdleTimeout,
					MinValue:     0,
					Description:  "Jupyt operation will be suspended in case of no activity for the specified time.",
				},
			},
		},
		{
			Title: "Resources",
			Options: []strawberry.OptionDescriptor{
				{
					Title:        "CPU",
					Name:         "cpu",
					Type:         strawberry.TypeInt64,
					CurrentValue: speclet.CPU,
					DefaultValue: DefaultCPU,
					MinValue:     1,
					MaxValue:     100,
					Description:  "Number of CPU cores.",
				},
				{
					Title:        "Total memory",
					Name:         "memory",
					Type:         strawberry.TypeByteCount,
					CurrentValue: speclet.Memory,
					DefaultValue: DefaultMemory,
					MinValue:     2 * gib,
					MaxValue:     300 * gib,
					Description:  "Amount of RAM in bytes.",
				},
				{
					Title:        "GPU",
					Name:         "gpu",
					Type:         strawberry.TypeInt64,
					CurrentValue: speclet.GPU,
					DefaultValue: DefaultGPU,
					MinValue:     0,
					MaxValue:     100,
					Description:  "Number of GPU cards.",
				},
			},
		},
	}
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any) map[string]any {
	speclet := parsedSpeclet.(Speclet)

	return map[string]any{
		"cpu":          speclet.CPUOrDefault(),
		"memory":       speclet.MemoryOrDefault(),
		"gpu":          speclet.GPUOrDefault(),
		"docker_image": speclet.JupyterDockerImage,
	}
}

func (c *Controller) appendConfigs(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet, filePaths *[]ypath.Rich) error {
	serverConfig := jupytServerConfig{
		YTProxy:          c.YTProxy(),
		YTAuthCookieName: c.config.YTAuthCookieNameOrDefault(),
		YTACOName:        oplet.Alias(),
		YTACONamespace:   c.Family(),
		YTACORootPath:    strawberry.AccessControlNamespacesPath.String(),
	}
	serverConfigYTPath, err := c.uploadConfig(ctx, oplet.Alias(), "server_config.json", serverConfig)
	if err != nil {
		return nil
	}
	*filePaths = append(*filePaths, serverConfigYTPath)
	return nil
}

func (c *Controller) appendAdditionalFiles(filePaths *[]ypath.Rich) error {
	for _, additionalFilePath := range c.config.AdditionalFiles {
		var richPath *ypath.Rich
		richPath, err := ypath.Parse(additionalFilePath)
		if err != nil {
			return err
		}
		*filePaths = append(*filePaths, *richPath)
	}
	return nil
}

func (c *Controller) GetScalerTarget(ctx context.Context, opletInfo strawberry.OpletInfoForScaler) (*strawberry.ScalerTarget, error) {
	speclet := opletInfo.ControllerSpeclet.(Speclet)

	if !speclet.EnableIdleSuspension {
		return nil, nil
	}

	endpointInfo, err := getEndpoint(ctx, c.ytc, opletInfo.OperationID)
	if err != nil {
		return nil, err
	}

	lastActivityRelURL := c.config.LastActivityURLPathOrDefault()
	lastActivityURL := fmt.Sprintf("%s/%s", endpointInfo.Address, lastActivityRelURL)
	resp, err := http.Get(lastActivityURL)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jupyter responded with error: %s", body)
	}
	var jupyterResponse struct {
		LastActivity time.Time `json:"last_activity"`
	}
	err = json.Unmarshal(body, &jupyterResponse)
	if err != nil {
		return nil, err
	}

	sinceLastActivity := time.Since(jupyterResponse.LastActivity)
	if sinceLastActivity > speclet.IdleTimeoutOrDefault() {
		reason := fmt.Sprintf("idle time %d > %d", sinceLastActivity, speclet.IdleTimeout)
		c.l.Info(
			"oplet should be suspended",
			log.String("alias", opletInfo.Alias),
			log.String("reason", reason),
		)
		return &strawberry.ScalerTarget{
			InstanceCount: 0,
			Reason:        reason,
		}, nil
	}

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
	c := &Controller{
		l:       l,
		ytc:     ytc,
		root:    root,
		cluster: cluster,
		config:  parseConfig(rawConfig),
	}
	return c
}
