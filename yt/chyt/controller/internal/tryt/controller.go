package tryt

import (
	"context"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var (
	_ strawberry.Controller = (*Controller)(nil)
)

type Controller struct {
	ytc     yt.Client
	l       log.Logger
	root    ypath.Path
	cluster string
	config  Config
}

func (c *Controller) Prepare(
	ctx context.Context,
	oplet *strawberry.Oplet,
) (
	spec map[string]any,
	description map[string]any,
	annotation map[string]any,
	err error,
) {
	return
}

func (c *Controller) Family() string {
	return "tryt"
}

func (c *Controller) Root() ypath.Path {
	return c.root
}

func (c *Controller) ParseSpeclet(specletYson yson.RawValue) (parsedSpeclet any, err error) {
	var speclet Speclet
	err = yson.Unmarshal(specletYson, &speclet)
	if err != nil {
		return nil, yterrors.Err("failed to parse speclet", err)
	}
	return speclet, nil
}

func (c *Controller) UpdateState() (changed bool, err error) {
	return false, nil
}

func (c *Controller) DescribeOptions(parsedSpeclet any) []strawberry.OptionGroupDescriptor {
	speclet := parsedSpeclet.(Speclet)

	return []strawberry.OptionGroupDescriptor{
		{
			Title: "Transfer params",
			Options: []strawberry.OptionDescriptor{
				{
					Title:        "Docker image",
					Name:         "jupyter_docker_image",
					Type:         strawberry.TypeString,
					CurrentValue: speclet.DockerImage,
					Description:  "A docker image containing jupyt and required stuff.",
				},
				{
					Title:        "Type of source",
					Name:         "src_type",
					Type:         strawberry.TypeString,
					CurrentValue: speclet.SourceType,
					DefaultValue: "pg",
					Description:  "Type of source endpoint, for example: kafka, pg, mysql, mongo.",
				},
				{
					Title:        "Source params",
					Name:         "src_params",
					Type:         strawberry.TypeString,
					CurrentValue: speclet.SourceParams,
					DefaultValue: "{}",
					Description:  "JSON-formatted params for source config.",
				},
				{
					Title:        "Type of destinations",
					Name:         "dst_type",
					Type:         strawberry.TypeString,
					CurrentValue: speclet.DestinationType,
					DefaultValue: "yt",
					Description:  "Type of destination endpoint, for example: yt.",
				},
				{
					Title:        "Destination params",
					Name:         "dst_params",
					Type:         strawberry.TypeString,
					CurrentValue: speclet.DestinationParams,
					DefaultValue: "{}",
					Description:  "JSON-formatted params for source config.",
				},
				{
					Title:        "Transfer type",
					Name:         "transfer_type",
					Type:         strawberry.TypeDuration,
					CurrentValue: speclet.TransferType,
					DefaultValue: "SNAPSHOT_AND_INCREMENT",
					Description:  "Transfer type: one of: SNAPSHOT_ONLY, INCREMENT_ONLY, SNAPSHOT_AND_INCREMENT",
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
					MaxValue:     128 * gib,
					Description:  "Amount of RAM in bytes.",
				},
			},
		},
	}
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any) map[string]any {
	speclet := parsedSpeclet.(Speclet)

	return map[string]any{
		"cpu":    speclet.CPUOrDefault(),
		"memory": speclet.MemoryOrDefault(),

		"transfer_type": speclet.TransferType,
		"src":           speclet.SourceType,
		"dst":           speclet.DestinationType,
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
	c := &Controller{
		l:       l,
		ytc:     ytc,
		root:    root,
		cluster: cluster,
		config:  parseConfig(rawConfig),
	}
	return c
}
