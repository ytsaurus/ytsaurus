package sleep

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Speclet struct {
	TestOption *uint64 `yson:"test_option"`
}

type Controller struct {
	ytc                 yt.Client
	root                ypath.Path
	controllerParameter string
}

func (c Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]any, description map[string]any, annotations map[string]any, err error) {
	err = nil
	spec = map[string]any{
		"tasks": map[string]any{
			"main": map[string]any{
				"command":   "sleep 10000",
				"job_count": 1,
			},
		}}
	description = map[string]any{
		"sleeper_foo": "I am sleeper, look at me!",
	}
	annotations = map[string]any{
		"sleeper_bar":          "Actually I'd like to wake up :)",
		"controller_parameter": c.controllerParameter,
	}
	return
}

func (c Controller) Family() string {
	return "sleep"
}

func (c *Controller) Root() ypath.Path {
	return c.root
}

func (c Controller) ParseSpeclet(specletYson yson.RawValue) (any, error) {
	var speclet Speclet
	if specletYson == nil {
		return speclet, nil
	}
	err := yson.Unmarshal(specletYson, &speclet)
	if err != nil {
		return nil, yterrors.Err("failed to parse sleep speclet", err)
	}
	return speclet, nil
}

func (c *Controller) UpdateState() (changed bool, err error) {
	var controllerParameter string
	err = c.ytc.GetNode(context.Background(), c.root.Attr("controller_parameter"), &controllerParameter, nil)
	if err != nil {
		return false, err
	}
	if c.controllerParameter != controllerParameter {
		c.controllerParameter = controllerParameter
		return true, nil
	}
	return false, nil
}

func (c *Controller) DescribeOptions(parsedSpeclet any) []strawberry.OptionGroupDescriptor {
	speclet := parsedSpeclet.(Speclet)

	return []strawberry.OptionGroupDescriptor{
		{
			Title: "Sleep options",
			Options: []strawberry.OptionDescriptor{
				{
					Name:         "test_option",
					Type:         strawberry.TypeUInt64,
					CurrentValue: speclet.TestOption,
				},
			},
		},
	}
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any) map[string]any {
	speclet := parsedSpeclet.(Speclet)
	return map[string]any{
		"test_option": speclet.TestOption,
	}
}

func NewController(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, config yson.RawValue) strawberry.Controller {
	return &Controller{
		ytc:                 ytc,
		root:                root,
		controllerParameter: "default",
	}
}
