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
	spec map[string]interface{}, description map[string]interface{}, annotations map[string]interface{}, err error) {
	err = nil
	spec = map[string]interface{}{
		"tasks": map[string]interface{}{
			"main": map[string]interface{}{
				"command":   "sleep 10000",
				"job_count": 1,
			},
		}}
	description = map[string]interface{}{
		"sleeper_foo": "I am sleeper, look at me!",
	}
	annotations = map[string]interface{}{
		"sleeper_bar": "Actually I'd like to wake up :)",
	}
	return
}

func (c Controller) Family() string {
	return "sleep"
}

func (c Controller) ParseSpeclet(specletYson yson.RawValue) (any, error) {
	var speclet Speclet
	err := yson.Unmarshal(specletYson, &speclet)
	if err != nil {
		return nil, yterrors.Err("failed to parse sleep speclet", err)
	}
	return speclet, nil
}

func (c *Controller) TryUpdate() (bool, error) {
	var controllerParameter string
	err := c.ytc.GetNode(context.Background(), c.root.Attr("controller_parameter"), &controllerParameter, nil)
	if err != nil {
		return false, err
	}
	if c.controllerParameter != controllerParameter {
		c.controllerParameter = controllerParameter
		return true, nil
	}
	return false, nil
}

func NewController(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, config yson.RawValue) strawberry.Controller {
	return &Controller{
		ytc:                 ytc,
		root:                root,
		controllerParameter: "default",
	}
}
