package livy

import (
	"bytes"
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Config struct {
	DefaultSpeclet *Speclet `yson:"default_speclet"`
}

type Controller struct {
	root ypath.Path
}

func (c *Controller) UpdateState() (changed bool, err error) {
	return false, nil
}

func (c *Controller) GetControllerSnapshot() (yson.RawValue, error) {
	return make(yson.RawValue, 0), nil
}

func (c *Controller) IsControllerSnapshotOutdated(snapshot yson.RawValue, _ *strawberry.Oplet) (bool, error) {
	current, err := c.GetControllerSnapshot()
	if err != nil {
		return false, err
	}
	return !bytes.Equal(snapshot, current), nil
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	map[string]any, map[string]any, map[string]any, bool, error) {
	return nil, nil, nil, false, yterrors.Err(
		"starting oplets is not supported for deprecated strawberry family \"livy\"")
}

func (c *Controller) Family() string {
	return "livy"
}

func (c *Controller) Root() ypath.Path {
	return c.root
}

func (c *Controller) ParseSpeclet(specletYson yson.RawValue) (any, error) {
	return Speclet{}, nil
}

func (c *Controller) CheckState(ctx context.Context, oplet *strawberry.Oplet) (strawberry.ControllerOpletState, error) {
	return strawberry.ControllerOpletState{
		Health:       strawberry.OpletHealthGood,
		NeedsRestart: false,
		Reason:       "",
	}, nil
}

func (c *Controller) DescribeOptions(parsedSpeclet any) []strawberry.OptionGroupDescriptor {
	return nil
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any, opletInfo yson.RawValue) map[string]any {
	return nil
}

func (c *Controller) GetScalerTarget(ctx context.Context, opletInfo strawberry.OpletInfoForScaler) (*strawberry.ScalerTarget, error) {
	return nil, nil
}

type livyOpletInfo struct{}

func (c *Controller) GetOpletInfo(ctx context.Context, oplet *strawberry.Oplet) (any, error) {
	return livyOpletInfo{}, nil
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
	parseConfig(rawConfig)
	return &Controller{root: root}
}
