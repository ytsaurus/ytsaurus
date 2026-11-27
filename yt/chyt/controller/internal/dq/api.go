package dq

import (
	"context"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func ListInstances(a *api.API, ctx context.Context, alias string) ([]map[string]any, error) {
	ctl, ok := a.Controller().(*Controller)
	if !ok {
		return nil, yterrors.Err("dq controller is required for list_instances command")
	}
	return ctl.ListDiscoveryInstances(ctx, alias)
}
