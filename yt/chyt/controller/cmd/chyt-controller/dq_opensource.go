package main

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/app"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func registerDQController(cfs map[string]strawberry.ControllerFactory, config app.Config) {}

func registerDQClusterInitializer(
	familyToInitializerFactory map[string]strawberry.ClusterInitializerFactory,
	families []string,
) {
}

func dqControllerCtor() func(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, rawConfig yson.RawValue) strawberry.Controller {
	return nil
}

func isDQFamily(family string) bool {
	return false
}
