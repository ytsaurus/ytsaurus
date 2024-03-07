package jupyt

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type ClusterInitializer struct {
	ytc yt.Client
}

func (initializer *ClusterInitializer) InitializeCluster() error {
	return nil
}

func (initializer *ClusterInitializer) ACONamespace() string {
	return "jupyt"
}

func NewClusterInitializer(l log.Logger, ytc yt.Client, root ypath.Path) strawberry.ClusterInitializer {
	return &ClusterInitializer{ytc: ytc}
}
