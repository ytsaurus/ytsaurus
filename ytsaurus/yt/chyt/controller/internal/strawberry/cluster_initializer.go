package strawberry

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type ClusterInitializer interface {
	// InitializeCluster does all the custom cluster initialization.
	InitializeCluster() error

	// ACONamespace is an identifier for the access control nodes namespace.
	ACONamespace() string
}

type ClusterInitializerFactory = func(l log.Logger, ytc yt.Client) ClusterInitializer
