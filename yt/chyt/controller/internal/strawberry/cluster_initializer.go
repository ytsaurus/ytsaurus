package strawberry

import (
	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/yt"
)

type ClusterInitializer interface {
	// InitializeCluster does all the custom cluster initialization.
	InitializeCluster() error

	// ACONamespace is an identifier for the access control nodes namespace.
	ACONamespace() string
}

type ClusterInitializerFactory = func(l log.Logger, ytc yt.Client) ClusterInitializer
