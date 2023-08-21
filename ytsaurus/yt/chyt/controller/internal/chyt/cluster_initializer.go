package chyt

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	DefaultDiscoveryV1Path = ypath.Path("//sys/clickhouse/cliques")
	YTClickHouseUser       = "yt-clickhouse"
)

type ClusterInitializer struct {
	l   log.Logger
	ytc yt.Client
}

func (initializer *ClusterInitializer) createNodesIfNotExist(ctx context.Context) error {
	_, err := initializer.ytc.CreateNode(ctx, DefaultDiscoveryV1Path, yt.NodeMap, &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	})
	return err
}

func (initializer *ClusterInitializer) createYTClickHouseUserIfNotExists(ctx context.Context) error {
	userPath := ypath.Path("//sys/users").Child(YTClickHouseUser)
	ok, err := initializer.ytc.NodeExists(ctx, userPath, nil)
	if err != nil {
		return err
	}
	if ok {
		initializer.l.Info("system clickhouse user already exists", log.String("username", YTClickHouseUser))
		return nil
	}

	initializer.l.Info("creating user yt-clickhouse")

	_, err = initializer.ytc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": YTClickHouseUser,
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (initializer *ClusterInitializer) addPermissionIfNeeded(ctx context.Context, path ypath.Path, acl yt.ACE) error {
	permissionsAreMissing := false
	for _, permission := range acl.Permissions {
		response, err := initializer.ytc.CheckPermission(ctx, YTClickHouseUser, permission, path, nil)
		if err != nil {
			return err
		}
		if response.Action != yt.ActionAllow {
			permissionsAreMissing = true
			break
		}
	}
	if !permissionsAreMissing {
		initializer.l.Info("enough permissions", log.String("path", path.String()), log.String("user", YTClickHouseUser))
		return nil
	}
	initializer.l.Info("setting permissions", log.String("path", path.String()), log.String("user", YTClickHouseUser))
	return initializer.ytc.SetNode(ctx, path.Attr("acl").Child("end"), acl, nil)
}

func (initializer *ClusterInitializer) setUpYTClickHouseUser(ctx context.Context) error {
	initializer.l.Info("configuring ACLs for yt-clickhouse")

	clickhouseACL := yt.ACE{
		Action:   yt.ActionAllow,
		Subjects: []string{YTClickHouseUser},
		Permissions: []yt.Permission{
			yt.PermissionWrite,
			yt.PermissionUse,
		},
	}
	err := initializer.addPermissionIfNeeded(ctx, ypath.Path("//sys/clickhouse"), clickhouseACL)
	if err != nil {
		return err
	}

	sysACL := yt.ACE{
		Action:   yt.ActionAllow,
		Subjects: []string{YTClickHouseUser},
		Permissions: []yt.Permission{
			yt.PermissionUse,
		},
	}
	err = initializer.addPermissionIfNeeded(ctx, ypath.Path("//sys/accounts/sys"), sysACL)
	if err != nil {
		return err
	}

	orchidACL := yt.ACE{
		Action:   yt.ActionAllow,
		Subjects: []string{YTClickHouseUser},
		Permissions: []yt.Permission{
			yt.PermissionCreate,
		},
	}
	return initializer.addPermissionIfNeeded(ctx, ypath.Path("//sys/schemas/orchid"), orchidACL)
}

func (initializer *ClusterInitializer) InitializeCluster() error {
	ctx := context.Background()
	if err := initializer.createNodesIfNotExist(ctx); err != nil {
		return err
	}
	if err := initializer.createYTClickHouseUserIfNotExists(ctx); err != nil {
		return err
	}
	if err := initializer.setUpYTClickHouseUser(ctx); err != nil {
		return err
	}
	return nil
}

func (initializer *ClusterInitializer) ACONamespace() string {
	return "chyt"
}

func NewClusterInitializer(l log.Logger, ytc yt.Client) strawberry.ClusterInitializer {
	return &ClusterInitializer{
		l:   l,
		ytc: ytc,
	}
}
