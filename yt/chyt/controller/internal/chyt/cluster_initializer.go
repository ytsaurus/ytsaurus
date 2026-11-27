package chyt

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	YTClickHouseUser             = "yt-clickhouse"
	YTClickHouseCacheUser        = "yt-clickhouse-cache"
	YTClickHouseDictionariesUser = "yt-clickhouse-dictionaries"
	CHYTSQLObjectsUser           = "chyt-sql-objects"
)

type ClusterInitializer struct {
	l          log.Logger
	ytc        yt.Client
	familyRoot ypath.Path
}

func (initializer *ClusterInitializer) createNodesIfNotExist(ctx context.Context) error {
	for _, path := range []ypath.Path{ypath.Path("//sys/clickhouse"), CHYTBinaryDirectory, TrampolineBinaryDirectory} {
		_, err := initializer.ytc.CreateNode(ctx, path, yt.NodeMap, &yt.CreateNodeOptions{
			Recursive:      true,
			IgnoreExisting: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (initializer *ClusterInitializer) setUpIDMRoles(ctx context.Context) error {
	idmRoles := map[string]any{
		"manage": map[string]any{
			"idm_name": "Manage",
			"permissions": []yt.Permission{
				yt.PermissionRead,
				yt.PermissionManage,
				yt.PermissionRemove,
				yt.PermissionUse,
			},
		},
		"use": map[string]any{
			"idm_name": "Use",
			"permissions": []yt.Permission{
				yt.PermissionRead,
				yt.PermissionUse,
			},
		},
	}
	path := ypath.Path("//sys/access_control_object_namespaces").
		Child(initializer.ACONamespace()).
		Attr("idm_roles")
	return initializer.ytc.SetNode(ctx, path, idmRoles, nil)
}

func (initializer *ClusterInitializer) createUserIfNotExists(ctx context.Context, user string) error {
	userPath := ypath.Path("//sys/users").Child(user)
	ok, err := initializer.ytc.NodeExists(ctx, userPath, nil)
	if err != nil {
		return err
	}
	if ok {
		initializer.l.Info("user already exists", log.String("user", user))
		return nil
	}

	initializer.l.Info("creating user", log.String("user", user))

	_, err = initializer.ytc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": user,
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (initializer *ClusterInitializer) setUpYTClickHouseUser(ctx context.Context) error {
	if err := initializer.createUserIfNotExists(ctx, YTClickHouseUser); err != nil {
		return err
	}

	initializer.l.Info("configuring ACLs for user yt-clickhouse")

	if err := strawberry.GrantMissingPermissions(
		ctx, initializer.l, initializer.ytc,
		YTClickHouseUser, "//sys/clickhouse", yt.PermissionWrite, yt.PermissionUse,
	); err != nil {
		return err
	}

	if err := strawberry.GrantMissingPermissions(
		ctx, initializer.l, initializer.ytc,
		YTClickHouseUser, "//sys/accounts/sys", yt.PermissionUse,
	); err != nil {
		return err
	}

	if err := strawberry.GrantMissingPermissions(
		ctx, initializer.l, initializer.ytc,
		YTClickHouseUser, "//sys/schemas/orchid", yt.PermissionCreate,
	); err != nil {
		return err
	}

	return strawberry.GrantMissingPermissions(
		ctx, initializer.l, initializer.ytc,
		YTClickHouseUser, initializer.familyRoot,
		yt.PermissionWrite, yt.PermissionRead, yt.PermissionRemove, yt.PermissionMount,
	)
}

func (initializer *ClusterInitializer) setUpCHYTSQLObjectsUser(ctx context.Context) error {
	if err := initializer.createUserIfNotExists(ctx, CHYTSQLObjectsUser); err != nil {
		return err
	}

	initializer.l.Info("configuring ACLs for user chyt-sql-objects")

	if err := strawberry.GrantMissingPermissions(
		ctx, initializer.l, initializer.ytc,
		CHYTSQLObjectsUser, initializer.familyRoot,
		yt.PermissionWrite, yt.PermissionRead, yt.PermissionRemove,
	); err != nil {
		return err
	}

	return strawberry.GrantMissingPermissions(
		ctx, initializer.l, initializer.ytc,
		CHYTSQLObjectsUser, "//sys/accounts/sys", yt.PermissionUse,
	)
}

func (initializer *ClusterInitializer) InitializeCluster(robotUsername string) error {
	ctx := context.Background()
	if err := initializer.createNodesIfNotExist(ctx); err != nil {
		return err
	}
	if err := initializer.setUpIDMRoles(ctx); err != nil {
		return err
	}
	if err := initializer.setUpYTClickHouseUser(ctx); err != nil {
		return err
	}
	if err := initializer.setUpCHYTSQLObjectsUser(ctx); err != nil {
		return err
	}
	if err := initializer.createUserIfNotExists(ctx, YTClickHouseDictionariesUser); err != nil {
		return err
	}
	if err := initializer.createUserIfNotExists(ctx, YTClickHouseCacheUser); err != nil {
		return err
	}
	return initializer.setUpRobotPermissions(ctx, robotUsername)
}

func (initializer *ClusterInitializer) ACONamespace() string {
	return "chyt"
}

func (initializer *ClusterInitializer) setUpRobotPermissions(ctx context.Context, robotUsername string) error {
	if robotUsername == "" {
		return nil
	}

	for _, path := range []ypath.Path{ypath.Path("//sys/clickhouse"), CHYTBinaryDirectory, TrampolineBinaryDirectory} {
		if err := strawberry.GrantMissingPermissions(
			ctx, initializer.l, initializer.ytc,
			robotUsername, path, yt.PermissionRead, yt.PermissionWrite, yt.PermissionRemove,
		); err != nil {
			return err
		}
	}
	return nil
}

func NewClusterInitializer(l log.Logger, ytc yt.Client, familyRoot ypath.Path) strawberry.ClusterInitializer {
	return &ClusterInitializer{
		l:          l,
		ytc:        ytc,
		familyRoot: familyRoot,
	}
}
