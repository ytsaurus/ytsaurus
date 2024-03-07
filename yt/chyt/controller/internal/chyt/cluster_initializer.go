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
	CHYTSQLObjectsUser     = "chyt-sql-objects"
)

type ClusterInitializer struct {
	l    log.Logger
	ytc  yt.Client
	root ypath.Path
}

func (initializer *ClusterInitializer) createNodesIfNotExist(ctx context.Context) error {
	_, err := initializer.ytc.CreateNode(ctx, DefaultDiscoveryV1Path, yt.NodeMap, &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	})
	return err
}

func (initializer *ClusterInitializer) setUpIDMRoles(ctx context.Context) error {
	idmRoles := map[string]any{
		"manage": map[string]any{
			"idm_name": "Manage",
			"permissions": []yt.Permission{
				yt.PermissionRead,
				yt.PermissionManage,
				yt.PermissionRemove,
			},
		},
		"use": map[string]any{
			"idm_name": "Use",
			"permissions": []yt.Permission{
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

func (initializer *ClusterInitializer) addPermissionIfNeeded(ctx context.Context, user string, path ypath.Path, acl yt.ACE) error {
	permissionsAreMissing := false
	for _, permission := range acl.Permissions {
		response, err := initializer.ytc.CheckPermission(ctx, user, permission, path, nil)
		if err != nil {
			return err
		}
		if response.Action != yt.ActionAllow {
			permissionsAreMissing = true
			break
		}
	}
	if !permissionsAreMissing {
		initializer.l.Info("enough permissions", log.String("path", path.String()), log.String("user", user))
		return nil
	}
	initializer.l.Info("setting permissions", log.String("path", path.String()), log.String("user", user))
	return initializer.ytc.SetNode(ctx, path.Attr("acl").Child("end"), acl, nil)
}

func (initializer *ClusterInitializer) setUpYTClickHouseUser(ctx context.Context) error {
	if err := initializer.createUserIfNotExists(ctx, YTClickHouseUser); err != nil {
		return err
	}

	initializer.l.Info("configuring ACLs for user yt-clickhouse")

	clickhouseACL := yt.ACE{
		Action:   yt.ActionAllow,
		Subjects: []string{YTClickHouseUser},
		Permissions: []yt.Permission{
			yt.PermissionWrite,
			yt.PermissionUse,
		},
	}
	err := initializer.addPermissionIfNeeded(ctx, YTClickHouseUser, "//sys/clickhouse", clickhouseACL)
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
	err = initializer.addPermissionIfNeeded(ctx, YTClickHouseUser, ypath.Path("//sys/accounts/sys"), sysACL)
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
	return initializer.addPermissionIfNeeded(ctx, YTClickHouseUser, ypath.Path("//sys/schemas/orchid"), orchidACL)
}

func (initializer *ClusterInitializer) setUpCHYTSQLObjectsUser(ctx context.Context) error {
	if err := initializer.createUserIfNotExists(ctx, CHYTSQLObjectsUser); err != nil {
		return err
	}

	initializer.l.Info("configuring ACLs for user chyt-sql-objects")

	rootACL := yt.ACE{
		Action:   yt.ActionAllow,
		Subjects: []string{CHYTSQLObjectsUser},
		Permissions: []yt.Permission{
			yt.PermissionWrite,
			yt.PermissionRead,
			yt.PermissionRemove,
		},
	}
	err := initializer.addPermissionIfNeeded(ctx, CHYTSQLObjectsUser, initializer.root, rootACL)
	if err != nil {
		return err
	}

	sysACL := yt.ACE{
		Action:   yt.ActionAllow,
		Subjects: []string{CHYTSQLObjectsUser},
		Permissions: []yt.Permission{
			yt.PermissionUse,
		},
	}
	return initializer.addPermissionIfNeeded(ctx, CHYTSQLObjectsUser, ypath.Path("//sys/accounts/sys"), sysACL)
}

func (initializer *ClusterInitializer) InitializeCluster() error {
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
	return nil
}

func (initializer *ClusterInitializer) ACONamespace() string {
	return "chyt"
}

func NewClusterInitializer(l log.Logger, ytc yt.Client, root ypath.Path) strawberry.ClusterInitializer {
	return &ClusterInitializer{
		l:    l,
		ytc:  ytc,
		root: root,
	}
}
