package strawberry

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type ClusterInitializer interface {
	// InitializeCluster does all the custom cluster initialization.
	InitializeCluster(robotUsername string) error

	// ACONamespace is an identifier for the access control nodes namespace.
	ACONamespace() string
}

type ClusterInitializerFactory = func(l log.Logger, ytc yt.Client, familyRoot ypath.Path) ClusterInitializer

// GrantMissingPermissions appends an allow entry for permissions the user lacks on path.
func GrantMissingPermissions(
	ctx context.Context,
	l log.Logger,
	ytc yt.Client,
	user string,
	path ypath.Path,
	permissions ...yt.Permission,
) error {
	var missingPermissions []yt.Permission
	for _, permission := range permissions {
		response, err := ytc.CheckPermission(ctx, user, permission, path, nil)
		if err != nil {
			return err
		}
		if response.Action != yt.ActionAllow {
			missingPermissions = append(missingPermissions, permission)
		}
	}
	if len(missingPermissions) == 0 {
		return nil
	}

	l.Info("granting permissions",
		log.String("path", path.String()),
		log.String("user", user),
		log.Any("permissions", missingPermissions))
	return ytc.SetNode(ctx, path.Attr("acl").Child("end"), yt.ACE{
		Action:      yt.ActionAllow,
		Subjects:    []string{user},
		Permissions: missingPermissions,
	}, nil)
}
