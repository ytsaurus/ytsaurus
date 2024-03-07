package app

import (
	"context"
	"fmt"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type ClusterInitializerConfig struct {
	BaseConfig

	// RobotUsername is the name of the robot from which all controller operations are done.
	RobotUsername string `yson:"robot_username"`

	Families []string `yson:"families"`
}

type ClusterInitializer struct {
	ytc                 yt.Client
	l                   log.Logger
	config              ClusterInitializerConfig
	familyToInitializer map[string]strawberry.ClusterInitializer
}

func NewClusterInitializer(config *ClusterInitializerConfig, familyToInitializerFactory map[string]strawberry.ClusterInitializerFactory) (initializer ClusterInitializer) {
	l := newLogger("init_cluster", false /*stderr*/)
	initializer.l = l
	initializer.config = *config

	config.Token = getStrawberryToken(config.Token)

	var err error
	initializer.ytc, err = ythttp.NewClient(&yt.Config{
		Token:  config.Token,
		Proxy:  initializer.config.Proxy,
		Logger: withName(l, "yt"),
	})
	if err != nil {
		panic(err)
	}
	initializer.familyToInitializer = make(map[string]strawberry.ClusterInitializer)
	for family, initializerFactory := range familyToInitializerFactory {
		initializer.familyToInitializer[family] = initializerFactory(l, initializer.ytc, config.StrawberryRoot)
	}
	return
}

func (initializer *ClusterInitializer) checkRobotPermissions(ctx context.Context) error {
	if initializer.config.RobotUsername == "" {
		return nil
	}
	userPath := ypath.Path("//sys/users").Child(initializer.config.RobotUsername)
	ok, err := initializer.ytc.NodeExists(ctx, userPath, nil)
	if err != nil {
		return err
	}
	if !ok {
		return yterrors.Err(fmt.Sprintf("user %v does not exist", initializer.config.RobotUsername))
	}

	paths := []ypath.Path{
		initializer.config.StrawberryRoot,
		ypath.Path("//sys/schemas/access_control_object"),
	}
	for _, strawberryInitializer := range initializer.familyToInitializer {
		paths = append(paths, ypath.Path("//sys/access_control_object_namespaces").Child(strawberryInitializer.ACONamespace()))
	}

	permissions := []yt.Permission{yt.PermissionCreate, yt.PermissionRead}

	for _, path := range paths {
		for _, permission := range permissions {
			response, err := initializer.ytc.CheckPermission(
				ctx,
				initializer.config.RobotUsername,
				permission,
				path,
				nil)
			if err != nil {
				return err
			}
			if response.Action != yt.ActionAllow {
				return yterrors.Err(fmt.Sprintf("robot has no permission to %v in %v", permission, path))
			}
		}
	}

	return nil
}

func (initializer *ClusterInitializer) createRootsIfNotExists(ctx context.Context) error {
	root := initializer.config.StrawberryRoot
	_, err := initializer.ytc.CreateNode(ctx, root, yt.NodeMap, &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	})
	if err != nil {
		return err
	}
	for _, family := range initializer.config.Families {
		_, err := initializer.ytc.CreateNode(ctx, root.Child(family), yt.NodeMap, &yt.CreateNodeOptions{
			IgnoreExisting: true,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (initializer *ClusterInitializer) createACONamespaceIfNotExists(ctx context.Context) error {
	for _, strawberryInitializer := range initializer.familyToInitializer {
		_, err := initializer.ytc.CreateObject(ctx, yt.NodeAccessControlObjectNamespace, &yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name": strawberryInitializer.ACONamespace(),
			},
			IgnoreExisting: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (initializer *ClusterInitializer) InitCluster() error {
	ctx := context.Background()
	if err := initializer.createRootsIfNotExists(ctx); err != nil {
		return err
	}
	if err := initializer.createACONamespaceIfNotExists(ctx); err != nil {
		return err
	}
	for _, strawberryInitializer := range initializer.familyToInitializer {
		if err := strawberryInitializer.InitializeCluster(); err != nil {
			return err
		}
	}
	if err := initializer.checkRobotPermissions(ctx); err != nil {
		return err
	}
	return nil
}
