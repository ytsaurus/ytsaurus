package api

import (
	"context"
	"fmt"
	"reflect"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/internal/go/ythttputil"
)

// API implements all the contorller-api logic.
type API struct {
	ytc yt.Client
	cfg APIConfig
	l   log.Logger
}

func NewAPI(ytc yt.Client, cfg APIConfig, l log.Logger) *API {
	return &API{
		ytc: ytc,
		cfg: cfg,
		l:   l,
	}
}

func getUser(ctx context.Context) (string, error) {
	user, ok := ythttputil.ContextRequester(ctx)
	if !ok {
		// Actually, should never happen.
		return "", yterrors.Err("requester is missing in the request context")
	}
	return user, nil
}

func (a *API) CheckExistence(ctx context.Context, alias string, shouldExist bool) error {
	exists, err := a.Exists(ctx, alias)
	if err != nil {
		return err
	}
	if exists != shouldExist {
		if exists {
			return yterrors.Err(
				fmt.Sprintf("strawberry operation %v already exists", alias),
				yterrors.CodeAlreadyExists,
				yterrors.Attr("alias", alias))
		} else {
			return yterrors.Err(
				fmt.Sprintf("strawberry operation %v does not exist", alias),
				yterrors.CodeResolveError,
				yterrors.Attr("alias", alias))
		}
	}
	return nil
}

func (a *API) CheckPermissionToOp(ctx context.Context, alias string, permission yt.Permission) error {
	user, err := getUser(ctx)
	if err != nil {
		a.l.Error("failed to get user", log.Error(err))
		return err
	}

	accessNodePath := strawberry.AccessControlNamespacesPath.JoinChild(a.cfg.Family, alias, "principal")
	response, err := a.ytc.CheckPermission(ctx, user, permission, accessNodePath, nil)
	if err != nil {
		return err
	}

	if response.Action != yt.ActionAllow {
		return yterrors.Err(
			fmt.Sprintf("%q access to strawberry operation %q denied for user %q", permission, alias, user),
			yterrors.CodeAuthorizationError,
			yterrors.Attr("alias", alias),
			yterrors.Attr("permission", permission),
			yterrors.Attr("user", user))
	}

	return nil
}

// TODO(dakovalkov): delete it when resolving pool -> path is available through scheduler on all clusters.
func findPoolPath(poolsNode map[string]any, pool string) string {
	for key, value := range poolsNode {
		if key == pool {
			return key
		}
		if subnode, ok := value.(map[string]any); ok {
			subpath := findPoolPath(subnode, pool)
			if subpath != "" {
				return key + "/" + subpath
			}
		}
	}
	return ""
}

func (a *API) CheckPermissionToPool(ctx context.Context, pool string, permission yt.Permission) error {
	user, err := getUser(ctx)
	if err != nil {
		a.l.Error("failed to get user", log.Error(err))
		return err
	}

	var poolsNode map[string]any
	err = a.ytc.GetNode(ctx, ypath.Path("//sys/pools"), &poolsNode, nil)
	if err != nil {
		return err
	}

	poolPath := findPoolPath(poolsNode, pool)
	if poolPath == "" {
		return yterrors.Err("pool not found", yterrors.Attr("pool", pool))
	}

	response, err := a.ytc.CheckPermission(ctx, user, permission, ypath.Path("//sys/pools/"+poolPath), nil)
	if err != nil {
		return err
	}

	if response.Action != yt.ActionAllow {
		return yterrors.Err(
			fmt.Sprintf("%v access to pool %v denied for user %v", permission, pool, user),
			yterrors.CodeAuthorizationError,
			yterrors.Attr("pool", pool),
			yterrors.Attr("permission", permission),
			yterrors.Attr("user", user))
	}

	return nil
}

// Create creates a new strawberry operation in cypress.
// If the creation fails due to a transient error, the resulting state can be inconsistent,
// because we can not create an access control object node in transactions.
// Anyway, it's guaranteed that in such state the Create command can be retried
// and that this state can be completely removed via Remove command.
func (a *API) Create(ctx context.Context, alias string) error {
	// It's not nessesary to check an operation existence, but we do it to provide better error messages.
	if err := a.CheckExistence(ctx, alias, false /*shouldExist*/); err != nil {
		return err
	}
	user, err := getUser(ctx)
	if err != nil {
		a.l.Error("failed to get user", log.Error(err))
		return err
	}

	// Create "access" node.
	_, err = a.ytc.CreateObject(ctx, yt.NodeAccessControlObject, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      alias,
			"namespace": a.cfg.Family,
			"principal_acl": append(a.cfg.BaseACL,
				// ACE for Use role.
				yt.ACE{
					Action:      yt.ActionAllow,
					Subjects:    []string{user},
					Permissions: []yt.Permission{yt.PermissionUse},
				},
				// ACE for Manage role.
				yt.ACE{
					Action:      yt.ActionAllow,
					Subjects:    []string{user},
					Permissions: []yt.Permission{yt.PermissionRead, yt.PermissionRemove, yt.PermissionManage},
				}),
			"idm_initial_roles": []map[string]any{
				{
					"subjects": []string{user},
					"roles":    []string{"use", "manage", "responsible"},
				},
			},
		},
	})

	if err != nil {
		if !yterrors.ContainsAlreadyExistsError(err) {
			return yterrors.Err(fmt.Sprintf("failed to create strawberry operation %v", alias), err)
		}
		// If the previous creation of the op failed, the access node can exist without a strawberry node.
		// In that case we check that user has proper access for an existing node and allow him to create a strawberry node.
		if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
			return err
		}
	}

	tx, err := a.ytc.BeginTx(ctx, &yt.StartTxOptions{})
	if err != nil {
		return err
	}
	defer a.ytc.AbortTx(ctx, tx.ID(), nil)

	txOptions := &yt.TransactionOptions{TransactionID: tx.ID()}

	// Create "main" node.
	_, err = a.ytc.CreateNode(ctx, a.cfg.Root.Child(alias), yt.NodeMap, &yt.CreateNodeOptions{TransactionOptions: txOptions})
	if err != nil {
		return err
	}

	// Create "speclet" node.
	_, err = a.ytc.CreateNode(ctx, a.cfg.Root.JoinChild(alias, "speclet"), yt.NodeDocument, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"value": map[string]any{
				"family": a.cfg.Family,
				"stage":  a.cfg.Stage,
			},
			"strawberry_persistent_state": map[string]any{
				"creator": user,
			},
		},
		TransactionOptions: txOptions,
	})
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Remove deletes the strawberry operation from cypress.
// If the deletion fails due to a transient error, the state can be partially removed,
// but it's guaranteed that the Remove command can be retried to delete the state completely.
func (a *API) Remove(ctx context.Context, alias string) error {
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionRemove); err != nil {
		if yterrors.ContainsResolveError(err) {
			// In case of failed Create/Remove commands the strawberry operation can "partially" exists.
			// In that case the access node exists, but the strawberry node does not.
			// In order to allow removing the access node in that case,
			// we check the existence only if the access node does not exist.
			// Actually, CheckExistence is used only for producing better error messages.
			if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
				return err
			}
		}
		return err
	}

	err := a.ytc.RemoveNode(ctx, a.cfg.Root.Child(alias), &yt.RemoveNodeOptions{Force: true, Recursive: true})
	if err != nil {
		return err
	}

	accessNodePath := strawberry.AccessControlNamespacesPath.JoinChild(a.cfg.Family, alias)
	return a.ytc.RemoveNode(ctx, accessNodePath, &yt.RemoveNodeOptions{Recursive: true})
}

func (a *API) Exists(ctx context.Context, alias string) (bool, error) {
	return a.ytc.NodeExists(ctx, a.cfg.Root.Child(alias), nil)
}

func (a *API) SetOption(ctx context.Context, alias, key string, value any) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	if key == "pool" {
		pool, ok := value.(string)
		if !ok {
			typeName := reflect.TypeOf(value).String()
			return yterrors.Err(
				fmt.Sprintf("pool option has unexpected value type %v", typeName),
				yterrors.Attr("type", typeName))
		}
		if err := a.CheckPermissionToPool(ctx, pool, yt.PermissionUse); err != nil {
			return err
		}
	}
	return a.ytc.SetNode(ctx, a.cfg.Root.JoinChild(alias, "speclet", key), value, &yt.SetNodeOptions{Recursive: true, Force: true})
}

func (a *API) RemoveOption(ctx context.Context, alias, key string) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	return a.ytc.RemoveNode(ctx, a.cfg.Root.JoinChild(alias, "speclet", key), &yt.RemoveNodeOptions{Recursive: true, Force: true})
}

func (a *API) List(ctx context.Context) ([]string, error) {
	var aliases []string
	err := a.ytc.ListNode(ctx, a.cfg.Root, &aliases, nil)
	return aliases, err
}
