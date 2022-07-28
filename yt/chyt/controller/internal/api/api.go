package api

import (
	"context"
	"reflect"

	"a.yandex-team.ru/library/go/core/log"
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
		return "", yterrors.Err("requester is missing in the request context")
	}
	return user, nil
}

func (a *API) CheckPermissionToOp(ctx context.Context, alias string, permission yt.Permission) error {
	user, err := getUser(ctx)
	if err != nil {
		a.l.Error("failed to get user", log.Error(err))
		return err
	}

	response, err := a.ytc.CheckPermission(ctx, user, permission, a.cfg.Root.JoinChild(alias, "access"), nil)
	if err != nil {
		return err
	}

	if response.Action != yt.ActionAllow {
		return yterrors.Err("operation access denied",
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
		return yterrors.Err("access for pool denied",
			yterrors.CodeAuthorizationError,
			yterrors.Attr("pool", pool),
			yterrors.Attr("permission", permission),
			yterrors.Attr("user", user))
	}

	return nil
}

func (a *API) Create(ctx context.Context, alias string) error {
	user, err := getUser(ctx)
	if err != nil {
		a.l.Error("failed to get user", log.Error(err))
		return err
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
		},
		TransactionOptions: txOptions,
	})
	if err != nil {
		return err
	}

	// Create "access" node.
	_, err = a.ytc.CreateNode(ctx, a.cfg.Root.JoinChild(alias, "access"), yt.NodeMap, &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"acl": []yt.ACE{
				{
					Action:      yt.ActionAllow,
					Subjects:    []string{user},
					Permissions: []yt.Permission{yt.PermissionRead, yt.PermissionRemove, yt.PermissionUse, yt.PermissionManage},
				},
			},
		},
		TransactionOptions: txOptions,
	})
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (a *API) Remove(ctx context.Context, alias string) error {
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionRemove); err != nil {
		return err
	}
	return a.ytc.RemoveNode(ctx, a.cfg.Root.Child(alias), &yt.RemoveNodeOptions{Recursive: true})
}

func (a *API) SetOption(ctx context.Context, alias, key string, value any) error {
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	if key == "pool" {
		pool, ok := value.(string)
		if !ok {
			return yterrors.Err("pool value has unexpected type", yterrors.Attr("type", reflect.TypeOf(value).String()))
		}
		if err := a.CheckPermissionToPool(ctx, pool, yt.PermissionUse); err != nil {
			return err
		}
	}
	return a.ytc.SetNode(ctx, a.cfg.Root.JoinChild(alias, "speclet", key), value, &yt.SetNodeOptions{Recursive: true, Force: true})
}

func (a *API) RemoveOption(ctx context.Context, alias, key string) error {
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	return a.ytc.RemoveNode(ctx, a.cfg.Root.JoinChild(alias, "speclet", key), &yt.RemoveNodeOptions{Recursive: true, Force: true})
}
