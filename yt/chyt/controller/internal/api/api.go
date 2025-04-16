package api

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/auth"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

// API implements all the controllers-api logic.
type API struct {
	Ytc yt.Client
	cfg APIConfig
	ctl strawberry.Controller
	l   log.Logger
}

func NewAPI(ytc yt.Client, cfg APIConfig, ctl strawberry.Controller, l log.Logger) *API {
	return &API{
		Ytc: ytc,
		cfg: cfg,
		ctl: ctl,
		l:   l,
	}
}

func getUser(ctx context.Context) (string, error) {
	user, ok := auth.ContextRequester(ctx)
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

	accessNodePath := strawberry.AccessControlNamespacesPath.JoinChild(a.ctl.Family(), alias, "principal")
	response, err := a.Ytc.CheckPermission(ctx, user, permission, accessNodePath, nil)
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

func (a *API) checkPermissionToPoolForPoolTree(ctx context.Context, poolTree string, pool string, user string, permission yt.Permission) error {
	poolTreePath := ypath.Path("//sys/pool_trees").Child(poolTree)

	var poolsNode map[string]any
	err := a.Ytc.GetNode(ctx, poolTreePath, &poolsNode, nil)
	if err != nil {
		if yterrors.ContainsResolveError(err) {
			return yterrors.Err(
				err,
				fmt.Sprintf("pool_tree %v does not exist", poolTree),
				yterrors.Attr("pool_tree", poolTree))
		} else {
			return err
		}
	}

	poolSubPath := findPoolPath(poolsNode, pool)
	if poolSubPath == "" {
		return yterrors.Err(
			fmt.Sprintf("pool %v in pool_tree %v does not exist", pool, poolTree),
			yterrors.Attr("pool", pool),
			yterrors.Attr("pool_tree", poolTree))
	}
	poolPath := poolTreePath.Child(poolSubPath)

	response, err := a.Ytc.CheckPermission(ctx, user, permission, poolPath, nil)
	if err != nil {
		return err
	}

	if response.Action != yt.ActionAllow {
		return yterrors.Err(
			fmt.Sprintf("%v permission to pool %v in pool_tree %v denied for user %v", permission, pool, poolTree, user),
			yterrors.CodeAuthorizationError,
			yterrors.Attr("pool", pool),
			yterrors.Attr("pool_tree", poolTree),
			yterrors.Attr("permission", permission),
			yterrors.Attr("user", user))
	}

	if a.cfg.RobotUsername != "" {
		response, err = a.Ytc.CheckPermission(ctx, a.cfg.RobotUsername, yt.PermissionUse, poolPath, nil)
		if err != nil {
			return err
		}
		if response.Action != yt.ActionAllow {
			return yterrors.Err(
				fmt.Sprintf("use permission to pool %v in pool_tree %v denied for system user %v; "+
					"in order to use the pool in the controller, you need to grant use permission to our system user %v",
					pool,
					poolTree,
					a.cfg.RobotUsername,
					a.cfg.RobotUsername),
				yterrors.Attr("pool", pool),
				yterrors.Attr("pool_tree", poolTree),
				yterrors.Attr("permission", permission),
				yterrors.Attr("user", a.cfg.RobotUsername))
		}
	}
	return nil
}

func (a *API) CheckPermissionToPool(ctx context.Context, poolTrees []string, pool string, permission yt.Permission) error {
	user, err := getUser(ctx)
	if err != nil {
		a.l.Error("failed to get user", log.Error(err))
		return err
	}

	var effectivePoolTrees []string
	if len(poolTrees) == 0 {
		var defaultPoolTree string
		err = a.Ytc.GetNode(ctx, ypath.Path("//sys/pool_trees/@default_tree"), &defaultPoolTree, nil)
		if err != nil {
			return err
		}
		effectivePoolTrees = []string{defaultPoolTree}
	} else {
		effectivePoolTrees = poolTrees
	}

	for _, poolTree := range effectivePoolTrees {
		if err = a.checkPermissionToPoolForPoolTree(ctx, poolTree, pool, user, permission); err != nil {
			return err
		}
	}

	return nil
}

func (a *API) validatePoolOption(ctx context.Context, poolValue any, poolTreesValue any) error {
	pool, ok := poolValue.(string)
	if !ok {
		typeName := reflect.TypeOf(poolValue).String()
		return yterrors.Err(
			fmt.Sprintf("pool option has unexpected value type %v", typeName),
			yterrors.Attr("type", typeName))
	}
	var poolTrees []string
	if poolTreesValue != nil {
		poolTreeValues, ok := poolTreesValue.([]any)
		if !ok {
			typeName := reflect.TypeOf(poolTreesValue).String()
			return yterrors.Err(
				fmt.Sprintf("pool_trees option has unexpected value type %v", typeName),
				yterrors.Attr("type", typeName))
		}
		poolTrees = make([]string, len(poolTreeValues))
		for _, poolTreeValue := range poolTreeValues {
			poolTree, ok := poolTreeValue.(string)
			if !ok {
				typeName := reflect.TypeOf(poolTreeValue).String()
				return yterrors.Err(
					fmt.Sprintf("element of pool_trees option has unexpected value type %v", typeName),
					yterrors.Attr("type", typeName))
			}
			poolTrees = append(poolTrees, poolTree)
		}
	}
	if a.cfg.ValidatePoolAccessOrDefault() {
		return a.CheckPermissionToPool(ctx, poolTrees, pool, yt.PermissionUse)
	}
	return nil

}

func (a *API) newOplet(
	alias string,
	userClient yt.Client,
	agentInfo strawberry.AgentInfo,
) *strawberry.Oplet {
	if userClient == nil {
		userClient = a.Ytc
	}
	return strawberry.NewOplet(strawberry.OpletOptions{
		AgentInfo:    agentInfo,
		Alias:        alias,
		Controller:   a.ctl,
		Logger:       a.l,
		UserClient:   userClient,
		SystemClient: a.Ytc,
	})
}

func (a *API) getOpletFromCypress(
	ctx context.Context,
	alias string,
	userClient yt.Client,
	agentInfo strawberry.AgentInfo,
) (oplet *strawberry.Oplet, err error) {
	oplet = a.newOplet(alias, userClient, agentInfo)

	if err = oplet.EnsureUpdatedFromCypress(ctx); err != nil {
		return
	}
	// Oplet should observe controller in up to date state.
	if _, err = a.ctl.UpdateState(); err != nil {
		return
	}
	return
}

func (a *API) getOpletFromYson(
	alias string,
	userClient yt.Client,
	agentInfo strawberry.AgentInfo,
	node yson.RawValue,
	acl []yt.ACE,
) (oplet *strawberry.Oplet, err error) {
	oplet = a.newOplet(alias, userClient, agentInfo)
	err = oplet.LoadFromYsonNode(node, acl)
	return
}

// getOpletBriefInfoFromCypress creates an oplet from cypress and extracts
// OpletBriefInfo even if the oplet is broken.
func (a *API) getOpletBriefInfoFromCypress(ctx context.Context, alias string) (strawberry.OpletBriefInfo, error) {
	oplet, err := a.getOpletFromCypress(ctx, alias, nil, a.cfg.AgentInfo)

	if err == nil {
		err = oplet.CheckOperationLiveness(ctx)
	}

	if err == nil || (oplet != nil && oplet.Broken()) {
		return oplet.GetBriefInfo(), nil
	} else {
		return strawberry.OpletBriefInfo{}, err
	}
}

// getOpletBriefInfoFromYson is similar to getOpletBriefInfoFromCypress,
// but it creates an oplet from an already loaded strawberry state in yson.
// It loads completely from provided state, so it's relatively cheap to call,
// but it does not do extra check for operation liveness and this data may
// be slightly outdated in the loaded state.
func (a *API) getOpletBriefInfoFromYson(
	alias string,
	node yson.RawValue,
	acl []yt.ACE,
) (strawberry.OpletBriefInfo, error) {
	oplet, err := a.getOpletFromYson(alias, nil, a.cfg.AgentInfo, node, acl)

	if err == nil || (oplet != nil && oplet.Broken()) {
		return oplet.GetBriefInfo(), nil
	} else {
		return strawberry.OpletBriefInfo{}, err
	}
}

func (a *API) getUserFromOperation(ctx context.Context, alias string) (string, error) {
	var opID yt.OperationID
	opIDPath := a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias).Attr("strawberry_persistent_state").Child("yt_operation_id")
	if err := a.Ytc.GetNode(ctx, opIDPath, &opID, nil); err != nil {
		return "", err
	}
	op, err := a.Ytc.GetOperation(ctx, opID, nil)
	if err != nil {
		return "", err
	}
	return op.AuthenticatedUser, nil
}

// Create creates a new strawberry operation in cypress.
// If the creation fails due to a transient error, the resulting state can be inconsistent,
// because we can not create an access control object node in transactions.
// Anyway, it's guaranteed that in such state the Create command can be retried
// and that this state can be completely removed via Remove command.
func (a *API) Create(
	ctx context.Context,
	alias string,
	specletOptions map[string]any,
	secrets map[string]any,
) error {
	// It's not necessary to check an operation existence, but we do it to provide better error messages.
	if err := a.CheckExistence(ctx, alias, false /*shouldExist*/); err != nil {
		return err
	}

	pool, poolIsSet := specletOptions["pool"]
	poolTrees := specletOptions["pool_trees"]

	if active, ok := specletOptions["active"]; ok {
		if err := validateBool(active); err != nil {
			return err
		}
		if active.(bool) && !poolIsSet {
			return yterrors.Err("can't start operation, pool is not set")
		}
	}

	if poolIsSet {
		if err := a.validatePoolOption(ctx, pool, poolTrees); err != nil {
			return err
		}
	}

	user, err := getUser(ctx)
	if err != nil {
		a.l.Error("failed to get user", log.Error(err))
		return err
	}

	// Create "access" node.
	acoOptions := &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name":      alias,
			"namespace": a.ctl.Family(),
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
	}

	if a.cfg.AssignAdministerToCreator {
		acoOptions.Attributes["acl"] = []yt.ACE{
			{
				Action:      yt.ActionAllow,
				Subjects:    []string{user},
				Permissions: []yt.Permission{yt.PermissionAdminister},
			},
		}
	}
	_, err = a.Ytc.CreateObject(ctx, yt.NodeAccessControlObject, acoOptions)

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

	tx, err := a.Ytc.BeginTx(ctx, &yt.StartTxOptions{})
	if err != nil {
		return err
	}
	defer a.Ytc.AbortTx(ctx, tx.ID(), nil)

	txOptions := &yt.TransactionOptions{TransactionID: tx.ID()}

	// Create "main" node.
	_, err = a.Ytc.CreateNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.Child(alias),
		yt.NodeMap, &yt.CreateNodeOptions{
			Attributes: map[string]any{
				"strawberry_persistent_state": map[string]any{
					"creator": user,
				},
			},
			TransactionOptions: txOptions,
		})

	if err != nil {
		return err
	}

	// Create "speclet" node.
	speclet := map[string]any{
		"family": a.ctl.Family(),
		"stage":  a.cfg.AgentInfo.Stage,
	}
	for key, value := range specletOptions {
		speclet[key] = value
	}

	_, err = a.Ytc.CreateNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet"),
		yt.NodeDocument,
		&yt.CreateNodeOptions{
			Attributes: map[string]any{
				"value": speclet,
			},
			TransactionOptions: txOptions,
		})

	if err != nil {
		return err
	}

	if secrets != nil {
		_, err = a.Ytc.CreateNode(
			ctx,
			a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "secrets"),
			yt.NodeDocument,
			getCreateSecretNodeOptions(secrets, txOptions),
		)
		if err != nil {
			return err
		}
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

	err := a.Ytc.RemoveNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.Child(alias),
		&yt.RemoveNodeOptions{Force: true, Recursive: true})

	if err != nil {
		return err
	}

	accessNodePath := strawberry.AccessControlNamespacesPath.JoinChild(a.ctl.Family(), alias)
	return a.Ytc.RemoveNode(ctx, accessNodePath, &yt.RemoveNodeOptions{Recursive: true})
}

func (a *API) Exists(ctx context.Context, alias string) (bool, error) {
	return a.Ytc.NodeExists(ctx, a.cfg.AgentInfo.StrawberryRoot.Child(alias), nil)
}

func (a *API) GetBriefInfo(ctx context.Context, alias string) (strawberry.OpletBriefInfo, error) {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return strawberry.OpletBriefInfo{}, err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return strawberry.OpletBriefInfo{}, err
	}
	return a.getOpletBriefInfoFromCypress(ctx, alias)
}

func (a *API) getOption(ctx context.Context, alias, key string) (value any, err error) {
	err = a.Ytc.GetNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", key),
		&value,
		nil)
	return
}

func (a *API) GetOption(ctx context.Context, alias, key string) (value any, err error) {
	if err = a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return
	}
	if err = a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return
	}
	value, err = a.getOption(ctx, alias, key)
	return
}

func (a *API) setOption(
	ctx context.Context,
	alias, key string,
	value any,
	options *yt.SetNodeOptions,
) error {
	// NB: pool and pool_trees options require validation.
	if key == "pool" || key == "pool_trees" {
		return a.EditOptions(ctx, alias, map[string]any{key: value}, nil)
	}

	return a.Ytc.SetNode(ctx, a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", key), value, options)
}

func (a *API) SetOption(ctx context.Context, alias, key string, value any) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	return a.setOption(ctx, alias, key, value, &yt.SetNodeOptions{Recursive: true, Force: true})
}

func (a *API) RemoveOption(ctx context.Context, alias, key string) error {
	// NB: pool and pool_trees options require validation.
	if key == "pool" || key == "pool_trees" {
		return a.EditOptions(ctx, alias, nil, []string{key})
	}

	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	return a.Ytc.RemoveNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", key),
		&yt.RemoveNodeOptions{Recursive: true, Force: true})
}

type AliasWithAttrs struct {
	Alias string         `yson:",value" json:"$value"`
	Attrs map[string]any `yson:",attrs" json:"$attributes"`
}

func (a *API) List(ctx context.Context, attributes []string, filters map[string]any) ([]AliasWithAttrs, error) {
	var attributesToList []string
	processAttributes := len(attributes) != 0 || len(filters) != 0 || a.cfg.ShowOnlyOwnSpeclets
	if processAttributes {
		attributesToList = strawberry.CypressStateAttributes
	}

	var ops map[string]yson.RawValue

	err := a.Ytc.GetNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot,
		&ops,
		&yt.GetNodeOptions{Attributes: attributesToList})
	if err != nil {
		return nil, err
	}

	var acls map[string]struct {
		ACL []yt.ACE `yson:"principal_acl,attr"`
	}

	if len(attributes) != 0 {
		err := a.Ytc.GetNode(
			ctx,
			strawberry.AccessControlNamespacesPath.JoinChild(a.ctl.Family()),
			&acls,
			&yt.GetNodeOptions{Attributes: []string{"principal_acl"}})
		if err != nil {
			return nil, err
		}

		// Ctl state should be updated in order to return valid status.
		if _, err := a.ctl.UpdateState(); err != nil {
			return nil, err
		}
	}

	currentUser, err := getUser(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]AliasWithAttrs, 0, len(ops))
	for alias, node := range ops {
		var resultAttrs map[string]any

		if processAttributes {
			briefInfo, err := a.getOpletBriefInfoFromYson(alias, node, acls[alias].ACL)
			// NB: Should never happen.
			if err != nil {
				return nil, err
			}
			opletAttrs := strawberry.GetOpBriefAttributes(briefInfo)

			if a.cfg.ShowOnlyOwnSpeclets && opletAttrs["creator"] != currentUser {
				continue
			}

			filterMismatch := false
			for filterAttr, filterValue := range filters {
				if oplVal, ok := opletAttrs[filterAttr]; !ok || oplVal != filterValue {
					filterMismatch = true
					break
				}
			}
			if filterMismatch {
				continue
			}

			resultAttrs = make(map[string]any)
			for _, attr := range attributes {
				if value, ok := opletAttrs[attr]; ok {
					resultAttrs[attr] = value
				} else {
					return nil, yterrors.Err(
						fmt.Sprintf("unknown attribute %v", attr),
						yterrors.Attr("attribute", attr))
				}
			}
		}

		result = append(result, AliasWithAttrs{
			Alias: alias,
			Attrs: resultAttrs,
		})
	}

	return result, nil
}

func (a *API) GetSpeclet(ctx context.Context, alias string) (speclet map[string]any, err error) {
	if err = a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return
	}
	if err = a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return
	}
	err = a.Ytc.GetNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet"),
		&speclet,
		nil)
	return
}

func (a *API) SetSpeclet(ctx context.Context, alias string, speclet map[string]any) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}

	if pool, ok := speclet["pool"]; ok {
		if err := a.validatePoolOption(ctx, pool, speclet["pool_trees"]); err != nil {
			return err
		}
	}

	var node struct {
		Speclet struct {
			Family string `yson:"family"`
			Stage  string `yson:"stage"`
		} `yson:"value"`
		Revision yt.Revision `yson:"revision"`
	}
	specletPath := a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet")
	err := a.Ytc.GetNode(ctx, specletPath.Attrs(), &node, &yt.GetNodeOptions{Attributes: []string{"revision", "value"}})
	if err != nil {
		return err
	}

	if _, ok := speclet["family"]; !ok {
		speclet["family"] = node.Speclet.Family
	}
	if _, ok := speclet["stage"]; !ok {
		speclet["stage"] = node.Speclet.Stage
	}

	err = a.Ytc.SetNode(
		ctx,
		specletPath,
		speclet,
		&yt.SetNodeOptions{
			PrerequisiteOptions: &yt.PrerequisiteOptions{
				Revisions: []yt.PrerequisiteRevision{
					{
						Path:     specletPath,
						Revision: node.Revision,
					},
				},
			},
		})
	if err != nil {
		return err
	}
	return nil
}

func (a *API) EditOptions(
	ctx context.Context,
	alias string,
	optionsToSet map[string]any,
	optionsToRemove []string,
) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}

	if len(optionsToSet) == 0 && len(optionsToRemove) == 0 {
		return nil
	}

	var node struct {
		Speclet  map[string]any `yson:"value"`
		Revision yt.Revision    `yson:"revision"`
	}
	specletPath := a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet")
	err := a.Ytc.GetNode(ctx, specletPath.Attrs(), &node, &yt.GetNodeOptions{Attributes: []string{"revision", "value"}})
	if err != nil {
		return err
	}

	speclet := node.Speclet

	poolOptionsChanged := false

	for _, key := range optionsToRemove {
		delete(speclet, key)
		if key == "pool" || key == "pool_trees" {
			poolOptionsChanged = true
		}
	}

	for key, value := range optionsToSet {
		speclet[key] = value
		if key == "pool" || key == "pool_trees" {
			poolOptionsChanged = true
		}
	}

	if pool, ok := speclet["pool"]; ok && poolOptionsChanged {
		if err := a.validatePoolOption(ctx, pool, speclet["pool_trees"]); err != nil {
			return err
		}
	}

	return a.Ytc.SetNode(
		ctx,
		specletPath,
		speclet,
		&yt.SetNodeOptions{
			PrerequisiteOptions: &yt.PrerequisiteOptions{
				Revisions: []yt.PrerequisiteRevision{
					{
						Path:     specletPath,
						Revision: node.Revision,
					},
				},
			},
		})
}

func (a *API) getAgentInfoForUntrackedStage() strawberry.AgentInfo {
	agentInfo := a.cfg.AgentInfo
	agentInfo.Stage = strawberry.StageUntracked
	agentInfo.OperationNamespace = a.ctl.Family() + ":" + strawberry.StageUntracked
	return agentInfo
}

func (a *API) Start(ctx context.Context, alias string, untracked bool, userClient yt.Client) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	if err := a.setOption(ctx, alias, "active", true, nil); err != nil {
		return err
	}
	if !untracked {
		if err := a.setOption(ctx, alias, "stage", a.cfg.AgentInfo.Stage, nil); err != nil {
			return err
		}
		return nil
	}
	if err := a.setOption(ctx, alias, "stage", strawberry.StageUntracked, nil); err != nil {
		return err
	}
	agentInfo := a.getAgentInfoForUntrackedStage()
	oplet, err := a.getOpletFromCypress(ctx, alias, userClient, agentInfo)
	if err != nil {
		return err
	}
	return oplet.Pass(ctx, true /*checkOpLiveness*/)
}

func (a *API) Stop(ctx context.Context, alias string) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	var stage string
	err := a.Ytc.GetNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", "stage"),
		&stage,
		nil)
	if err != nil {
		return err
	}
	if err = a.setOption(ctx, alias, "active", false, nil); err != nil {
		return err
	}
	if stage != strawberry.StageUntracked {
		return nil
	}
	agentInfo := a.getAgentInfoForUntrackedStage()
	oplet, err := a.getOpletFromCypress(ctx, alias, nil, agentInfo)
	if err != nil {
		return err
	}
	return oplet.Pass(ctx, true /*checkOpLiveness*/)
}

func (a *API) Restart(ctx context.Context, alias string, force bool, userClient yt.Client) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	specletPath := a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet")
	var node struct {
		Speclet struct {
			Stage  string `yson:"stage"`
			Active bool   `yson:"active"`
		} `yson:"value"`
		Revision yt.Revision `yson:"revision"`
	}
	options := yt.GetNodeOptions{Attributes: []string{"revision", "value"}}
	if err := a.Ytc.GetNode(ctx, specletPath.Attrs(), &node, &options); err != nil {
		return err
	}

	if !node.Speclet.Active {
		return yterrors.Err(
			fmt.Sprintf("clique %v is inactive, restart can be done only for running cliques", alias))
	}

	if node.Speclet.Stage == strawberry.StageUntracked && !force {
		currentUser, err := getUser(ctx)
		if err != nil {
			return err
		}
		userStartedOp, err := a.getUserFromOperation(ctx, alias)
		if err != nil {
			return err
		}
		if currentUser != userStartedOp {
			return yterrors.Err(
				fmt.Sprintf("previous operation was started by %v, "+
					"if you want to restart operation from %v use --force option, "+
					"be careful, without correct permissions force restart will be unsuccessful",
					userStartedOp,
					currentUser))
		}
	}

	err := a.setOption(
		ctx,
		alias,
		"min_speclet_revision",
		node.Revision+1,
		&yt.SetNodeOptions{
			PrerequisiteOptions: &yt.PrerequisiteOptions{
				Revisions: []yt.PrerequisiteRevision{
					{
						Path:     specletPath,
						Revision: node.Revision,
					},
				},
			},
		})
	if err != nil {
		return err
	}

	if node.Speclet.Stage != strawberry.StageUntracked {
		return nil
	}

	agentInfo := a.getAgentInfoForUntrackedStage()
	oplet, err := a.getOpletFromCypress(ctx, alias, userClient, agentInfo)
	if err != nil {
		return err
	}

	return oplet.Pass(ctx, true /*checkOpLiveness*/)
}

func (a *API) DescribeOptions(ctx context.Context, alias string) ([]strawberry.OptionGroupDescriptor, error) {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return nil, err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return nil, err
	}

	var specletYson yson.RawValue
	err := a.Ytc.GetNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet"),
		&specletYson,
		nil)
	if err != nil {
		return nil, err
	}

	strawberrySpeclet, err := strawberry.ParseSpeclet(specletYson)
	if err != nil {
		return nil, err
	}
	strawberryOptions := strawberry.DescribeOptions(a.cfg.AgentInfo, strawberrySpeclet)

	ctlSpeclet, err := a.ctl.ParseSpeclet(specletYson)
	if err != nil {
		return nil, err
	}
	ctlOptions := a.ctl.DescribeOptions(ctlSpeclet)

	return append(strawberryOptions, ctlOptions...), nil
}

func (a *API) GetSecrets(ctx context.Context, alias string) (secrets map[string]any, err error) {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return nil, err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return nil, err
	}

	secretsPath := a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "secrets")
	secretsNodeExists, err := a.Ytc.NodeExists(ctx, secretsPath, nil)
	if err != nil {
		return nil, err
	}

	if secretsNodeExists {
		err = a.Ytc.GetNode(
			ctx,
			secretsPath,
			&secrets,
			nil)
		if err != nil {
			return nil, err
		}
	}

	return
}

func (a *API) SetSecrets(ctx context.Context, alias string, secrets map[string]any) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}

	secretsPath := a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "secrets")

	_, err := a.Ytc.CreateNode(
		ctx,
		secretsPath,
		yt.NodeDocument,
		getCreateSecretNodeOptions(secrets, nil),
	)
	return err
}

func (a *API) Resume(ctx context.Context, alias string) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	} // TODO: maybe yt.PermissionUse?

	marker := fmt.Sprintf("%s_%s", strconv.FormatInt(time.Now().Unix(), 10), getRandomString(5))
	return a.setOption(ctx, alias, "resume_marker", marker, nil)
}
