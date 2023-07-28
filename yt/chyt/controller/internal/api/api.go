package api

import (
	"context"
	"fmt"
	"reflect"

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
	ytc yt.Client
	cfg APIConfig
	ctl strawberry.Controller
	l   log.Logger
}

func NewAPI(ytc yt.Client, cfg APIConfig, ctl strawberry.Controller, l log.Logger) *API {
	return &API{
		ytc: ytc,
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

	poolSubPath := findPoolPath(poolsNode, pool)
	if poolSubPath == "" {
		return yterrors.Err(
			fmt.Sprintf("pool %v does not exist", pool),
			yterrors.Attr("pool", pool))
	}
	poolPath := ypath.Path("//sys/pools").Child(poolSubPath)

	response, err := a.ytc.CheckPermission(ctx, user, permission, poolPath, nil)
	if err != nil {
		return err
	}

	if response.Action != yt.ActionAllow {
		return yterrors.Err(
			fmt.Sprintf("%v permission to pool %v denied for user %v", permission, pool, user),
			yterrors.CodeAuthorizationError,
			yterrors.Attr("pool", pool),
			yterrors.Attr("permission", permission),
			yterrors.Attr("user", user))
	}

	if a.cfg.RobotUsername != "" {
		response, err = a.ytc.CheckPermission(ctx, a.cfg.RobotUsername, yt.PermissionUse, poolPath, nil)
		if err != nil {
			return err
		}
		if response.Action != yt.ActionAllow {
			return yterrors.Err(
				fmt.Sprintf("use permission to pool %v denied for system user %v; "+
					"in order to use the pool in the controller, you need to grant use permission to our system user %v",
					pool,
					a.cfg.RobotUsername,
					a.cfg.RobotUsername),
				yterrors.Attr("pool", pool),
				yterrors.Attr("permission", permission),
				yterrors.Attr("user", a.cfg.RobotUsername))
		}
	}

	return nil
}

func (a *API) validatePoolOption(ctx context.Context, value any) error {
	pool, ok := value.(string)
	if !ok {
		typeName := reflect.TypeOf(value).String()
		return yterrors.Err(
			fmt.Sprintf("pool option has unexpected value type %v", typeName),
			yterrors.Attr("type", typeName))
	}
	if a.cfg.ValidatePoolAccessOrDefault() {
		return a.CheckPermissionToPool(ctx, pool, yt.PermissionUse)
	}
	return nil

}

func (a *API) getOplet(
	ctx context.Context,
	alias string,
	userClient yt.Client,
	agentInfo strawberry.AgentInfo) (*strawberry.Oplet, error) {
	if userClient == nil {
		userClient = a.ytc
	}
	options := strawberry.OpletOptions{
		AgentInfo:    agentInfo,
		Alias:        alias,
		Controller:   a.ctl,
		Logger:       a.l,
		UserClient:   userClient,
		SystemClient: a.ytc,
	}

	oplet := strawberry.NewOplet(options)

	if err := oplet.EnsureUpdatedFromCypress(ctx); err != nil {
		return nil, err
	}

	// Oplet should observe controller in up to date state.
	if _, err := a.ctl.UpdateState(); err != nil {
		return nil, err
	}

	return oplet, nil
}

// Create creates a new strawberry operation in cypress.
// If the creation fails due to a transient error, the resulting state can be inconsistent,
// because we can not create an access control object node in transactions.
// Anyway, it's guaranteed that in such state the Create command can be retried
// and that this state can be completely removed via Remove command.
func (a *API) Create(ctx context.Context, alias string) error {
	// It's not necessary to check an operation existence, but we do it to provide better error messages.
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
	_, err = a.ytc.CreateNode(
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
	_, err = a.ytc.CreateNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet"),
		yt.NodeDocument,
		&yt.CreateNodeOptions{
			Attributes: map[string]any{
				"value": map[string]any{
					"family": a.ctl.Family(),
					"stage":  a.cfg.AgentInfo.Stage,
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

	err := a.ytc.RemoveNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.Child(alias),
		&yt.RemoveNodeOptions{Force: true, Recursive: true})

	if err != nil {
		return err
	}

	accessNodePath := strawberry.AccessControlNamespacesPath.JoinChild(a.ctl.Family(), alias)
	return a.ytc.RemoveNode(ctx, accessNodePath, &yt.RemoveNodeOptions{Recursive: true})
}

func (a *API) Exists(ctx context.Context, alias string) (bool, error) {
	return a.ytc.NodeExists(ctx, a.cfg.AgentInfo.StrawberryRoot.Child(alias), nil)
}

func (a *API) Status(ctx context.Context, alias string) (strawberry.OpletStatus, error) {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return strawberry.OpletStatus{}, err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return strawberry.OpletStatus{}, err
	}

	oplet, err := a.getOplet(ctx, alias, nil, a.cfg.AgentInfo)
	if err != nil {
		return strawberry.OpletStatus{}, err
	}
	if err := oplet.LoadInfoState(ctx); err != nil {
		return strawberry.OpletStatus{}, err
	}
	if err := oplet.CheckOperationLiveness(ctx); err != nil {
		return strawberry.OpletStatus{}, err
	}
	return oplet.Status()
}

func (a *API) GetOption(ctx context.Context, alias, key string) (value yson.RawValue, err error) {
	if err = a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return
	}
	if err = a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return
	}
	err = a.ytc.GetNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", key),
		&value,
		nil)
	return
}

func (a *API) SetOption(ctx context.Context, alias, key string, value any) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	if key == "pool" {
		if err := a.validatePoolOption(ctx, value); err != nil {
			return err
		}
	}
	return a.ytc.SetNode(ctx, a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", key), value, &yt.SetNodeOptions{Recursive: true, Force: true})
}

func (a *API) RemoveOption(ctx context.Context, alias, key string) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	return a.ytc.RemoveNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", key),
		&yt.RemoveNodeOptions{Recursive: true, Force: true})
}

type AliasWithAttrs struct {
	Alias string         `yson:",value"`
	Attrs map[string]any `yson:",attrs"`
}

func (a *API) List(ctx context.Context, attributes []string) ([]AliasWithAttrs, error) {
	var attributesToList []string
	if len(attributes) != 0 {
		attributesToList = []string{
			"strawberry_persistent_state",
			"strawberry_info_state",
		}
	}

	var ops []struct {
		Alias           string                     `yson:",value"`
		InfoState       strawberry.InfoState       `yson:"strawberry_info_state,attr"`
		PersistentState strawberry.PersistentState `yson:"strawberry_persistent_state,attr"`
	}
	err := a.ytc.ListNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot,
		&ops,
		&yt.ListNodeOptions{Attributes: attributesToList})
	if err != nil {
		return nil, err
	}

	result := make([]AliasWithAttrs, len(ops))
	for i, op := range ops {
		var resultAttrs map[string]any

		if len(attributes) != 0 {
			strawberryAttrs, err := strawberry.GetOpBriefAttributes(
				op.PersistentState,
				op.InfoState)
			if err != nil {
				return nil, err
			}

			speclet, err := a.ctl.ParseSpeclet(op.PersistentState.YTOpSpeclet)
			if err != nil {
				return nil, err
			}
			ctlAttrs, err := a.ctl.GetOpBriefAttributes(speclet)
			if err != nil {
				return nil, err
			}

			// Sanity check.
			for attr := range strawberryAttrs {
				if _, ok := ctlAttrs[attr]; ok {
					return nil, fmt.Errorf("this is a bug, attribute %v is duplicated", attr)
				}
			}

			resultAttrs = make(map[string]any)
			for _, attr := range attributes {
				if value, ok := strawberryAttrs[attr]; ok {
					resultAttrs[attr] = value
				} else if value, ok := ctlAttrs[attr]; ok {
					resultAttrs[attr] = value
				} else {
					return nil, yterrors.Err(
						fmt.Sprintf("unknown attribute %v", attr),
						yterrors.Attr("attribute", attr))
				}
			}
		}

		result[i] = AliasWithAttrs{
			Alias: op.Alias,
			Attrs: resultAttrs,
		}
	}

	return result, nil
}

func (a *API) GetSpeclet(ctx context.Context, alias string) (speclet yson.RawValue, err error) {
	if err = a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return
	}
	if err = a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return
	}
	err = a.ytc.GetNode(
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
		if err := a.validatePoolOption(ctx, pool); err != nil {
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
	err := a.ytc.GetNode(ctx, specletPath.Attrs(), &node, &yt.GetNodeOptions{Attributes: []string{"revision", "value"}})
	if err != nil {
		return err
	}

	if _, ok := speclet["family"]; !ok {
		speclet["family"] = node.Speclet.Family
	}
	if _, ok := speclet["stage"]; !ok {
		speclet["stage"] = node.Speclet.Stage
	}

	err = a.ytc.SetNode(
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

func (a *API) SetOptions(ctx context.Context, alias string, options map[string]any) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}

	if pool, ok := options["pool"]; ok {
		if err := a.validatePoolOption(ctx, pool); err != nil {
			return err
		}
	}

	var node struct {
		Speclet  map[string]any `yson:"value"`
		Revision yt.Revision    `yson:"revision"`
	}
	specletPath := a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet")
	err := a.ytc.GetNode(ctx, specletPath.Attrs(), &node, &yt.GetNodeOptions{Attributes: []string{"revision", "value"}})
	if err != nil {
		return err
	}

	for key, value := range options {
		node.Speclet[key] = value
	}

	return a.ytc.SetNode(
		ctx,
		specletPath,
		node.Speclet,
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
	agentInfo.Stage = strawberry.UntrackedStage
	agentInfo.OperationNamespace = a.ctl.Family() + ":" + strawberry.UntrackedStage
	return agentInfo
}

func (a *API) Start(ctx context.Context, alias string, untracked bool, userClient yt.Client) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	if err := a.SetOption(ctx, alias, "active", true); err != nil {
		return err
	}
	if !untracked {
		if err := a.SetOption(ctx, alias, "stage", a.cfg.AgentInfo.Stage); err != nil {
			return err
		}
		return nil
	}
	if err := a.SetOption(ctx, alias, "stage", strawberry.UntrackedStage); err != nil {
		return err
	}
	agentInfo := a.getAgentInfoForUntrackedStage()
	oplet, err := a.getOplet(ctx, alias, userClient, agentInfo)
	if err != nil {
		return err
	}
	return oplet.Pass(ctx)
}

func (a *API) Stop(ctx context.Context, alias string) error {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionManage); err != nil {
		return err
	}
	var stage string
	err := a.ytc.GetNode(
		ctx,
		a.cfg.AgentInfo.StrawberryRoot.JoinChild(alias, "speclet", "stage"),
		&stage,
		nil)
	if err != nil {
		return err
	}
	if err = a.SetOption(ctx, alias, "active", false); err != nil {
		return err
	}
	if stage != strawberry.UntrackedStage {
		return nil
	}
	agentInfo := a.getAgentInfoForUntrackedStage()
	oplet, err := a.getOplet(ctx, alias, nil, agentInfo)
	if err != nil {
		return err
	}
	return oplet.Pass(ctx)
}

func (a *API) DescribeOptions(ctx context.Context, alias string) ([]strawberry.OptionGroupDescriptor, error) {
	if err := a.CheckExistence(ctx, alias, true /*shouldExist*/); err != nil {
		return nil, err
	}
	if err := a.CheckPermissionToOp(ctx, alias, yt.PermissionRead); err != nil {
		return nil, err
	}

	var specletYson yson.RawValue
	err := a.ytc.GetNode(
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
