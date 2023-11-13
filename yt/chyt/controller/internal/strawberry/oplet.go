package strawberry

import (
	"context"
	"reflect"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

// AgentInfo contains information about the Agent which is needed in Oplet.
type AgentInfo struct {
	StrawberryRoot     ypath.Path
	Hostname           string
	Stage              string
	Proxy              string
	Family             string
	OperationNamespace string
	// RobotUsername is needed for a temporary workaround to add the robot to the operation acl.
	//
	// TODO(dakovalkov): remove after YT-17557
	RobotUsername         string
	DefaultNetworkProject *string
	ClusterURL            string
}

func DescribeOptions(a AgentInfo, speclet Speclet) []OptionGroupDescriptor {
	return []OptionGroupDescriptor{
		{
			Title: "Basic options",
			Options: []OptionDescriptor{
				{
					Title:        "Pool",
					Name:         "pool",
					Type:         TypePool,
					CurrentValue: speclet.Pool,
					Description:  "Name of the compute pool to start a corresponding YT operation in.",
				},
				{
					Title:        "Network project",
					Name:         "network_project",
					Type:         TypeString,
					CurrentValue: speclet.NetworkProject,
					DefaultValue: a.DefaultNetworkProject,
				},
				{
					Title:        "Preemption mode",
					Name:         "preemption_mode",
					Type:         TypeString,
					CurrentValue: speclet.PreemptionMode,
					DefaultValue: "normal",
					Choices:      []any{"normal", "graceful"},
					Description:  "Preemption mode for a corresponding YT operation.",
				},
				{
					Title:        "Restart on speclet change",
					Name:         "restart_on_speclet_change",
					Type:         TypeBool,
					CurrentValue: speclet.RestartOnSpecletChange,
					DefaultValue: DefaultRestartOnSpecletChange,
					Description:  "If true, automatically restart a corresponding YT operation on every speclet change.",
				},
			},
		},
	}
}

type OpletOptions struct {
	AgentInfo
	Alias        string
	Controller   Controller
	Logger       log.Logger
	UserClient   yt.Client
	SystemClient yt.Client
}

const (
	StageUntracked = "untracked"
)

type Oplet struct {
	alias           string
	persistentState PersistentState
	infoState       InfoState

	// specletYson is a full unparsed speclet from a speclet cypress node.
	specletYson yson.RawValue
	// specletModificationTime is a modification time of the speclet cypress node.
	specletModificationTime yson.Time
	// strawberrySpeclet is a parsed part of the specletYson with general options.
	strawberrySpeclet Speclet
	// controllerSpeclet is a parsed part of the specletYson with controller specific options.
	controllerSpeclet any

	// ytOpStrawberrySpeclet is a parsed part of the persistentState.ytOpSpeclet with general options.
	ytOpStrawberrySpeclet Speclet
	// ytOpControllerSpeclet is a parsed part of the persistentState.ytOpSpeclet with controller specific options.
	ytOpControllerSpeclet any

	// strawberryStateModificationTime is a modification time of the strawberry cypress node.
	strawberryStateModificationTime yson.Time
	// strawberryStateModificationTime is a creation time of the strawberry cypress node.
	strawberryStateCreationTime yson.Time

	// flushedPersistentState is the last flushed persistentState. It is used to detect an external
	// state change in the cypress and a local change of persistentState. It would be enough to
	// store just a hash of flushed state, but the cool reflection hash-lib is not in arcadia yet.
	flushedPersistentState PersistentState
	// flushedInfoState is the last flushed infoState. It used to detect a local change of the state
	// alongside with flushedPersistentState.
	flushedInfoState InfoState
	// flushedStateRevision is the last known revision of a cypress node where persistentState and
	// infoState are stored. It is used as a prerequisite revision during a flushPersistentState
	// phase to guarantee that we will not override the cypress state if it was changed externally.
	flushedStateRevision yt.Revision

	// pendingUpdateFromCypressNode is set when we know that the oplet cypress state has been changed
	// (e.g. from RevisionTracker) and we need to reload it from a cypress during the next pass.
	pendingUpdateFromCypressNode bool

	// pendingRestart is set when an external event triggers operation restart
	// (e.g. changing cluster_connection).
	//
	// TODO(dakovalkov): We should not rely on non-persistent fields,
	// because it is not fault-tolerant. Eliminate this.
	pendingRestart bool

	// brokenError is an error that led the oplet to broken state.
	// Flush persistent state is not allowed for a broken oplet because the state may be unparsed yet.
	// So this error cannot be a part of persistent or info state and cannot be flushed.
	brokenError error
	// brokenReason is a brief reason why the oplet is in broken state.
	// It contains a message from brokenError without any details like attributes and inner errors.
	brokenReason string

	acl []yt.ACE

	cypressNode  ypath.Path
	l            log.Logger
	c            Controller
	userClient   yt.Client
	systemClient yt.Client

	agentInfo AgentInfo
}

func NewOplet(options OpletOptions) *Oplet {
	oplet := &Oplet{
		alias:                        options.Alias,
		pendingUpdateFromCypressNode: true,
		cypressNode:                  options.StrawberryRoot.Child(options.Alias),
		l:                            log.With(options.Logger, log.String("alias", options.Alias)),
		c:                            options.Controller,
		userClient:                   options.UserClient,
		systemClient:                 options.SystemClient,
		agentInfo:                    options.AgentInfo,
	}
	return oplet
}

// TODO(dakovalkov): eliminate this.
func (oplet *Oplet) SetPendingRestart(reason string) {
	oplet.l.Debug("setting pending restart", log.String("reason", reason))
	oplet.pendingRestart = true
}

func (oplet *Oplet) Alias() string {
	return oplet.alias
}

func (oplet *Oplet) NextIncarnationIndex() int {
	return oplet.persistentState.IncarnationIndex + 1
}

func (oplet *Oplet) StrawberrySpeclet() Speclet {
	return oplet.strawberrySpeclet
}

func (oplet *Oplet) ControllerSpeclet() any {
	return oplet.controllerSpeclet
}

func (oplet *Oplet) Active() bool {
	return oplet.strawberrySpeclet.ActiveOrDefault()
}

func (oplet *Oplet) Untracked() bool {
	return oplet.strawberrySpeclet.StageOrDefault() == StageUntracked && oplet.Active()
}

func (oplet *Oplet) CypressNode() ypath.Path {
	return oplet.cypressNode
}

func (oplet *Oplet) Broken() bool {
	return oplet.brokenError != nil
}

func (oplet *Oplet) BrokenReason() string {
	return oplet.brokenReason
}

func (oplet *Oplet) BrokenError() error {
	return oplet.brokenError
}

// Inappropriate returns |true| whenever the oplet does not belong to provided agent.
func (oplet *Oplet) Inappropriate() bool {
	return oplet.strawberrySpeclet.FamilyOrDefault() != oplet.c.Family() ||
		oplet.strawberrySpeclet.StageOrDefault() != oplet.agentInfo.Stage
}

func (oplet *Oplet) HasYTOperation() bool {
	return oplet.persistentState.YTOpID != yt.NullOperationID &&
		!oplet.persistentState.YTOpState.IsFinished()
}

func (oplet *Oplet) UpToDateWithCypress() bool {
	return !oplet.pendingUpdateFromCypressNode
}

func (oplet *Oplet) OperationInfo() (yt.OperationID, yt.OperationState) {
	return oplet.persistentState.YTOpID, oplet.persistentState.YTOpState
}

type OpletState string

const (
	OpletStateActive    OpletState = "active"
	OpletStateInactive  OpletState = "inactive"
	OpletStateUntracked OpletState = "untracked"
)

func (oplet *Oplet) State() OpletState {
	if oplet.Untracked() {
		return OpletStateUntracked
	} else if oplet.Active() {
		return OpletStateActive
	} else {
		return OpletStateInactive
	}
}

type OpletStatus string

const (
	OpletStatusGood    OpletStatus = "good"
	OpletStatusPending OpletStatus = "pending"
	OpletStatusFailed  OpletStatus = "failed"
)

func (oplet *Oplet) Status() (status OpletStatus, statusReason string) {
	if oplet.Broken() {
		return OpletStatusFailed, "oplet is broken: " + oplet.brokenReason
	}
	if oplet.infoState.Error != nil {
		return OpletStatusFailed, "info state contains error"
	}

	if ok, reason := oplet.needsRestart(); ok {
		if oplet.Untracked() {
			return OpletStatusFailed, "untracked operation is pending restart: " + reason
		} else {
			return OpletStatusPending, "operation is pending restart: " + reason
		}
	}
	if ok, reason := oplet.needsAbort(); ok {
		if oplet.Untracked() {
			return OpletStatusFailed, "untracked operation is pending abort: " + reason
		} else {
			return OpletStatusPending, "operation is pending abort: " + reason
		}
	}
	if ok, reason := oplet.needsUpdateOpParameters(); ok {
		if oplet.Untracked() {
			return OpletStatusFailed, "untracked operation is pending update op parameters: " + reason
		} else {
			return OpletStatusPending, "operation is pending update op parameters: " + reason
		}
	}

	if oplet.Active() {
		if oplet.persistentState.YTOpState == yt.StatePending {
			return OpletStatusFailed, "operation is in pending state: max running operation count is probably exceeded"
		} else if oplet.persistentState.YTOpState != yt.StateRunning {
			return OpletStatusPending, "operation is not in running state"
		}
	}

	return OpletStatusGood, ""
}

// setBroken sets oplet state to broken and returns corresponding error.
func (oplet *Oplet) setBroken(reason string, args ...any) (brokenError error) {
	oplet.brokenReason = reason
	oplet.brokenError = yterrors.Err(append(args, reason)...)
	oplet.l.Debug("Oplet is broken", log.Error(oplet.brokenError))
	return oplet.brokenError
}

func (oplet *Oplet) OnCypressNodeChanged() {
	oplet.pendingUpdateFromCypressNode = true
}

func (oplet *Oplet) SetACL(acl []yt.ACE) {
	if !reflect.DeepEqual(oplet.acl, acl) {
		oplet.acl = acl
	}
}

func (oplet *Oplet) EnsureUpdatedFromCypress(ctx context.Context) error {
	if oplet.pendingUpdateFromCypressNode {
		if err := oplet.updateFromCypressNode(ctx); err != nil {
			return err
		}
	}
	if oplet.acl == nil {
		if err := oplet.UpdateACLFromNode(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (oplet *Oplet) CheckOperationLiveness(ctx context.Context) error {
	opID := oplet.persistentState.YTOpID

	if opID == yt.NullOperationID {
		return nil
	}

	oplet.l.Debug("getting operation info", log.String("operation_id", opID.String()))

	ytOp, err := oplet.systemClient.GetOperation(ctx, opID, nil)
	if err != nil {
		if yterrors.ContainsMessageRE(err, noSuchOperationRE) {
			oplet.l.Info("operation with current operation id does not exist",
				log.String("operation_id", opID.String()))
			oplet.persistentState.YTOpState = yt.StateCompleted
		} else {
			oplet.l.Error("error getting operation info", log.Error(err))
			oplet.setError(err)
		}
		return err
	}

	oplet.l.Debug("operation found",
		log.String("operation_id", opID.String()),
		log.String("operation_state", string(ytOp.State)))

	oplet.persistentState.YTOpState = ytOp.State
	oplet.infoState.YTOpStartTime = ytOp.StartTime
	oplet.infoState.YTOpFinishTime = ytOp.FinishTime

	return nil
}

func (oplet *Oplet) EnsureOperationInValidState(ctx context.Context) error {
	if ok, reason := oplet.needsRestart(); ok {
		if err := oplet.restartOp(ctx, reason); err != nil {
			return err
		}
	}
	if ok, reason := oplet.needsUpdateOpParameters(); ok {
		if err := oplet.updateOpParameters(ctx, reason); err != nil {
			return err
		}
	}
	if ok, reason := oplet.needsAbort(); ok {
		if err := oplet.abortOp(ctx, reason); err != nil {
			return err
		}
	}
	return nil
}

func (oplet *Oplet) EnsurePersistentStateFlushed(ctx context.Context) (err error) {
	if oplet.needFlushPersistentState() {
		for {
			err = oplet.flushPersistentState(ctx)
			if !yterrors.ContainsMessageRE(err, prerequisiteCheckFailedRE) {
				break
			}
			err = oplet.updateFromCypressNode(ctx)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (oplet *Oplet) needFlushPersistentState() bool {
	return !reflect.DeepEqual(oplet.persistentState, oplet.flushedPersistentState) ||
		!reflect.DeepEqual(oplet.infoState, oplet.flushedInfoState)
}

func (oplet *Oplet) setError(err error) {
	oplet.l.Debug("oplet is in error state", log.Error(err))
	stringError := err.Error()
	oplet.infoState.Error = &stringError
}

func (oplet *Oplet) clearError() {
	oplet.infoState.Error = nil
}

func (oplet *Oplet) needsRestart() (needsRestart bool, reason string) {
	if !oplet.Active() {
		return false, "oplet is in inactive state"
	}
	if !oplet.HasYTOperation() {
		return true, "oplet does not have running yt operation"
	}
	if oplet.strawberrySpeclet.RestartOnSpecletChangeOrDefault() {
		if !reflect.DeepEqual(oplet.ytOpControllerSpeclet, oplet.controllerSpeclet) {
			oplet.l.Debug("speclet diff", log.Any("diff", specletDiff(oplet.ytOpControllerSpeclet, oplet.controllerSpeclet)))
			return true, "speclet changed"
		}
		if !reflect.DeepEqual(oplet.strawberrySpeclet.RestartRequiredOptions, oplet.ytOpStrawberrySpeclet.RestartRequiredOptions) {
			oplet.l.Debug("strawberry speclet diff",
				log.Any("diff",
					specletDiff(oplet.strawberrySpeclet.RestartRequiredOptions, oplet.ytOpStrawberrySpeclet.RestartRequiredOptions)))
			return true, "strawberry speclet changed"
		}
	}
	if oplet.strawberrySpeclet.MinSpecletRevision > oplet.persistentState.YTOpSpecletRevision {
		if oplet.strawberrySpeclet.MinSpecletRevision > oplet.persistentState.SpecletRevision {
			oplet.l.Warn("min speclet revision is greater than last seen speclet revision; "+
				"it can lead to infinite operation restart",
				log.UInt64("min_speclet_revision", uint64(oplet.strawberrySpeclet.MinSpecletRevision)),
				log.UInt64("last_seen_speclet_revision", uint64(oplet.persistentState.SpecletRevision)))
		}
		return true, "min speclet revision is unsatisfied"
	}
	// TODO(dakovalkov): eliminate this.
	if oplet.pendingRestart {
		return true, "pendingRestart is set"
	}
	return false, "up to date"
}

func (oplet *Oplet) needsUpdateOpParameters() (needsUpdate bool, reason string) {
	if !oplet.Active() {
		return false, "oplet is in inactive state"
	}
	if !oplet.HasYTOperation() {
		return false, "oplet does not have running yt operation"
	}
	if !reflect.DeepEqual(oplet.getOpACL(), oplet.persistentState.YTOpACL) {
		return true, "acl changed"
	}
	if !reflect.DeepEqual(oplet.strawberrySpeclet.Pool, oplet.persistentState.YTOpPool) {
		return true, "pool changed"
	}
	return false, "up to date"
}

func (oplet *Oplet) needsAbort() (needsAbort bool, reason string) {
	if !oplet.HasYTOperation() {
		return false, "oplet does not have running yt operation"
	}
	if !oplet.strawberrySpeclet.ActiveOrDefault() {
		return true, "oplet is in inactive state"
	}
	if oplet.strawberrySpeclet.StageOrDefault() != StageUntracked && oplet.strawberrySpeclet.Pool == nil {
		return true, "pool is not set"
	}
	return false, "up to date"
}

func (oplet *Oplet) needsBackoff() bool {
	return oplet.persistentState.BackoffUntil.After(time.Now())
}

func (oplet *Oplet) increaseBackoff() {
	backoffDuration := oplet.persistentState.BackoffDuration
	if backoffDuration == time.Duration(0) {
		backoffDuration = initialBackoffDuration
	}

	oplet.persistentState.BackoffUntil = time.Now().Add(backoffDuration)

	backoffDuration = time.Duration(float64(backoffDuration) * exponentialBackoffFactor)
	if backoffDuration >= maxBackoffDuration {
		backoffDuration = maxBackoffDuration
	}
	oplet.persistentState.BackoffDuration = backoffDuration
}

func (oplet *Oplet) resetBackoff() {
	oplet.persistentState.BackoffUntil = time.Time{}
	oplet.persistentState.BackoffDuration = time.Duration(0)
}

var CypressStateAttributes = []string{
	"strawberry_persistent_state",
	"strawberry_info_state",
	"revision",
	"creation_time",
	"modification_time",
	"value",
}

func (oplet *Oplet) updateFromCypressNode(ctx context.Context) error {
	oplet.l.Info("updating strawberry operations state from cypress",
		log.UInt64("state_revision", uint64(oplet.flushedStateRevision)),
		log.UInt64("speclet_revision", uint64(oplet.persistentState.SpecletRevision)))

	var node yson.RawValue

	err := oplet.systemClient.GetNode(ctx, oplet.cypressNode, &node, &yt.GetNodeOptions{Attributes: CypressStateAttributes})

	if yterrors.ContainsResolveError(err) {
		return oplet.setBroken("cypress state does not exist", err)
	} else if err != nil {
		oplet.l.Error("faied to get operation state from cypress", log.Error(err))
		return err
	}

	oplet.pendingUpdateFromCypressNode = false

	return oplet.updateFromYsonNode(node)
}

func (oplet *Oplet) updateFromYsonNode(nodeValue yson.RawValue) error {
	initialUpdate := oplet.flushedStateRevision == 0

	var node struct {
		PersistentState  PersistentState `yson:"strawberry_persistent_state,attr"`
		InfoState        InfoState       `yson:"strawberry_info_state,attr"`
		Revision         yt.Revision     `yson:"revision,attr"`
		CreationTime     yson.Time       `yson:"creation_time,attr"`
		ModificationTime yson.Time       `yson:"modification_time,attr"`
		Speclet          *struct {
			Value            yson.RawValue `yson:"value,attr"`
			ModificationTime yson.Time     `yson:"modification_time,attr"`
			Revision         yt.Revision   `yson:"revision,attr"`
		} `yson:"speclet"`
	}

	err := yson.Unmarshal(nodeValue, &node)
	if err != nil {
		return oplet.setBroken("failed to parse cypress state node", err)
	}

	// Validate operation node.

	if node.Speclet == nil {
		return oplet.setBroken("speclet node is missing")
	}

	if node.Speclet.Value == nil {
		return oplet.setBroken("speclet node is empty")
	}

	var strawberrySpeclet Speclet
	err = yson.Unmarshal(node.Speclet.Value, &strawberrySpeclet)
	if err != nil {
		return oplet.setBroken("failed to parse strawberry speclet from node",
			err,
			yterrors.Attr("speclet_yson", node.Speclet.Value),
			yterrors.Attr("speclet_revision", uint64(node.Speclet.Revision)))
	}

	controllerSpeclet, err := oplet.c.ParseSpeclet(node.Speclet.Value)
	if err != nil {
		return oplet.setBroken("failed to parse controller speclet from node",
			err,
			yterrors.Attr("speclet_yson", string(node.Speclet.Value)),
			yterrors.Attr("speclet_revision", uint64(node.Speclet.Revision)))
	}

	oplet.l.Debug("state collected and validated")

	// Handle persistent state change.
	if !reflect.DeepEqual(node.PersistentState, oplet.flushedPersistentState) {
		if !initialUpdate {
			oplet.l.Info("cypress persistent state change detected; loading it",
				log.UInt64("flushed_state_revision", uint64(oplet.flushedStateRevision)),
				log.UInt64("cypress_state_revision", uint64(node.Revision)))

			if !reflect.DeepEqual(oplet.persistentState, oplet.flushedPersistentState) {
				oplet.l.Warn("cypress and local state conflict detected; local state will be overridden",
					log.UInt64("flushed_state_revision", uint64(oplet.flushedStateRevision)),
					log.UInt64("cypress_state_revision", uint64(node.Revision)))
			}
		}
		oplet.persistentState = node.PersistentState
		oplet.flushedPersistentState = node.PersistentState
		err := yson.Unmarshal(oplet.persistentState.YTOpSpeclet, &oplet.ytOpStrawberrySpeclet)
		if err != nil {
			oplet.l.Warn("failed to parse strawberry speclet from persistent state",
				log.Error(err),
				log.Binary("yt_op_speclet", oplet.persistentState.YTOpSpeclet))
			oplet.ytOpStrawberrySpeclet = Speclet{}
			err = nil
		}
		oplet.ytOpControllerSpeclet, err = oplet.c.ParseSpeclet(oplet.persistentState.YTOpSpeclet)
		if err != nil {
			oplet.l.Warn("failed to parse controller speclet from persistent state",
				log.Error(err),
				log.Binary("yt_op_speclet", oplet.persistentState.YTOpSpeclet))
			oplet.ytOpControllerSpeclet = nil
			err = nil
		}
	}
	oplet.infoState = node.InfoState
	oplet.flushedInfoState = node.InfoState
	oplet.flushedStateRevision = node.Revision

	oplet.specletYson = node.Speclet.Value
	oplet.persistentState.SpecletRevision = node.Speclet.Revision
	oplet.strawberryStateModificationTime = node.ModificationTime
	oplet.strawberryStateCreationTime = node.CreationTime
	oplet.specletModificationTime = node.Speclet.ModificationTime
	oplet.strawberrySpeclet = strawberrySpeclet
	oplet.controllerSpeclet = controllerSpeclet

	oplet.l.Info("strawberry operation state updated from cypress",
		log.UInt64("state_revision", uint64(oplet.flushedStateRevision)),
		log.UInt64("speclet_revision", uint64(oplet.flushedStateRevision)))

	return nil
}

func (oplet *Oplet) LoadFromYsonNode(node yson.RawValue, acl []yt.ACE) error {
	// Assume that the provided yson node is a freshly loaded state from cypress,
	// do not need to update from cypress once again.
	oplet.pendingUpdateFromCypressNode = false

	if err := oplet.updateFromYsonNode(node); err != nil {
		return err
	}
	if acl == nil {
		return oplet.setBroken("acl node is missing")
	}
	oplet.SetACL(acl)
	return nil
}

func (oplet *Oplet) getACLFromNode(ctx context.Context) (acl []yt.ACE, err error) {
	aclPath := AccessControlNamespacesPath.JoinChild(oplet.c.Family(), oplet.alias).Attr("principal_acl")
	err = oplet.systemClient.GetNode(ctx, aclPath, &acl, nil)
	return
}

func (oplet *Oplet) UpdateACLFromNode(ctx context.Context) error {
	oplet.l.Info("updating acl from access node")

	acl, err := oplet.getACLFromNode(ctx)
	if yterrors.ContainsResolveError(err) {
		return oplet.setBroken("acl node is missing", err)
	} else if err != nil {
		oplet.l.Error("error getting acl from access node")
		return err
	}

	oplet.SetACL(acl)

	oplet.l.Info("acl updated from access node")

	return nil
}

func (oplet *Oplet) abortOp(ctx context.Context, reason string) error {
	oplet.l.Info("aborting operation", log.String("reason", reason))

	err := oplet.systemClient.AbortOperation(
		ctx,
		yt.OperationID(oplet.persistentState.YTOpID),
		&yt.AbortOperationOptions{})

	if err != nil {
		oplet.setError(err)
	} else {
		oplet.persistentState.YTOpState = yt.StateAborted
	}
	return err
}

func (oplet *Oplet) getOpACL() (acl []yt.ACE) {
	if oplet.agentInfo.RobotUsername != "" {
		acl = []yt.ACE{
			yt.ACE{
				Action:      yt.ActionAllow,
				Subjects:    []string{oplet.agentInfo.RobotUsername},
				Permissions: []yt.Permission{yt.PermissionRead, yt.PermissionManage},
			},
		}
	}
	readACL := toOperationACL(oplet.acl)
	if readACL != nil {
		acl = append(acl, readACL...)
	}
	return
}

func (oplet *Oplet) restartOp(ctx context.Context, reason string) error {
	oplet.l.Info("restarting operation",
		log.Int("next_incarnation_index", oplet.NextIncarnationIndex()),
		log.String("reason", reason))
	spec, description, annotations, err := oplet.c.Prepare(ctx, oplet)

	if err != nil {
		oplet.setError(err)
		return err
	}

	// Extend our own annotations and description by controller's annotation and description.
	strawberryAnnotations := oplet.OpAnnotations()
	for k, v := range strawberryAnnotations {
		annotations[k] = v
	}
	strawberryDescription := oplet.OpDescription()
	for k, v := range strawberryDescription {
		description[k] = v
	}

	// TODO(gudqeit): move speclet patching to a separate method.
	spec["annotations"] = annotations
	spec["description"] = description
	spec["alias"] = "*" + oplet.alias
	if oplet.strawberrySpeclet.Pool != nil {
		spec["pool"] = *oplet.strawberrySpeclet.Pool
	} else if oplet.strawberrySpeclet.StageOrDefault() != StageUntracked {
		err := yterrors.Err("can't run operation because pool is not set")
		oplet.setError(err)
		return err
	}
	if oplet.strawberrySpeclet.PreemptionMode != nil {
		spec["preemption_mode"] = *oplet.strawberrySpeclet.PreemptionMode
	}

	networkProject := oplet.agentInfo.DefaultNetworkProject
	if oplet.strawberrySpeclet.NetworkProject != nil {
		networkProject = oplet.strawberrySpeclet.NetworkProject
	}

	if _, ok := spec["tasks"]; ok {
		tasks, ok := spec["tasks"].(map[string]any)
		if !ok {
			return yterrors.Err("tasks type is not 'map'",
				yterrors.Attr("tasks", spec["tasks"]),
				yterrors.Attr("type", reflect.TypeOf(spec["tasks"]).String()))
		}
		for key := range tasks {
			task, ok := tasks[key].(map[string]any)
			if !ok {
				return yterrors.Err("task type is not 'map'",
					yterrors.Attr("task_name", key),
					yterrors.Attr("task", tasks[key]),
					yterrors.Attr("type", reflect.TypeOf(tasks[key]).String()))
			}
			if networkProject != nil {
				task["network_project"] = *networkProject
			}
			if oplet.strawberrySpeclet.LayerPaths != nil {
				task["layer_paths"] = oplet.strawberrySpeclet.LayerPaths
			}
		}
	}

	opACL := oplet.getOpACL()
	spec["acl"] = opACL
	spec["add_authenticated_user_to_acl"] = false

	if oplet.HasYTOperation() {
		if err := oplet.abortOp(ctx, "operation restart"); err != nil {
			oplet.setError(err)
			return err
		}
	}

	opID, err := oplet.userClient.StartOperation(ctx, yt.OperationVanilla, spec, nil)

	// TODO(dakovalkov): Add GetOperationByAlias in go yt api and eliminate this.
	if yterrors.ContainsMessageRE(err, aliasAlreadyUsedRE) {
		oplet.l.Debug("alias is already used, aborting previous operation")
		// Try to abort already existing operation with that alias.
		oldOpID := extractOpID(err)
		oplet.l.Debug("aborting operation", log.String("operation_id", oldOpID.String()))
		abortErr := oplet.systemClient.AbortOperation(ctx, yt.OperationID(oldOpID), &yt.AbortOperationOptions{})
		if abortErr != nil {
			oplet.setError(abortErr)
			return abortErr
		}
		opID, err = oplet.userClient.StartOperation(ctx, yt.OperationVanilla, spec, nil)
	}

	if err != nil {
		oplet.setError(err)
		return err
	} else {
		oplet.clearError()
	}

	oplet.persistentState.YTOpID = opID
	oplet.persistentState.YTOpState = yt.StateInitializing
	oplet.infoState.YTOpStartTime = yson.Time{}
	oplet.infoState.YTOpFinishTime = yson.Time{}

	oplet.persistentState.YTOpSpeclet = oplet.specletYson
	oplet.persistentState.YTOpSpecletRevision = oplet.persistentState.SpecletRevision

	oplet.ytOpStrawberrySpeclet = oplet.strawberrySpeclet
	oplet.ytOpControllerSpeclet = oplet.controllerSpeclet

	oplet.persistentState.YTOpACL = opACL
	oplet.persistentState.YTOpPool = oplet.strawberrySpeclet.Pool

	oplet.persistentState.IncarnationIndex++

	// TODO(dakovalkov): eliminate this.
	oplet.pendingRestart = false

	oplet.l.Info("operation started", log.String("operation_id", opID.String()),
		log.Int("incarnation_index", oplet.persistentState.IncarnationIndex))

	return nil
}

func (oplet *Oplet) updateOpParameters(ctx context.Context, reason string) error {
	oplet.l.Info("updating operation parameters", log.String("reason", reason))

	opACL := oplet.getOpACL()
	err := oplet.systemClient.UpdateOperationParameters(
		ctx,
		oplet.persistentState.YTOpID,
		map[string]any{
			"acl":  opACL,
			"pool": oplet.strawberrySpeclet.Pool,
		},
		nil)

	if err != nil {
		oplet.l.Error("error updating operation parameters", log.Error(err))
		oplet.setError(err)
		return err
	}

	oplet.l.Info("operation parameters updated")

	oplet.persistentState.YTOpACL = opACL
	oplet.persistentState.YTOpPool = oplet.strawberrySpeclet.Pool

	return nil
}

func (oplet *Oplet) flushPersistentState(ctx context.Context) error {
	oplet.l.Info("flushing new operation's state",
		log.UInt64("flushed_state_revision", uint64(oplet.flushedStateRevision)))

	// Sanity check, should never happen.
	if oplet.Broken() {
		return yterrors.Err("cannot flush persistent state of broken oplet",
			yterrors.Attr("broken_reason", oplet.brokenReason))
	}

	annotation := oplet.CypAnnotation()

	// Always override controller's address on flush.
	oplet.infoState.Controller.Address = oplet.agentInfo.Hostname

	err := oplet.systemClient.MultisetAttributes(
		ctx,
		oplet.cypressNode.Attrs(),
		map[string]any{
			"strawberry_persistent_state": oplet.persistentState,
			"strawberry_info_state":       oplet.infoState,
			"annotation":                  annotation,
		},
		&yt.MultisetAttributesOptions{
			PrerequisiteOptions: &yt.PrerequisiteOptions{
				Revisions: []yt.PrerequisiteRevision{
					{
						Path:     oplet.cypressNode,
						Revision: oplet.flushedStateRevision,
					},
				},
			},
		})

	if err != nil {
		if yterrors.ContainsMessageRE(err, prerequisiteCheckFailedRE) {
			oplet.l.Info("prerequisite check failed during flushing persistent state", log.Error(err))
			// If the state revision is outdated, we can not flush the state because we can accidentally
			// override changed cypress state. In that case we assume that cypress state is correct
			// and reload it on the next pass.
			oplet.pendingUpdateFromCypressNode = true
		} else {
			oplet.l.Error("error flushing operation", log.Error(err))
		}
		return err
	}

	oplet.l.Info("operation's persistent state flushed")

	oplet.flushedPersistentState = oplet.persistentState
	oplet.flushedInfoState = oplet.infoState

	// flushedStateRevision is outdated after flushing.
	// We can not do set and get_revision atomically now, so just reload the whole state from cypress.
	oplet.pendingUpdateFromCypressNode = true

	return nil
}

func (oplet *Oplet) Pass(ctx context.Context) error {
	err := oplet.EnsureUpdatedFromCypress(ctx)

	// If something has changed, the error may go away,
	// so reset backoff and try to process op again.
	if oplet.needFlushPersistentState() {
		oplet.resetBackoff()
	}

	// Skip further processing if the oplet does not belong to the controller or is broken.
	if oplet.Broken() || oplet.Inappropriate() || oplet.needsBackoff() {
		return err
	}

	if err == nil {
		err = oplet.CheckOperationLiveness(ctx)
	}
	if err == nil {
		err = oplet.EnsureOperationInValidState(ctx)
	}

	if err == nil {
		oplet.resetBackoff()
	} else {
		oplet.increaseBackoff()
	}

	// We always try to flush the state, even if the error has occurred,
	// because we want to persist this error in cypress.
	flushErr := oplet.EnsurePersistentStateFlushed(ctx)

	if err != nil {
		if flushErr != nil {
			oplet.l.Debug("failed to flush error to cypress", log.Error(flushErr))
		}
		return err
	} else {
		return flushErr
	}
}

type YTOperationBriefInfo struct {
	ID         yt.OperationID    `yson:"id,omitempty" json:"id,omitempty"`
	URL        string            `yson:"url,omitempty" json:"url,omitempty"`
	State      yt.OperationState `yson:"state,omitempty" json:"state,omitempty"`
	StartTime  *yson.Time        `yson:"start_time,omitempty" json:"start_time,omitempty"`
	FinishTime *yson.Time        `yson:"finish_time,omitempty" json:"finish_time,omitempty"`
}

type OpletBriefInfo struct {
	State                           OpletState           `yson:"state" json:"state"`
	Status                          OpletStatus          `yson:"status" json:"status"`
	StatusReason                    string               `yson:"status_reason" json:"status_reason"`
	SpecletDiff                     map[string]FieldDiff `yson:"speclet_diff,omitempty" json:"speclet_diff,omitempty"`
	YTOperation                     YTOperationBriefInfo `yson:"yt_operation,omitempty" json:"yt_operation,omitempty"`
	Creator                         string               `yson:"creator,omitempty" json:"creator,omitempty"`
	Pool                            string               `yson:"pool,omitempty" json:"pool,omitempty"`
	Stage                           string               `yson:"stage" json:"stage"`
	CreationTime                    *yson.Time           `yson:"creation_time,omitempty" json:"creation_time,omitempty"`
	StrawberryStateModificationTime *yson.Time           `yson:"strawberry_state_modification_time,omitempty" json:"strawberry_state_modification_time,omitempty"`
	SpecletModificationTime         *yson.Time           `yson:"speclet_modification_time,omitempty" json:"speclet_modification_time,omitempty"`
	IncarnationIndex                int                  `yson:"incarnation_index" json:"incarnation_index"`
	CtlAttributes                   map[string]any       `yson:"ctl_attributes" json:"ctl_attributes"`
	Error                           string               `yson:"error,omitempty" json:"error,omitempty"`
}

// GetBriefInfo should work even if oplet is broken.
func (oplet *Oplet) GetBriefInfo() (briefInfo OpletBriefInfo) {
	briefInfo.State = oplet.State()
	briefInfo.Status, briefInfo.StatusReason = oplet.Status()
	briefInfo.Creator = oplet.persistentState.Creator
	briefInfo.Stage = oplet.strawberrySpeclet.StageOrDefault()
	briefInfo.CreationTime = getYSONTimePointerOrNil(oplet.strawberryStateCreationTime)
	briefInfo.StrawberryStateModificationTime = getYSONTimePointerOrNil(oplet.strawberryStateModificationTime)
	briefInfo.SpecletModificationTime = getYSONTimePointerOrNil(oplet.specletModificationTime)
	briefInfo.IncarnationIndex = oplet.persistentState.IncarnationIndex
	if oplet.strawberrySpeclet.Pool != nil {
		briefInfo.Pool = *oplet.strawberrySpeclet.Pool
	}

	if oplet.persistentState.YTOpID != yt.NullOperationID {
		briefInfo.YTOperation.URL = operationStringURL(oplet.agentInfo.ClusterURL, oplet.persistentState.YTOpID)
		briefInfo.YTOperation.State = oplet.persistentState.YTOpState
		briefInfo.YTOperation.ID = oplet.persistentState.YTOpID
		briefInfo.YTOperation.StartTime = getYSONTimePointerOrNil(oplet.infoState.YTOpStartTime)
		briefInfo.YTOperation.FinishTime = getYSONTimePointerOrNil(oplet.infoState.YTOpFinishTime)

		if !oplet.Broken() && oplet.Active() {
			if !reflect.DeepEqual(oplet.ytOpControllerSpeclet, oplet.controllerSpeclet) {
				briefInfo.SpecletDiff = specletDiff(oplet.ytOpControllerSpeclet, oplet.controllerSpeclet)
			}
			if !reflect.DeepEqual(oplet.strawberrySpeclet.RestartRequiredOptions, oplet.ytOpStrawberrySpeclet.RestartRequiredOptions) {
				diff := specletDiff(oplet.ytOpStrawberrySpeclet.RestartRequiredOptions, oplet.strawberrySpeclet.RestartRequiredOptions)
				if briefInfo.SpecletDiff == nil {
					briefInfo.SpecletDiff = diff
				} else {
					for key, fieldDiff := range diff {
						briefInfo.SpecletDiff[key] = fieldDiff
					}
				}
			}
		}
	}

	if oplet.controllerSpeclet == nil {
		// NB: We should get a list of available options even if
		// an oplet is broken and there is no speclet.
		defaultSpeclet, _ := oplet.c.ParseSpeclet(yson.RawValue("{}"))
		briefInfo.CtlAttributes = oplet.c.GetOpBriefAttributes(defaultSpeclet)
	} else {
		briefInfo.CtlAttributes = oplet.c.GetOpBriefAttributes(oplet.controllerSpeclet)
	}

	if oplet.Broken() {
		briefInfo.Error = oplet.BrokenError().Error()
	} else if oplet.infoState.Error != nil {
		briefInfo.Error = *oplet.infoState.Error
	}

	return
}
