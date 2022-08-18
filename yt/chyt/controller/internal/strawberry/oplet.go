package strawberry

import (
	"context"
	"reflect"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

// AgentInfo contains information about the Agent which is nedeed in Oplet.
type AgentInfo struct {
	StrawberryRoot     ypath.Path
	Hostname           string
	Stage              string
	Proxy              string
	OperationNamespace string
}

type OpletOptions struct {
	AgentInfo
	Alias        string
	Controller   Controller
	Logger       log.Logger
	UserClient   yt.Client
	SystemClient yt.Client
}

type OpletState string

const (
	StateOK = "ok"
	// State is does not exist if there was a resolve error during update from cypress.
	StateDoesNotExist = "does_not_exist"

	StateAccessNodeMissing  = "access_node_missing"
	StateSpecletNodeMissing = "speclet_node_missing"
	StateInvalidSpeclet     = "invalid_speclet"
)

type Oplet struct {
	alias           string
	persistentState PersistentState
	infoState       InfoState

	// specletYson is the last speclet seen by the agent.
	specletYson yson.RawValue
	// strawberrySpeclet is a parsed part of the specletYson with general options.
	strawberrySpeclet Speclet

	// flushedPersistentState is the last flushed persistentState. It is used to detect an external
	// state change in the cypress and a local change of persistentState. It would be enough to
	// store just a hash of flushed state, but the cool reflection hash-lib is not in arcadia yet.
	flushedPersistentState PersistentState
	// flushedInfoState is the last flushed infoState. It used to detect a local change of the state
	// alongside with flushedPersistentState.
	flushedInfoState InfoState
	// flushedStateRevision is the last known revision of a cypress node where persistenState and
	// infoState are stored. It is used as a prerequisite revision during a flushPersistentState
	// phase to guarantee that we will not override the cypress state if it was changed externally.
	flushedStateRevision yt.Revision

	// pendingUpdateFromCypressNode is set when we know that the oplet cypress state has been changed
	// (e.g. from RevisionTracker) and we need to reload it from a cypress during the next pass.
	pendingUpdateFromCypressNode bool
	// pendingRestart is set when agent scheduled a restart of coresponding yt operation on the
	// next pass. It should be set via setPendingRestart only for proper log information.
	pendingRestart bool
	// pendingUpdateOpParameters is set when agent scheduled an operation parameters update on the
	// next pass. It should be set via setPendingUpdateOpParameters only for proper log information.
	pendingUpdateOpParameters bool

	state OpletState

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
		state:                        StateOK,
		cypressNode:                  options.StrawberryRoot.Child(options.Alias),
		l:                            log.With(options.Logger, log.String("alias", options.Alias)),
		c:                            options.Controller,
		userClient:                   options.UserClient,
		systemClient:                 options.SystemClient,
		agentInfo:                    options.AgentInfo,
	}
	oplet.infoState.Controller.Address = options.Hostname
	// Set same to the flushedInfoState to avoid flushing it after controller change.
	oplet.flushedInfoState = oplet.infoState
	return oplet
}

func (oplet *Oplet) Alias() string {
	return oplet.alias
}

func (oplet *Oplet) NextIncarnationIndex() int {
	return oplet.persistentState.IncarnationIndex + 1
}

func (oplet *Oplet) Speclet() yson.RawValue {
	return oplet.specletYson
}

func (oplet *Oplet) Active() bool {
	return oplet.strawberrySpeclet.ActiveOrDefault()
}

func (oplet *Oplet) CypressNode() ypath.Path {
	return oplet.cypressNode
}

func (oplet *Oplet) Broken() bool {
	return oplet.state == StateAccessNodeMissing ||
		oplet.state == StateSpecletNodeMissing ||
		oplet.state == StateInvalidSpeclet
}

func (oplet *Oplet) DoesNotExist() bool {
	return oplet.state == StateDoesNotExist
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

func (oplet *Oplet) SetState(state OpletState, reason string) {
	if oplet.state != state {
		oplet.l.Debug("Oplet state changed",
			log.String("old_state", string(oplet.state)),
			log.String("new_state", string(state)),
			log.String("reason", reason))
		oplet.state = state
	}
}

func (oplet *Oplet) OnCypressNodeChanged() {
	oplet.pendingUpdateFromCypressNode = true
}

func (oplet *Oplet) SetACL(acl []yt.ACE) {
	if !reflect.DeepEqual(oplet.acl, acl) {
		if !reflect.DeepEqual(toOperationACL(oplet.acl), toOperationACL(acl)) {
			oplet.setPendingUpdateOpParameters("ACL change")
		}
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
		oplet.setPendingRestart("strawberry operation does not have operation id")
		return nil
	}

	oplet.l.Debug("getting operation info", log.String("operation_id", opID.String()))

	ytOp, err := oplet.userClient.GetOperation(ctx, opID, nil)
	if err != nil {
		if yterrors.ContainsMessageRE(err, noSuchOperationRE) {
			oplet.setPendingRestart("operation with current operation id does not exist")
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

	if requiresRestart(ytOp.State) {
		oplet.setPendingRestart("operation state requires restart")
	}
	return nil
}

func (oplet *Oplet) EnsureOperationInValidState(ctx context.Context) error {
	if oplet.strawberrySpeclet.ActiveOrDefault() {
		if oplet.pendingRestart {
			if err := oplet.restartOp(ctx); err != nil {
				return err
			}
		}
		if oplet.pendingUpdateOpParameters {
			if err := oplet.updateOpParameters(ctx); err != nil {
				return err
			}
		}
	} else {
		if oplet.HasYTOperation() {
			if err := oplet.abortOp(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (oplet *Oplet) EnsurePersistentStateFlushed(ctx context.Context) error {
	if oplet.needFlushPersistentState() {
		return oplet.flushPersistentState(ctx)
	}
	return nil
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

func (oplet *Oplet) setPendingRestart(reason string) {
	if oplet.pendingRestart {
		oplet.l.Info("pendingRestart is already set", log.String("reason", reason))
		return
	}

	oplet.pendingRestart = true
	oplet.l.Info("setting pendingRestart", log.String("reason", reason))
}

func (oplet *Oplet) setPendingUpdateOpParameters(reason string) {
	if oplet.pendingUpdateOpParameters {
		oplet.l.Info("pendingUpdateOpParameters is already set", log.String("reason", reason))
		return
	}

	oplet.pendingUpdateOpParameters = true
	oplet.l.Info("setting pendingUpdateOpParameters", log.String("reason", reason))
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

func (oplet *Oplet) updateFromCypressNode(ctx context.Context) error {
	oplet.l.Info("updating strawberry operations state from cypress",
		log.UInt64("state_revision", uint64(oplet.flushedStateRevision)),
		log.UInt64("speclet_revision", uint64(oplet.persistentState.SpecletRevision)))

	initialUpdate := oplet.flushedStateRevision == 0

	// Collect full attributes of the node.

	var node struct {
		PersistentState PersistentState `yson:"strawberry_persistent_state,attr"`
		Revision        yt.Revision     `yson:"revision,attr"`
		Speclet         *struct {
			Value    yson.RawValue `yson:"value,attr"`
			Revision yt.Revision   `yson:"revision,attr"`
		} `yson:"speclet"`
	}

	// Keep in sync with structure above.
	attributes := []string{
		"strawberry_persistent_state", "revision", "value",
	}

	err := oplet.systemClient.GetNode(ctx, oplet.cypressNode, &node, &yt.GetNodeOptions{Attributes: attributes})

	if yterrors.ContainsResolveError(err) {
		// Node has gone.
		oplet.SetState(StateDoesNotExist, err.Error())
		return err
	} else if err != nil {
		oplet.l.Error("error getting operation state from cypress", log.Error(err))
		return err
	}

	oplet.pendingUpdateFromCypressNode = false

	// Validate operation node

	if node.Speclet == nil {
		oplet.SetState(StateSpecletNodeMissing, "speclet node is missing")
		return yterrors.Err("speclet node is missing")
	}

	if node.Speclet.Value == nil {
		oplet.SetState(StateInvalidSpeclet, "speclet node is empty")
		return yterrors.Err("speclet node is empty")
	}

	var strawberrySpeclet Speclet
	err = yson.Unmarshal(node.Speclet.Value, &strawberrySpeclet)
	if err != nil {
		oplet.SetState(StateInvalidSpeclet, "error parsing speclet node")
		err = yterrors.Err("error parsing speclet node", err,
			yterrors.Attr("speclet_yson", string(node.Speclet.Value)),
			yterrors.Attr("speclet_revision", uint64(node.Speclet.Revision)))
		oplet.l.Error("error parsing speclet node", log.Error(err))
		return err
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
	}
	oplet.flushedStateRevision = node.Revision

	// Handle speclet change.
	if oplet.persistentState.SpecletRevision != node.Speclet.Revision {
		// If it's an initial update, we do not have a valid old speclet.
		// In that case we need to restart the operation, though it can lead to an unnecessary restart.
		// TODO(dakovalkov): The better solution here is to store speclet hash in persistent state and compare it with new speclet hash.
		if initialUpdate || NeedRestartOnSpecletChange(&oplet.strawberrySpeclet, &strawberrySpeclet) ||
			oplet.c.NeedRestartOnSpecletChange(oplet.specletYson, node.Speclet.Value) {
			if !oplet.persistentState.SpecletChangeRequiresRestart {
				oplet.persistentState.SpecletChangeRequiresRestart = true
				oplet.l.Info("speclet change requires restart")
			}
		}
		if !reflect.DeepEqual(strawberrySpeclet.Pool, strawberrySpeclet.Pool) {
			oplet.setPendingUpdateOpParameters("pool change")
		}
	}
	oplet.strawberrySpeclet = strawberrySpeclet
	oplet.specletYson = node.Speclet.Value
	oplet.persistentState.SpecletRevision = node.Speclet.Revision

	// If the speclet was not changed or its change does not require a restart,
	// then we assume that the operation is running with the latest speclet revision.
	if !oplet.persistentState.SpecletChangeRequiresRestart {
		oplet.persistentState.OperationSpecletRevision = node.Speclet.Revision
	}

	if oplet.strawberrySpeclet.RestartOnSpecletChangeOrDefault() && oplet.persistentState.SpecletChangeRequiresRestart {
		oplet.setPendingRestart("speclet change")
	}

	if oplet.persistentState.OperationSpecletRevision < oplet.strawberrySpeclet.MinSpecletRevision {
		// Sanity check to avoid infinite restart.
		if oplet.strawberrySpeclet.MinSpecletRevision > oplet.persistentState.SpecletRevision {
			oplet.l.Warn("min_speclet_revision is larger than the last speclet_revision",
				log.UInt64("min_speclet_revision", uint64(oplet.strawberrySpeclet.MinSpecletRevision)),
				log.UInt64("last_speclet_revision", uint64(oplet.persistentState.SpecletRevision)))
		} else {
			oplet.setPendingRestart("unsatisfied min speclet revision")
		}
	}

	oplet.l.Info("strawberry operation state updated from cypress",
		log.UInt64("state_revision", uint64(oplet.flushedStateRevision)),
		log.UInt64("speclet_revision", uint64(oplet.flushedStateRevision)))

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
		oplet.SetState(StateAccessNodeMissing, "acl node is missing")
		return err
	} else if err != nil {
		oplet.l.Error("error geting acl from access node")
		return err
	}

	oplet.SetACL(acl)

	oplet.l.Info("acl updated from access node")

	return nil
}

func (oplet *Oplet) abortOp(ctx context.Context) error {
	err := oplet.userClient.AbortOperation(
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

func (oplet *Oplet) restartOp(ctx context.Context) error {
	oplet.l.Info("restarting operation", log.Int("next_incarnation_index", oplet.NextIncarnationIndex()))
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

	spec["annotations"] = annotations
	spec["description"] = description
	spec["alias"] = "*" + oplet.alias
	if oplet.strawberrySpeclet.Pool != nil {
		spec["pool"] = *oplet.strawberrySpeclet.Pool
	}
	if oplet.acl != nil {
		spec["acl"] = toOperationACL(oplet.acl)
	}

	if oplet.HasYTOperation() {
		if err := oplet.abortOp(ctx); err != nil {
			oplet.setError(err)
			return err
		}
	}

	opID, err := oplet.userClient.StartOperation(ctx, yt.OperationVanilla, spec, nil)

	// TODO(dakovalkov): Add GetOperationByAlias in go yt api and aliminate this.
	if yterrors.ContainsMessageRE(err, aliasAlreadyUsedRE) {
		oplet.l.Debug("alias is already used, aborting previous operation")
		// Try to abort already existing operation with that alias.
		oldOpID := extractOpID(err)
		oplet.l.Debug("aborting operation", log.String("operation_id", oldOpID.String()))
		abortErr := oplet.userClient.AbortOperation(ctx, yt.OperationID(oldOpID), &yt.AbortOperationOptions{})
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
	oplet.persistentState.OperationSpecletRevision = oplet.persistentState.SpecletRevision
	oplet.persistentState.IncarnationIndex++
	oplet.persistentState.SpecletChangeRequiresRestart = false

	oplet.l.Info("operation started", log.String("operation_id", opID.String()),
		log.Int("incarnation_index", oplet.persistentState.IncarnationIndex))

	oplet.pendingRestart = false
	oplet.pendingUpdateOpParameters = false

	return nil
}

func (oplet *Oplet) updateOpParameters(ctx context.Context) error {
	oplet.l.Info("updating operation parameters")

	err := oplet.userClient.UpdateOperationParameters(
		ctx,
		oplet.persistentState.YTOpID,
		map[string]interface{}{
			"acl":  toOperationACL(oplet.acl),
			"pool": oplet.strawberrySpeclet.Pool,
		},
		nil)

	if err != nil {
		oplet.l.Error("error updating operation parameters", log.Error(err))
		oplet.setError(err)
		return err
	}

	oplet.l.Info("operation parameters updated")

	oplet.pendingUpdateOpParameters = false
	return nil
}

func (oplet *Oplet) flushPersistentState(ctx context.Context) error {
	oplet.l.Info("flushing new operation's state",
		log.UInt64("flushed_state_revision", uint64(oplet.flushedStateRevision)))

	annotation := oplet.CypAnnotation()

	err := oplet.systemClient.MultisetAttributes(
		ctx,
		oplet.cypressNode.Attrs(),
		map[string]interface{}{
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
			// If the state revision is outdated, we can not flush the state because we can accidently
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
	// so reset backoff and try to proccess op again.
	if oplet.needFlushPersistentState() {
		oplet.resetBackoff()
	}

	// Skip further processing if the oplet does not belong to the controller or is broken.
	if oplet.DoesNotExist() || oplet.Broken() || oplet.Inappropriate() || oplet.needsBackoff() {
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

	// We always try to flush the state, even if the error has occured,
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
