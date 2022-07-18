package strawberry

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/xerrors"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

var aliasAlreadyUsedRE = regexp.MustCompile("alias is already used by an operation")
var noSuchOperationRE = regexp.MustCompile("[Nn]o such operation")
var prerequisiteCheckFailedRE = regexp.MustCompile("[Pp]rerequisite check failed")

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

	// pendingUpdateFromCypress is set when we know that the oplet cypress state has been changed
	// (e.g. from RevisionTracker) and we need to reload it from a cypress during the next pass.
	pendingUpdateFromCypress bool
	// pendingUpdateACLFromNode is set when we know that the oplet acl is changed and we need to
	// reload it from the access node during the next pass.
	pendingUpdateACLFromNode bool
	// pendingRestart is set when agent scheduled a restart of coresponding yt operation on the
	// next pass. It should be set via setPendingRestart only for proper log information.
	pendingRestart bool
	// pendingUpdateOpParameters is set when agent scheduled an operation parameters update on the
	// next pass. It should be set via setPendingUpdateOpParameters only for proper log information.
	pendingUpdateOpParameters bool

	acl []yt.ACE

	l log.Logger
	c Controller
}

// Public API of oplet for usage in controllers.

func (oplet Oplet) Alias() string {
	return oplet.alias
}

func (oplet Oplet) NextIncarnationIndex() int {
	return oplet.persistentState.IncarnationIndex + 1
}

func (oplet Oplet) Speclet() yson.RawValue {
	return oplet.specletYson
}

// Private API of oplet for inner usage only.

func (oplet Oplet) needFlushPersistentState() bool {
	return !reflect.DeepEqual(oplet.persistentState, oplet.flushedPersistentState) ||
		!reflect.DeepEqual(oplet.infoState, oplet.flushedInfoState)
}

func (oplet *Oplet) setError(err error) {
	oplet.l.Debug("operation is in error state", log.Error(err))
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

type Agent struct {
	ytc        yt.Client
	l          log.Logger
	controller Controller
	config     *Config

	aliasToOp map[string]*Oplet

	hostname string
	proxy    string

	// nodeCh receives events of form "particular node in root has changed revision"
	nodeCh <-chan []ypath.Path

	started   bool
	ctx       context.Context
	cancelCtx context.CancelFunc

	backgroundStopCh chan struct{}
}

func NewAgent(proxy string, ytc yt.Client, l log.Logger, controller Controller, config *Config) *Agent {
	hostname, err := os.Hostname()
	if err != nil {
		l.Fatal("error getting hostname", log.Error(err))
	}

	return &Agent{
		ytc:              ytc,
		l:                l,
		controller:       controller,
		config:           config,
		hostname:         hostname,
		proxy:            proxy,
		backgroundStopCh: make(chan struct{}),
	}
}

func (a Agent) opletNode(oplet *Oplet) ypath.Path {
	return a.config.Root.Child(oplet.alias)
}

func (a *Agent) restartOp(oplet *Oplet) error {
	oplet.l.Info("restarting operation", log.Int("next_incarnation_index", oplet.NextIncarnationIndex()))
	spec, description, annotations, err := oplet.c.Prepare(a.ctx, *oplet)

	if err != nil {
		oplet.setError(err)
		return err
	}

	// Extend our own annotations and description by controller's annotation and description.
	strawberryAnnotations := opAnnotations(a, oplet)
	for k, v := range strawberryAnnotations {
		annotations[k] = v
	}
	strawberryDescription := opDescription(a, oplet)
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

	opID, err := a.ytc.StartOperation(a.ctx, yt.OperationVanilla, spec, nil)

	if yterrors.ContainsMessageRE(err, aliasAlreadyUsedRE) {
		oplet.l.Debug("alias is already used, aborting previous operation")
		// Try to abort already existing operation with that alias.
		var ytErr *yterrors.Error
		if ok := xerrors.As(err, &ytErr); !ok {
			panic(fmt.Errorf("cannot convert error to YT error: %v", err))
		}
		// TODO(max42): there must be a way better to do this...
		ytErr = ytErr.InnerErrors[0].InnerErrors[0]
		oldOpID, parseErr := guid.ParseString(ytErr.Attributes["operation_id"].(string))
		if parseErr != nil {
			panic(fmt.Errorf("malformed YT operation ID in error attributes: %v", parseErr))
		}
		oplet.l.Debug("aborting operation", log.String("operation_id", oldOpID.String()))
		abortErr := a.ytc.AbortOperation(a.ctx, yt.OperationID(oldOpID), &yt.AbortOperationOptions{})
		if abortErr != nil {
			oplet.setError(abortErr)
			return abortErr
		}
		opID, err = a.ytc.StartOperation(a.ctx, yt.OperationVanilla, spec, nil)
	}

	if err != nil {
		oplet.setError(err)
		return err
	} else {
		oplet.clearError()
	}

	oplet.persistentState.YTOpID = opID
	oplet.persistentState.OperationSpecletRevision = oplet.persistentState.SpecletRevision
	oplet.persistentState.IncarnationIndex++
	oplet.persistentState.SpecletChangeRequiresRestart = false

	oplet.l.Info("operation started", log.String("operation_id", opID.String()),
		log.Int("incarnation_index", oplet.persistentState.IncarnationIndex))

	oplet.pendingRestart = false
	oplet.pendingUpdateOpParameters = false

	return nil
}

func (a *Agent) updateOpParameters(oplet *Oplet) {
	oplet.l.Info("updating operation parameters")

	err := a.ytc.UpdateOperationParameters(
		a.ctx,
		oplet.persistentState.YTOpID,
		map[string]interface{}{
			"acl":  toOperationACL(oplet.acl),
			"pool": oplet.strawberrySpeclet.Pool,
		},
		nil)

	if err != nil {
		oplet.l.Error("error updating operation parameters", log.Error(err))
		oplet.setError(err)
		return
	}

	oplet.l.Info("operation parameters updated")

	oplet.pendingUpdateOpParameters = false
}

func (a *Agent) flushPersistentState(oplet *Oplet) {
	oplet.l.Info("flushing new operation's state",
		log.UInt64("flushed_state_revision", uint64(oplet.flushedStateRevision)))

	annotation := cypAnnotation(a, oplet)

	err := a.ytc.MultisetAttributes(
		a.ctx,
		a.opletNode(oplet).Attrs(),
		map[string]interface{}{
			"strawberry_persistent_state": oplet.persistentState,
			"strawberry_info_state":       oplet.infoState,
			"annotation":                  annotation,
		},
		&yt.MultisetAttributesOptions{
			PrerequisiteOptions: &yt.PrerequisiteOptions{
				Revisions: []yt.PrerequisiteRevision{
					{
						Path:     a.opletNode(oplet),
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
			oplet.pendingUpdateFromCypress = true
		} else {
			oplet.l.Error("error flushing operation", log.Error(err))
		}
		return
	}

	oplet.l.Info("operation's persistent state flushed")

	oplet.flushedPersistentState = oplet.persistentState
	oplet.flushedInfoState = oplet.infoState

	// flushedStateRevision is outdated after flushing.
	// We can not do set and get_revision atomically now, so just reload the whole state from cypress.
	oplet.pendingUpdateFromCypress = true
}

func (a *Agent) abortDangling() {
	family := a.controller.Family()
	l := log.With(a.l, log.String("family", family))
	l.Info("collecting running operations")

	optFilter := `"strawberry_operation_namespace"="` + a.OperationNamespace() + `"`
	optState := yt.StateRunning
	optType := yt.OperationVanilla

	runningOps, err := yt.ListAllOperations(
		a.ctx,
		a.ytc,
		&yt.ListOperationsOptions{
			Filter: &optFilter,
			State:  &optState,
			Type:   &optType,
			MasterReadOptions: &yt.MasterReadOptions{
				ReadFrom: yt.ReadFromFollower,
			},
		})

	if err != nil {
		l.Error("error collecting running operations", log.Error(err))
	}

	opIDStrs := make([]string, len(runningOps))
	for i, op := range runningOps {
		opIDStrs[i] = op.ID.String()
	}

	l.Debug("collected running operations", log.Strings("operation_ids", opIDStrs))

	l.Info("aborting dangling operations")
	for _, op := range runningOps {
		needAbort := false

		if op.BriefSpec == nil {
			// This may happen on early stages of operation lifetime.
			continue
		}

		opAlias, ok := op.BriefSpec["alias"]
		if !ok {
			l.Debug("operation misses alias (how is that possible?), aborting it",
				log.String("operation_id", op.ID.String()))
			needAbort = true
		} else {
			alias := opAlias.(string)[1:]
			oplet, ok := a.aliasToOp[alias]
			if !ok {
				l.Debug("operation alias unknown, aborting it",
					log.String("alias", alias), log.String("operation_id", op.ID.String()))
				needAbort = true
			} else if !oplet.pendingUpdateFromCypress && !oplet.strawberrySpeclet.ActiveOrDefault() {
				oplet.l.Debug("strawberry operation is in inactive state, aborting its yt operation",
					log.String("alias", alias), log.String("operation_id", op.ID.String()))
				needAbort = true
			} else if !oplet.pendingUpdateFromCypress && oplet.persistentState.YTOpID != op.ID {
				oplet.l.Debug("yt operation has unexpected id, aborting it",
					log.String("alias", alias), log.String("operation_id", op.ID.String()),
					log.String("expected_id", oplet.persistentState.YTOpID.String()))
				needAbort = true
			}
		}

		if needAbort {
			err := a.ytc.AbortOperation(a.ctx, op.ID, nil)
			if err != nil {
				l.Error("error aborting operation",
					log.String("operation_id", op.ID.String()),
					log.Error(err))
			}
		}
	}
}

func (a *Agent) pass() {
	startedAt := time.Now()

	a.l.Info("starting pass", log.Int("oplet_count", len(a.aliasToOp)))

	broken := make(map[string]bool)

	// Update pending operations from attributes.
	for alias, oplet := range a.aliasToOp {
		if oplet.pendingUpdateFromCypress {
			err := a.updateFromCypress(oplet)
			if err != nil {
				a.l.Error("error updating operation from attributes", log.String("alias", alias), log.Error(err))
				broken[alias] = true
			}
		}
	}

	// Update pending operations from access node.
	for alias, oplet := range a.aliasToOp {
		if oplet.pendingUpdateACLFromNode {
			err := a.updateACLFromNode(oplet)
			if err != nil {
				a.l.Error("error updating acl from node", log.String("alias", alias), log.Error(err))
				broken[alias] = true
			}
		}
	}

	// Restart operations which were scheduled for restart.
	for alias, oplet := range a.aliasToOp {
		if !broken[alias] && oplet.pendingRestart && oplet.strawberrySpeclet.ActiveOrDefault() {
			err := a.restartOp(oplet)
			if err != nil {
				a.l.Error("error restarting op", log.String("alias", alias), log.Error(err))
				broken[alias] = true
			}
		}
	}

	// Update op parameters.
	for alias, oplet := range a.aliasToOp {
		if !broken[alias] && oplet.pendingUpdateOpParameters && oplet.strawberrySpeclet.ActiveOrDefault() {
			a.updateOpParameters(oplet)
		}
	}

	a.l.Info("checking operations' liveness")
	for alias, oplet := range a.aliasToOp {
		if broken[alias] {
			continue
		}

		opID := oplet.persistentState.YTOpID

		if opID == yt.OperationID(guid.FromHalves(0, 0)) {
			oplet.setPendingRestart("strawberry operation does not have operation id")
			continue
		}

		oplet.l.Debug("getting operation info", log.String("operation_id", opID.String()))

		ytOp, err := a.ytc.GetOperation(a.ctx, opID, nil)
		if err != nil {
			if yterrors.ContainsMessageRE(err, noSuchOperationRE) {
				oplet.setPendingRestart("operation with current operation id does not exist")
			} else {
				oplet.l.Error("error getting operation info", log.Error(err))
				oplet.setError(err)
			}
			continue
		}

		oplet.l.Debug("operation found",
			log.String("operation_id", opID.String()),
			log.String("operation_state", string(ytOp.State)))

		if oplet.infoState.YTOpState != ytOp.State {
			oplet.infoState.YTOpState = ytOp.State
		}

		if requiresRestart(ytOp.State) {
			oplet.setPendingRestart("operation state requires restart")
		}
	}

	// TODO(max42): if agent is stopped by this moment, cancellation errors may
	// be flushed into operations or even lead to unexpected abortions of operations.

	// Flush operation states.
	a.l.Info("flushing operations")
	for _, oplet := range a.aliasToOp {
		if oplet.needFlushPersistentState() {
			a.flushPersistentState(oplet)
		}
	}

	// Abort dangling operations. This results in fetching running operations
	// and filtering those which are not listed in our idToOp.

	a.l.Info("aborting dangling operations")

	a.abortDangling()

	// Sanity check.
	for alias, oplet := range a.aliasToOp {
		if oplet.alias != alias {
			panic(fmt.Errorf("invariant violation: alias %v points to oplet for operation with alias %v", alias, oplet.alias))
		}
	}

	a.l.Info("pass completed", log.Duration("elapsed_time", time.Since(startedAt)))
}

func (a *Agent) background(period time.Duration) {
	a.l.Info("starting background activity", log.Duration("period", period))
	ticker := time.NewTicker(period)
loop:
	for {
		select {
		case <-a.ctx.Done():
			break loop
		case paths := <-a.nodeCh:
			for _, path := range paths {
				tokens := tokenize(path)
				if len(tokens) >= 1 {
					alias := tokens[0]
					subnodes := tokens[1:]
					oplet, ok := a.aliasToOp[alias]
					switch {
					case reflect.DeepEqual(subnodes, []string{}) || reflect.DeepEqual(subnodes, []string{"speclet"}):
						if ok {
							oplet.pendingUpdateFromCypress = true
						} else {
							a.registerNewOplet(alias)
						}
					case reflect.DeepEqual(subnodes, []string{"access"}):
						if ok {
							oplet.pendingUpdateACLFromNode = true
						}
					}
				}
			}
		case <-ticker.C:
			a.pass()
		}
	}
	a.l.Info("background activity stopped")
	a.backgroundStopCh <- struct{}{}
}

func (a *Agent) registerNewOplet(alias string) *Oplet {
	oplet := &Oplet{
		alias:                    alias,
		pendingUpdateFromCypress: true,
		pendingUpdateACLFromNode: true,
		l:                        log.With(a.l, log.String("alias", alias)),
		c:                        a.controller,
	}
	oplet.infoState.Controller.Address = a.hostname
	// Set same to the flushedInfoState to avoid flushing it after controller change.
	oplet.flushedInfoState = oplet.infoState

	if _, ok := a.aliasToOp[oplet.alias]; ok {
		panic(fmt.Errorf("invariant violation: alias %v is already registered", oplet.alias))
	}
	a.aliasToOp[oplet.alias] = oplet

	oplet.l.Info("operation registered")
	return oplet
}

func (a *Agent) unregisterOplet(oplet *Oplet) {
	if actual := a.aliasToOp[oplet.alias]; actual != oplet {
		panic(fmt.Errorf("invariant violation: alias %v expected to match oplet %v, actual %v",
			oplet.alias, oplet, actual))
	}
	delete(a.aliasToOp, oplet.alias)
	oplet.l.Info("operation unregistered")
}

func (a *Agent) getACLFromNode(alias string) (acl []yt.ACE, err error) {
	err = a.ytc.GetNode(a.ctx, a.config.Root.Child(alias).Child("access").Attr("acl"), &acl, nil)
	return
}

func (a *Agent) updateFromCypress(oplet *Oplet) error {
	oplet.l.Info("updating strawberry operations state from cypress",
		log.UInt64("state_revision", uint64(oplet.flushedStateRevision)),
		log.UInt64("speclet_revision", uint64(oplet.persistentState.SpecletRevision)))

	initialUpdate := oplet.flushedStateRevision == 0

	// Collect full attributes of the node.

	var node struct {
		PersistentState PersistentState `yson:"strawberry_persistent_state,attr"`
		Revision        yt.Revision     `yson:"revision,attr"`
		Speclet         struct {
			Value    yson.RawValue `yson:"value,attr"`
			Revision yt.Revision   `yson:"revision,attr"`
		} `yson:"speclet"`
	}

	// Keep in sync with structure above.
	attributes := []string{
		"strawberry_persistent_state", "revision", "value",
	}

	err := a.ytc.GetNode(a.ctx, a.opletNode(oplet), &node, &yt.GetNodeOptions{Attributes: attributes})

	if yterrors.ContainsResolveError(err) {
		// Node has gone. Remove this oplet.
		a.unregisterOplet(oplet)
		return nil
	} else if err != nil {
		a.l.Error("error getting operation state from cypress", log.Error(err))
		return err
	}

	oplet.pendingUpdateFromCypress = false

	// Validate operation node

	if node.Speclet.Value == nil {
		oplet.l.Debug("skipping oplet: `speclet` node is empty or missing")

		a.unregisterOplet(oplet)
		return err
	}

	var strawberrySpeclet Speclet
	err = yson.Unmarshal(node.Speclet.Value, &strawberrySpeclet)
	if err != nil {
		oplet.l.Error("skipping oplet: error parsing `speclet` node",
			log.Error(err),
			log.String("speclet_yson", string(node.Speclet.Value)),
			log.UInt64("speclet_revision", uint64(node.Speclet.Revision)))

		a.unregisterOplet(oplet)
		return err
	}

	if strawberrySpeclet.Family == nil || *strawberrySpeclet.Family != a.controller.Family() {
		oplet.l.Debug("skipping oplet from unknown family",
			log.Any("family", strawberrySpeclet.Family))

		a.unregisterOplet(oplet)
		return nil
	}

	if strawberrySpeclet.StageOrDefault() != a.config.Stage {
		oplet.l.Debug("skipping oplet from another stage",
			log.String("stage", strawberrySpeclet.StageOrDefault()))

		// Strawberry stage has changed and we do not need to maintain it any more.
		a.unregisterOplet(oplet)
		return nil
	}

	oplet.l.Debug("state collected and validated")

	// Handle persistent state change.
	if !initialUpdate && !reflect.DeepEqual(node.PersistentState, oplet.flushedPersistentState) {
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

func (a *Agent) updateACLFromNode(oplet *Oplet) error {
	oplet.l.Info("updating acl from access node")

	newACL, err := a.getACLFromNode(oplet.alias)
	if err != nil {
		oplet.l.Error("error geting acl from access node")
		return err
	}

	if !reflect.DeepEqual(oplet.acl, newACL) {
		oplet.l.Info("strawberry operation acl changed", log.Any("old_acl", oplet.acl), log.Any("new_acl", newACL))

		if !reflect.DeepEqual(toOperationACL(oplet.acl), toOperationACL(newACL)) {
			oplet.setPendingUpdateOpParameters("ACL change")
		}
	}

	oplet.acl = newACL

	oplet.l.Info("acl updated from access node")

	oplet.pendingUpdateACLFromNode = false

	return nil
}

func (a *Agent) Start() {
	if a.started {
		return
	}

	a.l.Info("starting agent")
	a.started = true
	a.ctx, a.cancelCtx = context.WithCancel(context.Background())

	a.aliasToOp = make(map[string]*Oplet)

	a.nodeCh = TrackChildren(a.ctx, a.config.Root, time.Millisecond*1000, a.ytc, a.l)

	var initialAliases []string
	err := a.ytc.ListNode(a.ctx, a.config.Root, &initialAliases, nil)
	if err != nil {
		panic(err)
	}

	// TODO(dakovalkov): we can do initialization more optimal with get on the whole directory.
	for _, alias := range initialAliases {
		a.registerNewOplet(alias)
	}

	go a.background(time.Duration(a.config.PassPeriod))
	a.l.Info("agent started")
}

func (a *Agent) Stop() {
	if !a.started {
		return
	}
	a.l.Info("stopping agent")
	a.cancelCtx()
	<-a.backgroundStopCh

	a.ctx = nil
	a.aliasToOp = nil
	a.nodeCh = nil
	a.started = false
	a.l.Info("agent stopped")
}

// OperationNamespace generates a special value unique across controllers
// which allows to mark and effectively filter its operations.
func (a *Agent) OperationNamespace() string {
	return a.controller.Family() + ":" + a.config.Stage
}
