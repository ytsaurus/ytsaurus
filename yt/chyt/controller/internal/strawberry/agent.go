package strawberry

import (
	"context"
	"fmt"
	"os"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

type Oplet struct {
	// Fields below are public since they are accessed via reflection in fancy functions.
	Alias            string
	YTOpID           yt.OperationID
	IncarnationIndex int

	l log.Logger

	speclet yson.RawValue

	pendingRestart                bool
	pendingFlush                  bool
	pendingControllerNotification bool

	acl       yson.RawValue
	pool      string
	ytOpState yt.OperationState
	err       error
}

func (oplet *Oplet) setError(err error) {
	oplet.l.Debug("operation is in error state", log.Error(err))
	oplet.pendingFlush = true
	oplet.err = err
}

func (oplet *Oplet) setYTOpID(id yt.OperationID) {
	oplet.pendingFlush = true
	oplet.YTOpID = id
}

func (oplet *Oplet) setPendingRestart(reason string) {
	if oplet.pendingRestart {
		oplet.l.Info("pendingRestart is already set", log.String("reason", reason))
		return
	}

	oplet.pendingRestart = true
	oplet.l.Info("setting pendingRestart", log.String("reason", reason))
}

type Agent struct {
	ytc        yt.Client
	l          log.Logger
	controller Controller
	root       ypath.Path

	ops    []Oplet
	idToOp map[yt.OperationID]*Oplet

	hostname string
	Proxy    string
}

func NewAgent(proxy string, ytc yt.Client, l log.Logger, controller Controller, root ypath.Path) *Agent {
	hostname, err := os.Hostname()
	if err != nil {
		l.Fatal("error getting hostname", log.Error(err))
	}

	return &Agent{
		ytc:        ytc,
		l:          l,
		controller: controller,
		root:       root,
		hostname:   hostname,
		Proxy:      proxy,
	}
}

func (a *Agent) restartOp(oplet *Oplet) {
	oplet.l.Info("restarting operation")
	spec, description, annotations, err := a.controller.Prepare(oplet.Alias, oplet.IncarnationIndex+1, oplet.speclet)

	if err != nil {
		oplet.l.Info("error restarting operation", log.Error(err))
		oplet.setError(err)
		return
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
	spec["alias"] = "*" + oplet.Alias
	spec["pool"] = oplet.pool
	spec["acl"] = oplet.acl

	opID, err := a.ytc.StartOperation(context.TODO(), yt.OperationVanilla, spec, nil)

	if err != nil {
		oplet.setError(err)
		return
	}

	oplet.l.Info("operation started", log.String("operation_id", opID.String()),
		log.Int("incarnation_index", oplet.IncarnationIndex))

	// Delete obsolete operation id from idToOp map.
	delete(a.idToOp, oplet.YTOpID)

	oplet.setYTOpID(opID)
	a.idToOp[oplet.YTOpID] = oplet

	oplet.pendingRestart = false

	oplet.IncarnationIndex++
	oplet.pendingFlush = true
	oplet.pendingControllerNotification = true
}

func (a *Agent) flushOp(state *Oplet) {
	state.l.Debug("flushing operation")

	annotation := cypAnnotation(a, state)

	err := a.ytc.MultisetAttributes(
		context.TODO(),
		a.root.Child(state.Alias).Attrs(),
		map[string]interface{}{
			"strawberry_error":             state.err,
			"strawberry_operation_id":      state.YTOpID,
			"strawberry_acl":               state.acl,
			"strawberry_incarnation_index": state.IncarnationIndex,
			"strawberry_controller": map[string]string{
				"address": a.hostname,
				// TODO(max42): build revision, etc.
			},
			"strawberry_family": a.controller.Family(),
			"annotation":        annotation,
		},
		nil)
	if err != nil {
		state.l.Error("error flushing operation", log.Error(err))
		return
	}
	state.pendingFlush = false
}

func (a *Agent) pass() {
	a.l.Info("starting pass")

	// Restart operations which were scheduled for restart.

	a.l.Info("restarting pending operations")
	for i := range a.ops {
		op := &a.ops[i]
		if op.pendingRestart {
			a.restartOp(op)
		}
	}

	// Check liveness of registered operations.

	a.l.Info("checking operations' liveness")
	for i := range a.ops {
		op := &a.ops[i]
		opID := op.YTOpID
		a.l.Debug("getting operation info", log.String("operation_id", opID.String()))

		ytOp, err := a.ytc.GetOperation(context.TODO(), opID, nil)
		if err != nil {
			op.setError(err)
			op.setPendingRestart("error getting operation info")
		}

		a.l.Debug("operation found",
			log.String("operation_id", opID.String()),
			log.String("operation_state", string(ytOp.State)))

		op.ytOpState = ytOp.State

		if requiresRestart(ytOp.State) {
			op.setPendingRestart("operation state requires restart")
		}
	}

	// Flush operation states.

	a.l.Info("flushing operations")
	for i := range a.ops {
		op := &a.ops[i]
		if op.pendingFlush {
			a.flushOp(op)
		}
	}

	// Abort dangling operations. This requires fetching running operations
	// and filtering those which are not listed in our idToOp.

	a.l.Info("collecting running operations")

	optFilter := "\"strawberry_family\"=\"" + a.controller.Family() + "\""
	optState := yt.StateRunning
	optType := yt.OperationVanilla

	runningOps, err := yt.ListAllOperations(
		context.TODO(),
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
		a.l.Error("error collecting running operations", log.Error(err))
	}

	opIDStrs := make([]string, len(runningOps))
	for i, op := range runningOps {
		opIDStrs[i] = op.ID.String()
	}

	a.l.Debug("collected running operations", log.Strings("operation_ids", opIDStrs))

	a.l.Info("aborting dangling operations")
	for _, op := range runningOps {
		id := op.ID
		if _, ok := a.idToOp[id]; !ok {
			a.l.Debug("aborting operation", log.String("operation_id", id.String()))
			err := a.ytc.AbortOperation(context.TODO(), id, nil)
			if err != nil {
				a.l.Error("error aborting operation", log.String("operation_id", id.String()), log.Error(err))
			}
		}
	}

	// Sanity check.
	for id, op := range a.idToOp {
		if op.YTOpID != id {
			panic(fmt.Errorf("invariant violation: operation %v points to state for operation %v", id, op.YTOpID))
		}
	}
}

func (a *Agent) background(period time.Duration) {
	a.l.Info("starting background activity", log.Duration("period", period))
	for {
		a.pass()
		time.Sleep(period)
	}
}

type CypressState struct {
	ACL              yson.RawValue
	Pool             string
	OperationID      yt.OperationID
	IncarnationIndex int
	Speclet          yson.RawValue
}

func requiresRestart(state yt.OperationState) bool {
	return state == yt.StateAborted ||
		state == yt.StateAborting ||
		state == yt.StateCompleted ||
		state == yt.StateCompleting ||
		state == yt.StateFailed ||
		state == yt.StateFailing
}

func (a *Agent) newOperationState(alias string, cState CypressState) Oplet {
	opID := cState.OperationID

	opState := Oplet{
		Alias:                         alias,
		acl:                           cState.ACL,
		pool:                          cState.Pool,
		IncarnationIndex:              cState.IncarnationIndex,
		YTOpID:                        opID,
		speclet:                       cState.Speclet,
		pendingControllerNotification: true,
		l:                             log.With(a.l, log.String("alias", alias)),
	}

	if opID == yt.OperationID(guid.FromParts(0, 0, 0, 0)) {
		opState.setPendingRestart("missing yt operation id")
		return opState
	}

	return opState
}

func (a *Agent) initOperations(forceFlush bool) {
	var ops []struct {
		Alias            string         `yson:",value"`
		ACL              yson.RawValue  `yson:"strawberry_acl,attr"`
		OperationID      yt.OperationID `yson:"strawberry_operation_id,attr"`
		IncarnationIndex int            `yson:"strawberry_incarnation_index,attr"`
		Speclet          yson.RawValue  `yson:"strawberry_speclet,attr"`
		Family           string         `yson:"strawberry_family,attr"`
		Pool             string         `yson:"strawberry_pool,attr"`
	}

	// Keep in sync with structure above.
	attributes := []string{
		"strawberry_operation_id", "strawberry_acl", "strawberry_incarnation_index",
		"strawberry_speclet", "strawberry_family", "strawberry_pool",
	}

	err := a.ytc.ListNode(context.TODO(), ypath.Path(a.root), &ops, &yt.ListNodeOptions{Attributes: attributes})

	if err != nil {
		a.l.Fatal("error listing root node", log.Error(err))
		return
	}

	a.idToOp = make(map[yt.OperationID]*Oplet)

	for _, op := range ops {
		l := log.With(a.l, log.String("alias", op.Alias), log.String("operation_id", op.OperationID.String()))
		// Validate operation node.
		if op.Family != a.controller.Family() {
			l.Debug("skipping operation from different family",
				log.String("family", op.Family))
			continue
		}

		if op.Speclet == nil {
			l.Debug("skipping operation due to missing `strawberry_speclet` attribute")
			continue
		}

		if op.ACL == nil {
			l.Debug("skipping operation due to missing `strawberry_acl` attribute")
			continue
		}

		if op.Pool == "" {
			l.Debug("skipping operation due to missing `strawberry_pool` attribute")
			continue
		}

		l.Debug("collected operation")

		a.ops = append(a.ops, a.newOperationState(op.Alias, CypressState{
			ACL:              op.ACL,
			OperationID:      op.OperationID,
			IncarnationIndex: op.IncarnationIndex,
			Speclet:          op.Speclet,
			Pool:             op.Pool,
		}))
		a.idToOp[op.OperationID] = &a.ops[len(a.ops)-1]
	}

	if forceFlush {
		for i := range a.ops {
			op := &a.ops[i]
			op.pendingFlush = true
		}
	}
}

func (a *Agent) Start(forceFlush bool, period time.Duration) {
	a.initOperations(forceFlush)

	go a.background(period)
}
