package strawberry

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/xerrors"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

var aliasAlreadyUsedRE, _ = regexp.Compile("alias is already used by an operation")

type Oplet struct {
	// Fields below are public since they are accessed via reflection in fancy functions.
	Alias            string
	YTOpID           yt.OperationID
	IncarnationIndex int

	l log.Logger
	c Controller

	speclet yson.RawValue

	pendingRestart bool
	pendingFlush   bool

	acl       []yt.ACE
	pool      *string
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
	config     *Config

	aliasToOp map[string]*Oplet

	hostname string
	Proxy    string

	// nodeCh receives events of form "particular node in root has changed revision"
	nodeCh <-chan []ypath.Path

	// pendingAliases contains all aliases that should be updated from Cypress attributes.
	pendingAliases map[string]struct{}

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
		Proxy:            proxy,
		backgroundStopCh: make(chan struct{}),
	}
}

func (a *Agent) restartOp(oplet *Oplet) {
	oplet.l.Info("restarting operation")
	spec, description, annotations, err := oplet.c.Prepare(a.ctx, oplet.Alias, oplet.IncarnationIndex+1, oplet.speclet)

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
	if oplet.pool != nil {
		spec["pool"] = oplet.pool
	}
	if oplet.acl != nil {
		spec["acl"] = oplet.acl
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
			return
		}
		opID, err = a.ytc.StartOperation(a.ctx, yt.OperationVanilla, spec, nil)
	}

	if err != nil {
		oplet.setError(err)
		return
	}

	oplet.l.Info("operation started", log.String("operation_id", opID.String()),
		log.Int("incarnation_index", oplet.IncarnationIndex))

	oplet.setYTOpID(opID)

	oplet.pendingRestart = false

	oplet.IncarnationIndex++
	oplet.pendingFlush = true
}

func (a *Agent) flushOp(oplet *Oplet) {
	oplet.l.Debug("flushing operation")

	annotation := cypAnnotation(a, oplet)

	err := a.ytc.MultisetAttributes(
		a.ctx,
		a.config.Root.Child(oplet.Alias).Attrs(),
		map[string]interface{}{
			"strawberry_error":             oplet.err,
			"strawberry_operation_id":      oplet.YTOpID,
			"strawberry_incarnation_index": oplet.IncarnationIndex,
			"strawberry_controller": map[string]string{
				"address": a.hostname,
				// TODO(max42): build Revision, etc.
			},
			"strawberry_family": oplet.c.Family(),
			"annotation":        annotation,
		},
		nil)
	if err != nil {
		oplet.l.Error("error flushing operation", log.Error(err))
		return
	}
	oplet.pendingFlush = false
}

func (a *Agent) abortDangling() {
	family := a.controller.Family()
	l := log.With(a.l, log.String("family", family))
	l.Info("collecting running operations")

	optFilter := "\"strawberry_family\"=\"" + family + "\""
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

		alias, ok := op.BriefSpec["alias"]
		if !ok {
			l.Debug("operation misses alias (how is that possible?), aborting it", log.String("operation_id", op.ID.String()))
			needAbort = true
		} else if _, ok := a.aliasToOp[alias.(string)[1:]]; !ok {
			l.Debug("operation alias unknown, aborting it", log.String("operation_id", op.ID.String()))
			needAbort = true
		}

		if needAbort {
			err := a.ytc.AbortOperation(a.ctx, op.ID, nil)
			if err != nil {
				l.Error("error aborting operation", log.String("operation_id", op.ID.String()), log.Error(err))
			}
		}
	}
}

func (a *Agent) pass() {
	a.l.Info("starting pass")

	// Update pending operations from attributes.

	for alias := range a.pendingAliases {
		oplet := a.aliasToOp[alias]
		err := a.updateFromAttrs(oplet, alias)
		if err != nil {
			a.l.Error("error updating operation from attributes", log.String("alias", alias), log.Error(err))
		} else {
			delete(a.pendingAliases, alias)
		}
	}

	// Restart operations which were scheduled for restart.

	a.l.Info("restarting pending operations")
	for _, op := range a.aliasToOp {
		if op.pendingRestart {
			a.restartOp(op)
		}
	}

	// Check liveness of registered operations.

	a.l.Info("checking operations' liveness")
	for _, op := range a.aliasToOp {
		opID := op.YTOpID

		if opID == yt.OperationID(guid.FromHalves(0, 0)) {
			a.l.Debug("operation does not have operation id, skipping it")
			continue
		}

		a.l.Debug("getting operation info", log.String("operation_id", opID.String()))

		ytOp, err := a.ytc.GetOperation(a.ctx, opID, nil)
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

	// TODO(max42): if agent is stopped by this moment, cancellation errors may
	// be flushed into operations or even lead to unexpected abortions of operations.

	// Flush operation states.

	a.l.Info("flushing operations")
	for _, op := range a.aliasToOp {
		if op.pendingFlush {
			a.flushOp(op)
		}
	}

	// Abort dangling operations. This results in fetching running operations
	// and filtering those which are not listed in our idToOp.

	a.l.Info("aborting dangling operations")

	a.abortDangling()

	// Sanity check.
	for alias, op := range a.aliasToOp {
		if op.Alias != alias {
			panic(fmt.Errorf("invariant violation: alias %v points to oplet for operation with alias %v", alias, op.Alias))
		}
	}
}

func tokenize(path ypath.Path) []string {
	parts := strings.Split(string(path), "/")
	var j int
	for i := 0; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[j] = parts[i]
			j++
		}
	}
	return parts[:j]
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
				if len(tokens) != 1 {
					continue
				}
				a.pendingAliases[tokens[0]] = struct{}{}
			}
		case <-ticker.C:
			a.pass()
		}
	}
	a.l.Info("background activity stopped")
	a.backgroundStopCh <- struct{}{}
}

type CypressState struct {
	ACL              []yt.ACE
	Pool             *string
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

func (a *Agent) newOplet(alias string, controller Controller, cState CypressState) Oplet {
	opID := cState.OperationID

	opState := Oplet{
		Alias:            alias,
		acl:              cState.ACL,
		pool:             cState.Pool,
		IncarnationIndex: cState.IncarnationIndex,
		YTOpID:           opID,
		speclet:          cState.Speclet,
		pendingFlush:     true,
		l:                log.With(a.l, log.String("alias", alias)),
		c:                controller,
	}

	if opID == yt.OperationID(guid.FromParts(0, 0, 0, 0)) {
		opState.setPendingRestart("missing yt operation id")
		return opState
	}

	return opState
}

func (a *Agent) registerOplet(oplet *Oplet) {
	if _, ok := a.aliasToOp[oplet.Alias]; ok {
		panic(fmt.Errorf("invariant violation: alias %v is already registered", oplet.Alias))
	}
	a.aliasToOp[oplet.Alias] = oplet
	oplet.l.Info("operation registered")
}

func (a *Agent) unregisterOplet(oplet *Oplet) {
	if actual := a.aliasToOp[oplet.Alias]; actual != oplet {
		panic(fmt.Errorf("invariant violation: alias %v expected to match oplet %v, actual %v",
			oplet.Alias, oplet, actual))
	}
	delete(a.aliasToOp, oplet.Alias)
	oplet.l.Info("operation unregistered")
}

func (a *Agent) updateFromAttrs(oplet *Oplet, alias string) error {
	l := log.With(a.l, log.String("alias", alias))
	l.Info("updating operations from node attributes")

	// Collect full attributes of the node.

	var node struct {
		ACL              []yt.ACE       `yson:"strawberry_acl"`
		OperationID      yt.OperationID `yson:"strawberry_operation_id"`
		IncarnationIndex int            `yson:"strawberry_incarnation_index"`
		Speclet          yson.RawValue  `yson:"strawberry_speclet"`
		Family           string         `yson:"strawberry_family"`
		Pool             *string        `yson:"strawberry_pool"`
	}

	// Keep in sync with structure above.
	attributes := []string{
		"strawberry_operation_id", "strawberry_acl", "strawberry_incarnation_index",
		"strawberry_speclet", "strawberry_family", "strawberry_pool",
	}

	err := a.ytc.GetNode(a.ctx, a.config.Root.Child(alias).Attrs(), &node, &yt.GetNodeOptions{Attributes: attributes})

	if yterrors.ContainsResolveError(err) {
		// Node has gone. Remove this oplet.
		if oplet != nil {
			a.unregisterOplet(oplet)
		}
		return nil
	} else if err != nil {
		a.l.Error("error getting attributes", log.Error(err))
		return err
	}

	// Validate operation node
	if node.Family != a.controller.Family() {
		l.Debug("skipping node from unknown family",
			log.String("family", node.Family))
		return nil
	}

	if node.Speclet == nil {
		l.Debug("skipping operation due to missing `strawberry_speclet` attribute")
		return nil
	}

	l.Debug("node attributes collected and validated")

	if oplet != nil {
		if !reflect.DeepEqual(oplet.acl, node.ACL) {
			oplet.acl = node.ACL
			oplet.setPendingRestart("ACL change")
		}
		if !reflect.DeepEqual(oplet.pool, node.Pool) {
			oplet.pool = node.Pool
			oplet.setPendingRestart("Pool change")
		}
		if !reflect.DeepEqual(oplet.speclet, node.Speclet) {
			oplet.speclet = node.Speclet
			oplet.setPendingRestart("Speclet change")
		}
	} else {
		// TODO(max42): current implementation does not restart operation in following case:
		// - ACL changes
		// - agent1 dies before noticing that
		// - agent2 starts and registers new oplet for the operation
		// Correct way to handle that is to compare ACL, pool and speclet from operation runtime information
		// with the intended values.
		newOplet := a.newOplet(alias, a.controller, CypressState{
			ACL:              node.ACL,
			OperationID:      node.OperationID,
			IncarnationIndex: node.IncarnationIndex,
			Speclet:          node.Speclet,
			Pool:             node.Pool,
		})
		a.registerOplet(&newOplet)
	}

	l.Info("operation updated from node attributes")

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
	a.pendingAliases = make(map[string]struct{})

	a.nodeCh = TrackChildren(a.ctx, a.config.Root, time.Millisecond*1000, a.ytc, a.l)

	var initialAliases []string
	err := a.ytc.ListNode(a.ctx, a.config.Root, &initialAliases, nil)
	if err != nil {
		panic(err)
	}

	for _, alias := range initialAliases {
		err := a.updateFromAttrs(nil, alias)
		if err != nil {
			panic(err)
		}
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
	a.pendingAliases = nil
	a.nodeCh = nil
	a.started = false
	a.l.Info("agent stopped")
}
