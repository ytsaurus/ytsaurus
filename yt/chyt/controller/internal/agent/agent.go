package agent

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/monitoring"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Agent struct {
	ytc        yt.Client
	l          log.Logger
	controller strawberry.Controller
	config     *Config

	aliasToOp map[string]*strawberry.Oplet

	hostname string
	proxy    string
	family   string
	root     ypath.Path

	// nodeCh receives events of form "particular node in root has changed revision"
	nodeCh <-chan []ypath.Path

	// runningOpsCh periodically receives all running vanilla operations
	// with operation namespace from controller.
	runningOpsCh <-chan []yt.OperationStatus

	started   bool
	ctx       context.Context
	cancelCtx context.CancelFunc

	backgroundStopCh chan struct{}
	healthState      *HealthState
}

func NewAgent(proxy string, ytc yt.Client, l log.Logger, controller strawberry.Controller, config *Config) *Agent {
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
		family:           controller.Family(),
		root:             controller.Root(),
		proxy:            proxy,
		backgroundStopCh: make(chan struct{}),
		healthState:      NewHealthState(),
	}
}

func (a *Agent) updateACLs() error {
	var result []struct {
		Alias string   `yson:",value"`
		ACL   []yt.ACE `yson:"principal_acl,attr"`
	}

	err := a.ytc.ListNode(a.ctx,
		strawberry.AccessControlNamespacesPath.Child(a.controller.Family()),
		&result,
		&yt.ListNodeOptions{Attributes: []string{"principal_acl"}})

	if err != nil {
		return err
	}

	aclUpdated := make(map[string]bool)

	for _, node := range result {
		aclUpdated[node.Alias] = true
		if oplet, ok := a.aliasToOp[node.Alias]; ok {
			oplet.SetACL(node.ACL)
		}
	}

	for alias, oplet := range a.aliasToOp {
		if !aclUpdated[alias] {
			a.l.Error("oplet is broken: missing acl node", log.String("alias", alias))
			a.unregisterOplet(oplet)
		}
	}

	return nil
}

func (a *Agent) abortDangling(runningOps []yt.OperationStatus) error {
	family := a.controller.Family()
	l := log.With(a.l, log.String("family", family))

	startedAt := time.Now()

	l.Info("aborting dangling operations")
	abortedOps := 0
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
			} else if opID, _ := oplet.OperationInfo(); oplet.UpToDateWithCypress() && opID != op.ID {
				a.l.Debug("yt operation has unexpected id, aborting it",
					log.String("alias", alias), log.String("operation_id", op.ID.String()),
					log.String("expected_id", opID.String()))
				needAbort = true
			}
		}

		if needAbort {
			abortedOps++
			err := a.ytc.AbortOperation(a.ctx, op.ID, nil)
			if err != nil {
				l.Error("error aborting operation",
					log.String("operation_id", op.ID.String()),
					log.Error(err))
			}
		}
	}

	l.Info("finished aborting dangling operations",
		log.Duration("elapsed_time", time.Since(startedAt)),
		log.Int("aborted_operations_count", abortedOps),
		log.Int("total_operations_count", len(runningOps)))

	return nil
}

func (a *Agent) processOplets() {
	startedAt := time.Now()

	a.l.Info("starting processing oplets")

	workerNumber := a.config.PassWorkerNumberOrDefault()
	var wg sync.WaitGroup
	wg.Add(workerNumber)

	opletsChan := make(chan *strawberry.Oplet, len(a.aliasToOp))

	for i := 0; i < workerNumber; i++ {
		go func() {
			defer wg.Done()
			for oplet := range opletsChan {
				_ = oplet.Pass(a.ctx)
			}
		}()
	}

	for _, oplet := range a.aliasToOp {
		opletsChan <- oplet
	}
	close(opletsChan)

	wg.Wait()
	a.l.Info("finished processing oplets", log.Duration("elapsed_time", time.Since(startedAt)))
}

func (a *Agent) pass() {
	startedAt := time.Now()

	a.l.Info("starting pass", log.Int("oplet_count", len(a.aliasToOp)))

	if err := a.updateControllerState(); err != nil {
		a.healthState.Store(yterrors.Err("failed to update controller's state", err))
		return
	}

	if err := a.updateACLs(); err != nil {
		a.healthState.Store(yterrors.Err("failed to update ACLs", err))
		return
	}

	a.processOplets()
	for _, oplet := range a.aliasToOp {
		if oplet.Broken() {
			a.l.Info("unregistering oplet: it is broken",
				log.String("alias", oplet.Alias()),
				log.String("reason", oplet.BrokenReason()))
			a.unregisterOplet(oplet)
		} else if oplet.Inappropriate() {
			a.l.Info("unregistering oplet: it is inappropriate", log.String("alias", oplet.Alias()))
			a.unregisterOplet(oplet)
		}
	}

	// Sanity check.
	for alias, oplet := range a.aliasToOp {
		if oplet.Alias() != alias {
			panic(fmt.Errorf("invariant violation: alias %v points to oplet for operation with alias %v", alias, oplet.Alias()))
		}
	}

	a.l.Info("pass completed", log.Duration("elapsed_time", time.Since(startedAt)))
	a.healthState.Store(nil)
}

func (a *Agent) updateControllerState() error {
	changed, err := a.controller.UpdateState()
	if err != nil {
		return err
	}
	if changed {
		for _, oplet := range a.aliasToOp {
			oplet.SetPendingRestart("controller's state changed")
		}
	}
	return nil
}

func (a *Agent) background() {
	passPeriod := time.Duration(a.config.PassPeriodOrDefault())
	a.l.Info("starting background activity", log.Duration("period", passPeriod))
	ticker := time.NewTicker(passPeriod)
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
							oplet.OnCypressNodeChanged()
						} else {
							a.registerNewOplet(alias)
						}
					}
				}
			}
		case runningOps := <-a.runningOpsCh:
			// Abort dangling operations. This results in filtering those
			// which are not listed in our aliasToOp.
			if err := a.abortDangling(runningOps); err != nil {
				a.healthState.Store(yterrors.Err("failed to abort dangling operations", err))
				return
			}
		case <-ticker.C:
			a.pass()
		}
	}
	a.l.Info("background activity stopped")
	a.backgroundStopCh <- struct{}{}
}

func (a *Agent) GetAgentInfo() strawberry.AgentInfo {
	clusterURLTemplateData := struct {
		Proxy string
	}{
		a.proxy,
	}

	return strawberry.AgentInfo{
		StrawberryRoot:        a.root,
		Hostname:              a.hostname,
		Stage:                 a.config.Stage,
		Proxy:                 a.proxy,
		Family:                a.family,
		OperationNamespace:    a.OperationNamespace(),
		RobotUsername:         a.config.RobotUsername,
		DefaultNetworkProject: a.config.DefaultNetworkProject,
		ClusterURL:            strawberry.ExecuteTemplate(a.config.ClusterURLTemplate, clusterURLTemplateData),
	}
}

func (a *Agent) getOpletOptions(alias string) strawberry.OpletOptions {
	return strawberry.OpletOptions{
		AgentInfo:    a.GetAgentInfo(),
		Alias:        alias,
		Controller:   a.controller,
		Logger:       a.l,
		UserClient:   a.ytc,
		SystemClient: a.ytc,
	}
}

func (a *Agent) registerOplet(oplet *strawberry.Oplet) {
	if _, ok := a.aliasToOp[oplet.Alias()]; ok {
		panic(fmt.Errorf("invariant violation: alias %v is already registered", oplet.Alias()))
	}
	a.aliasToOp[oplet.Alias()] = oplet

	a.l.Info("oplet registered", log.String("alias", oplet.Alias()))
}

func (a *Agent) registerNewOplet(alias string) {
	a.registerOplet(strawberry.NewOplet(a.getOpletOptions(alias)))
}

func (a *Agent) unregisterOplet(oplet *strawberry.Oplet) {
	if actual := a.aliasToOp[oplet.Alias()]; actual != oplet {
		panic(fmt.Errorf("invariant violation: alias %v expected to match oplet %v, actual %v",
			oplet.Alias(), oplet, actual))
	}
	delete(a.aliasToOp, oplet.Alias())
	a.l.Info("oplet unregistered", log.String("alias", oplet.Alias()))
}

func (a *Agent) Start() {
	if a.started {
		return
	}

	a.l.Info("starting agent")
	a.started = true
	a.ctx, a.cancelCtx = context.WithCancel(context.Background())

	a.aliasToOp = make(map[string]*strawberry.Oplet)

	a.nodeCh = TrackChildren(a.ctx, a.root, time.Millisecond*1000, a.ytc, a.l)

	a.runningOpsCh = CollectOperations(
		a.ctx,
		a.ytc,
		time.Duration(a.config.CollectOperationsPeriodOrDefault()),
		a.OperationNamespace(),
		a.l)

	var initialAliases []string
	err := a.ytc.ListNode(a.ctx, a.root, &initialAliases, nil)
	if err != nil {
		panic(err)
	}

	// TODO(dakovalkov): we can do initialization more optimal with get on the whole directory.
	for _, alias := range initialAliases {
		a.registerNewOplet(alias)
	}

	go a.background()
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
	a.runningOpsCh = nil
	a.started = false
	a.l.Info("agent stopped")
}

// OperationNamespace generates a special value unique across controllers
// which allows to mark and effectively filter its operations.
func (a *Agent) OperationNamespace() string {
	return a.controller.Family() + ":" + a.config.Stage
}

func (a *Agent) GetHealthStatus() monitoring.HealthStatus {
	return a.healthState.Load()
}
