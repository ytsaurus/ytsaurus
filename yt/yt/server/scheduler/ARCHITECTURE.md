## `yt/yt/server/scheduler` — YT Scheduler Daemon

Central coordinator of distributed compute resources in YTsaurus: routes node heartbeats to the fair-share strategy, manages operation lifecycle in collaboration with remote controller agents, and persists state to the YT master.

**Design overview.**

The scheduler sits at the intersection of three external systems:
- **Exec nodes** — send periodic heartbeats with resource availability; the scheduler responds with allocation start/preempt decisions.
- **Controller agents** — remote daemon processes that manage per-operation internal logic (job retry, task graphs). The scheduler tracks them, enqueues allocation events for them, and issues `ScheduleAllocation` requests to start work.
- **YT master** — persistent store for operation metadata and cluster configuration. The scheduler reads initial state on connect and flushes operation state changes back.

Internally the scheduler is decomposed into loosely coupled subsystems:

1. **Node shard layer** (`TNodeManager` + `TNodeShard`): heartbeat processing is partitioned across a fixed set of shards for horizontal scaling. A node is mapped to exactly one shard; each shard runs on its own invoker.
2. **Fair-share strategy** (`NStrategy::IStrategy`, see `strategy/`): owns the pool tree forest, runs periodic fair-share updates, and on each heartbeat produces allocation start/preempt decisions.
3. **Controller agent layer** (`TControllerAgentTracker` + `TControllerAgent`): tracks connected agents; uses asynchronous message-queue outboxes / inboxes to decouple the control thread from inter-process RPC latency.
4. **Operation layer** (`TOperation` + `TOperationControllerImpl`): represents a user-submitted operation end-to-end; bridges between the strategy (resource consumer) and the controller agent (execution engine).
5. **Master connector** (`TMasterConnector`): manages the connect/disconnect lifecycle with the YT master, runs periodic watchers, and buffers Cypress writes.

Everything strategy-related is in the `NYT::NScheduler::NStrategy` sub-namespace; types from there are referenced with the `NStrategy::` prefix throughout this directory.

**Components:**

**`bootstrap.h`** — Daemon bootstrap.
- `TBootstrap` — constructs and wires all subsystems (RPC server, HTTP server, YT client, `TScheduler`, `TControllerAgentTracker`). Entry point for the scheduler binary. Exposes `GetControlInvoker(EControlQueue)` — the control thread is an `IEnumIndexedFairShareActionQueue<EControlQueue>` so that different subsystems time-share the control thread fairly.
- `EControlQueue` itself is declared in `yt/yt/server/lib/scheduler/public.h` (shared with other server components).

**`scheduler.h`** — Main scheduler class.
- `TScheduler` — top-level `TRefCounted` wrapper (Pimpl, `TImpl` lives in `.cpp`). The impl implements `NStrategy::IStrategyHost`, `INodeManagerHost`, `IOperationsCleanerHost`, and `TEventLogHostBase`.
- Operation lifecycle: `StartOperation`, `AbortOperation`, `SuspendOperation`, `ResumeOperation`, `CompleteOperation`, `UpdateOperationParameters`, `PatchOperationSpec`, plus the `OnOperation…` callbacks from subsystems.
- Heartbeat routing: `ProcessNodeHeartbeat(context)` — dispatches to `TNodeManager`.
- Orchid (Cypress browser): `CreateOrchidService()` exposes live scheduler state under `//sys/scheduler/orchid`.
- Accessors: `GetStrategy()`, `GetNodeManager()`, `GetMasterConnector()`, `GetOperationsCleaner()`, `GetBackgroundInvoker()`, plus individual lookups like `FindOperation`, `FindOperationIdByAllocationId`, `GetAllocationBriefInfo`.

**`operation.h`** — Operation data model.
- `TOperation : NStrategy::IOperation` — represents one user operation throughout its lifecycle (initializing → materializing → pending → running → completing → finished). Holds parsed/raw spec, runtime parameters, controller attributes (`TControllerAttributes`), alerts (`TOperationAlert`), the `IOperationControllerPtr`, optional `TOperationRevivalDescriptor`, revived allocation list, operation events history, `SecureVault`, `TemporaryTokenNodeId`, `FinishTime`, etc.
- `TOperationEvent` — YSON-serializable `(Time, EOperationState, Attributes)` record of one state transition; appended to the operation's event log.
- `DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH` — macro that sets `ShouldFlush_ = true` on write, marking the operation for Cypress flush.
- `TPreprocessedSpec` + `ParseSpec` — parse user spec and merge in experiment and template overrides before the operation is registered.

**`node_manager.h`** — Node shard coordinator.
- `TNodeManager : INodeShardHost` — creates and manages the fixed-size `TNodeShard` array. Routes incoming heartbeats, allocation updates, and per-operation actions (register/unregister, abort allocations, suspend/resume) to the correct shard. Modulo-based routing via `GetNodeShardId(nodeId)`. Aggregates cross-shard statistics and maintains the address-to-id map.
- `INodeManagerHost` — callbacks the node manager needs from `TScheduler`: strategy access, archive version, formatted resource-usage strings.

**`node_shard.h`** — Per-shard node heartbeat processor.
- `TNodeShard` — processes heartbeats for its subset of exec nodes. Runs exclusively on its own cancelable invoker. Per-heartbeat work: update `TExecNode` descriptor and resource limits, consult the strategy's `INodeHeartbeatStrategyProxy` for allocation decisions, start/preempt/abort allocations, and route finished-allocation events back to the strategy.
- `INodeShardHost` — minimal interface (`GetNodeShardId`) implemented by `TNodeManager`.
- `TNodeShardGlobalSensors` — profiling counters shared across all shards; initialized on master connection so only the active scheduler emits metrics.

**`common/exec_node.h`** — Exec node data model.
- `TExecNode` — scheduler-side representation of one exec node. Owned by a node shard, thread affinity is that shard's invoker. Holds `ResourceLimits`, `ResourceUsage`, `DiskResources`, the running-allocation set and id→allocation map, pending aborts map, scheduling `Tags` (boolean formula), scheduling segment assignment, timestamps (heartbeat, logging), and node state flags.

**`common/allocation.h`** — Allocation data model.
- `TAllocation` — one scheduled allocation (corresponds to one job slot on a node). Holds `State` (`EAllocationState`: scheduled/running/finishing/finished), resource usage/limits/disk quota, `TAllocationAttributes`, preemption info (`PreemptionMode`, `PreemptionReason`, `PreemptedFor`, preempting-stage descriptor), and references to the owning operation and controller epoch. State transitions are driven by node shard heartbeat processing; a few scheduling-specific fields (`SchedulingIndex`, …) are flagged for future relocation into the strategy.

**`controller_agent_tracker.h`** — Controller agent lifecycle manager.
- `TControllerAgentTracker` — tracks all connected `TControllerAgent` instances; handles registration (`ProcessAgentHandshake`), heartbeats (`ProcessAgentHeartbeat`), and `ScheduleAllocation` heartbeats (`ProcessAgentScheduleAllocationHeartbeat`) on a dedicated cancelable heartbeat invoker. `CreateController`, `PickAgentForOperation`, `AssignOperationToAgent`, `UnregisterOperationFromAgent` — operation–agent lifecycle.
- `HandleAgentFailure` forces an unregister when the agent's lease lapses or an RPC fails.

**`controller_agent.h`** — Per-agent state.
- `TControllerAgent` — scheduler-side representation of one connected controller agent process. Holds:
  - `EControllerAgentState` — state machine: `Registering → WaitingForInitialHeartbeat → Registered → Unregistering → Unregistered`.
  - Three outboxes (scheduler → agent, `TMessageQueueOutbox`): `AllocationEventsOutbox`, `OperationEventsOutbox`, `ScheduleAllocationRequestsOutbox`.
  - Three inboxes (agent → scheduler, `TMessageQueueInbox`): `OperationEventsInbox`, `RunningAllocationStatisticsUpdatesInbox`, `ScheduleAllocationResponsesInbox`.
  - `Operations` set — which operations this agent currently manages.
  - Incarnation transaction, lease, memory statistics, cancelable invokers for control and heartbeat paths.
- The outbox/inbox pattern decouples node-shard-produced decisions from agent RPC latency: events are enqueued by one thread, drained at the next heartbeat.

**`operation_controller.h`** / **`operation_controller_impl.h`** — Controller-agent proxy for one operation.
- `IOperationController : NStrategy::ISchedulingOperationController` — interface through which the scheduler drives one operation's controller lifecycle. Agent management: `AssignAgent` / `RevokeAgent` / `FindAgent`. Async lifecycle steps: `Initialize`, `Prepare`, `Materialize`, `Revive`, `Commit`, `Terminate`, `Complete`, `Register`, `Unregister`, `UpdateRuntimeParameters`, `PatchSpec`. Local notifications: `Suspend`, `Resume`, `OnAllocationAborted`, `OnAllocationFinished`, plus the matching `OnInitializationFinished` / `OnPreparationFinished` / `OnMaterializationFinished` / `OnRevivalFinished` / `OnCommitFinished` completions driven by the tracker. `SetControllerRuntimeData` updates the shared live data on each agent heartbeat; `GetFullHeartbeatProcessed()` waits until the next heartbeat round-trip completes. `ScheduleAllocation` and `GetNeededResources` are inherited from the strategy-side base.
- `TOperationControllerImpl` — concrete implementation; posts RPCs to the assigned `TControllerAgent` or enqueues entries in its message-queue outboxes, and returns futures resolved from responses.
- `TControllerRuntimeData` (refcounted) — live mutable state shared with the strategy: `NeededResources` (composite), `GroupedNeededResources`. Updated on each agent heartbeat.
- Result structs: `TOperationControllerInitializeResult`, `TOperationControllerPrepareResult`, `TOperationControllerMaterializeResult`, `TOperationControllerReviveResult`, `TOperationControllerCommitResult`, `TOperationControllerUnregisterResult` — typed results returned from controller lifecycle RPCs. Each has a `FromProto(...)` conversion from the controller agent protobufs.

**`master_connector.h`** — YT master connection manager.
- `TMasterConnector` — owns the scheduler ↔ master lifecycle. State machine: `Disconnected → Connecting → Connected → Disconnecting`. On connect, acquires a lock transaction, fetches operation state, and produces a `TMasterHandshakeResult` (with revival descriptors) consumed by other subsystems. On disconnect, cancels in-flight work and cleans up.
- Flushes `TOperation` state changes (alerts, events, mutable attributes) into Cypress write transactions: `CreateOperationNode`, `UpdateInitializedOperationNode`, `FlushOperationNode`, `FetchOperationRevivalDescriptors`, `GetOperationNodeProgressAttributes`.
- Strategy persistence: `InvokeStoringStrategyState(TPersistentStrategyStatePtr)` round-trips the strategy's persistent state through Cypress.
- Token issuing: `IssueTemporaryOperationToken` creates a temporary-token Cypress node whose id is cached on the operation.
- Watcher system: `AddCommonWatcher(requester, handler, alert?)` for batched periodic Cypress reads piggy-backed on the common tick, and `SetCustomWatcher(type, requester, handler, period, alert?, lockOptions?)` for dedicated periods or lock-guarded watchers. `EWatcherType` enumerates custom watchers (`NodeAttributes`, `PoolTrees`).
- Signals: `MasterConnecting`, `MasterHandshake(result)`, `MasterConnected`, `MasterDisconnected`.

**`operations_cleaner.h`** — Finished-operation archiver.
- `IOperationsCleanerHost` — narrow host interface: alert setter, `GetBackgroundInvoker`, `GetOperationsCleanerInvoker`.
- `TOperationsCleaner` — background component that archives finished operations to the operations archive table and removes them from the in-memory map after a configurable retention period. Public API: `SubmitForArchivation(request)` / `SubmitForArchivation(ids)`, `SubmitForRemoval(requests)`, `EnqueueOperationAlertEvent`, `BuildOrchid`, `InitializeRequestFromOperation`. Signal `OperationsRemovedFromCypress` fires after each successful batch removal.
- `TArchiveOperationRequest` / `TRemoveOperationRequest` — YSON-serializable descriptors. `TArchiveOperationRequest::GetAttributeKeys()` / `GetProgressAttributeKeys()` return the Cypress attribute lists the archivation pass needs.
- `TOperationAlertEvent` (from `operation_alert_event.h`) — alert history record enqueued via `EnqueueOperationAlertEvent`.

**`allocation_tracker_service.h`** / **`scheduler_service.h`** / **`controller_agent_tracker_service.h`** — RPC service factories.
- `CreateAllocationTrackerService(bootstrap)` — RPC service node shards invoke through for allocation status updates. No response keeper (the service is not retry-deduped).
- `CreateOperationService(bootstrap, responseKeeper)` — client-facing operation management (start/abort/suspend/…). Response keeper deduplicates client retries.
- `CreateControllerAgentTrackerService(bootstrap, responseKeeper)` — controller-agent-facing handshake and heartbeats.

**`helpers.h`** — Utilities.
- `BuildMinimalOperationAttributes` / `BuildFullOperationAttributes` / `BuildMutableOperationAttributes` — YSON serialization helpers for Orchid and Cypress emission.
- `BuildSupportedFeatures` — enumerates scheduler-supported features for the handshake response.
- `TListOperationsResult` / `TAllocationDescription` — lightweight descriptors for Orchid views.

**Notes:**
- **Thread model.** The control thread is a `EControlQueue`-indexed fair-share action queue; different subsystems (user requests, master I/O, fair-share updates, GPU assignment, …) share it without starving each other. Node shards run on their own invokers. Controller-agent heartbeats run on a dedicated cancelable invoker owned by `TControllerAgentTracker`. Cross-thread communication uses futures or the explicit message-queue outbox/inbox pairs.
- **Revival.** On restart, the master connector fetches live operations and passes them through `TMasterHandshakeResult`. Each operation is revived by calling `controller->Revive()`, which returns `TOperationControllerReviveResult` with running allocations, grouped needed resources, banned tree ids, and the "revived from snapshot" bit. Allocations flow back through `TNodeManager::FinishOperationRevival` into the node shards.
- **Response keepers** deduplicate retried RPCs at the service layer — important because both clients and controller agents retry on timeout. `CreateAllocationTrackerService` omits the keeper since allocation updates are idempotent at the protocol level.
- **`EControlQueue` buckets** — the scheduler relies on fair-share control-invoker scheduling to prevent any single subsystem (e.g., orchid worker, operation spec parsing, fair-share updates, master flush) from starving the others.
- `common/` is meant to hold types shared with the strategy side of the codebase; do not add scheduler-internal state there.

**See also:**
- `strategy/ARCHITECTURE.md` for the fair-share scheduling strategy embedded in the scheduler.
- `yt/yt/server/controller_agent/` for the controller agent daemon that the scheduler coordinates with.
- `yt/yt/server/node/exec_node/` for the exec node daemon that sends heartbeats.
- `yt/yt/server/lib/scheduler/` for shared protocol types (`EControlQueue`, alert enums, message queue templates, `TMasterHandshakeResult`, …) used by both the scheduler and its clients.
