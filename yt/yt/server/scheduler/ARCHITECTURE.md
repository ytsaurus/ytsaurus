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

**`bootstrap.h`** — `TBootstrap` constructs and wires all subsystems (RPC server, HTTP server, YT client, `TScheduler`, `TControllerAgentTracker`) and owns the control thread: an `IEnumIndexedFairShareActionQueue<EControlQueue>` whose buckets let different subsystems time-share the thread fairly. `EControlQueue` itself is declared in `yt/yt/server/lib/scheduler/public.h`.

**`scheduler.h`** — `TScheduler`, the top-level Pimpl wrapper. The impl implements `NStrategy::IStrategyHost`, `INodeManagerHost`, `IOperationsCleanerHost`, and `TEventLogHostBase`; it drives the operation lifecycle (start, abort, suspend, resume, complete, runtime-parameter and spec updates, plus the `OnOperation…` callbacks from subsystems), routes node heartbeats to `TNodeManager`, and exposes live state through an Orchid service under `//sys/scheduler/orchid`.

**`operation.h`** — `TOperation : NStrategy::IOperation` represents one user operation throughout its lifecycle (initializing → materializing → pending → running → completing → finished): parsed and raw spec, runtime parameters, controller attributes, alerts, the `IOperationControllerPtr`, revival descriptor and revived allocations, event history, and the properties that mark the operation for a Cypress flush when written. `TPreprocessedSpec` + `ParseSpec` merge experiment and template overrides into the user spec before registration.

**`node_manager.h`** — `TNodeManager : INodeShardHost` creates the fixed-size `TNodeShard` array and routes heartbeats, allocation updates and per-operation actions (register/unregister, abort allocations, suspend/resume) to the right shard by node id. It also aggregates cross-shard statistics and maintains the address-to-id map. `INodeManagerHost` is the narrow interface it needs from `TScheduler`.

**`node_shard.h`** — `TNodeShard` processes heartbeats for its subset of exec nodes, exclusively on its own cancelable invoker: update the `TExecNode` descriptor and resource limits, consult the strategy's `INodeHeartbeatStrategyProxy` for allocation decisions, start/preempt/abort allocations, and route finished-allocation events back to the strategy. `TNodeShardGlobalSensors` holds the profiling counters shared across shards, initialized on master connection so only the active scheduler emits metrics.

**`common/exec_node.h`** — `TExecNode`, the scheduler-side representation of one exec node, owned by a node shard: resource limits and usage, disk resources, the running-allocation set, pending aborts, scheduling tags, scheduling segment, timestamps and state flags.

**`common/allocation.h`** — `TAllocation`, one scheduled allocation (one job slot on a node): `EAllocationState` (scheduled/running/finishing/finished), resource usage, limits and disk quota, preemption info, and references to the owning operation and controller epoch. State transitions are driven by node shard heartbeat processing.

**`controller_agent_tracker.h`** — `TControllerAgentTracker` tracks all connected `TControllerAgent` instances; handles handshakes, heartbeats and `ScheduleAllocation` heartbeats on a dedicated cancelable invoker; and owns the operation–agent lifecycle (pick an agent, assign and unregister operations, create controllers). `HandleAgentFailure` forces an unregister when the agent's lease lapses or an RPC fails.

**`controller_agent.h`** — `TControllerAgent`, the scheduler-side state of one connected agent: a state machine (`Registering → WaitingForInitialHeartbeat → Registered → Unregistering → Unregistered`), the set of operations it manages, incarnation transaction, lease, and the message-queue outboxes (scheduler → agent: allocation events, operation events, schedule-allocation requests) and inboxes (agent → scheduler: operation events, running-allocation statistics, schedule-allocation responses). The outbox/inbox pattern decouples node-shard-produced decisions from agent RPC latency: events are enqueued by one thread and drained at the next heartbeat.

**`operation_controller.h`** / **`operation_controller_impl.h`** — `IOperationController : NStrategy::ISchedulingOperationController` is the interface through which the scheduler drives one operation's controller: agent assignment and revocation, the async lifecycle steps (initialize, prepare, materialize, revive, commit, terminate, complete, register, unregister), local notifications (suspend, resume, allocation aborted / finished) and the matching completion callbacks driven by the tracker. `TOperationControllerImpl` posts RPCs to the assigned agent or enqueues entries in its outboxes and returns futures resolved from responses. `TControllerRuntimeData` is the live mutable state shared with the strategy (needed resources, grouped needed resources), updated on each agent heartbeat. Typed result structs with `FromProto` conversions wrap the lifecycle RPC responses.

**`master_connector.h`** — `TMasterConnector` owns the scheduler ↔ master lifecycle (`Disconnected → Connecting → Connected → Disconnecting`). On connect it acquires a lock transaction, fetches operation state, and produces a `TMasterHandshakeResult` (with revival descriptors) consumed by other subsystems; on disconnect it cancels in-flight work. It flushes `TOperation` state changes into Cypress, round-trips the strategy's persistent state, issues temporary operation tokens, and runs the watcher system: common watchers piggy-back on one periodic tick, custom watchers (`EWatcherType`) get their own period or lock. Signals: `MasterConnecting`, `MasterHandshake`, `MasterConnected`, `MasterDisconnected`.

**`operations_cleaner.h`** — `TOperationsCleaner` archives finished operations to the operations archive table and removes them after a configurable retention period; it also records operation alert events. `TArchiveOperationRequest` / `TRemoveOperationRequest` are the YSON-serializable descriptors it works with. `IOperationsCleanerHost` is its narrow host interface.

**`allocation_tracker_service.h`** / **`scheduler_service.h`** / **`controller_agent_tracker_service.h`** — RPC service factories: allocation status updates from node shards (no response keeper — the protocol is idempotent), client-facing operation management, and controller-agent-facing handshake and heartbeats. The latter two deduplicate retries through a response keeper.

**`helpers.h`** — YSON serialization helpers for operation attributes (Orchid and Cypress), `BuildSupportedFeatures` for the handshake response, and lightweight descriptors for Orchid views.

**Notes:**
- **Thread model.** The control thread is a `EControlQueue`-indexed fair-share action queue; different subsystems (user requests, master I/O, fair-share updates, GPU assignment, …) share it without starving each other. Node shards run on their own invokers. Controller-agent heartbeats run on a dedicated cancelable invoker owned by `TControllerAgentTracker`. Cross-thread communication uses futures or the explicit message-queue outbox/inbox pairs.
- **Revival.** On restart, the master connector fetches live operations and passes them through `TMasterHandshakeResult`. Each operation is revived by calling `controller->Revive()`, which returns running allocations, grouped needed resources, banned tree ids, and the "revived from snapshot" bit; allocations flow back through `TNodeManager::FinishOperationRevival` into the node shards. The same path handles a controller agent failure. Revival spans every layer and is easy to break: read `docs/revival.md` before touching operation enable / disable / register / unregister or allocation update handling.
- **Response keepers** deduplicate retried RPCs at the service layer — important because both clients and controller agents retry on timeout.
- `common/` is meant to hold types shared with the strategy side of the codebase; do not add scheduler-internal state there.

**See also:**
- `docs/revival.md` for the operation revival protocol and its invariants.
- `strategy/ARCHITECTURE.md` for the fair-share scheduling strategy embedded in the scheduler.
- `yt/yt/server/controller_agent/` for the controller agent daemon that the scheduler coordinates with.
- `yt/yt/server/node/exec_node/` for the exec node daemon that sends heartbeats.
- `yt/yt/server/lib/scheduler/` for shared protocol types (`EControlQueue`, alert enums, message queue templates, `TMasterHandshakeResult`, …) used by both the scheduler and its clients.
