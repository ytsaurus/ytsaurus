## `yt/yt/server/scheduler/strategy` — Fair-Share Scheduling Strategy

Implements the YT fair-share scheduling strategy: a forest of pool trees that assign compute resources to operations using the HDRF (Hierarchical Dominant Resource Fairness) algorithm, with support for preemption, scheduling segments, gang scheduling, and GPU-specific allocation policies.

Everything in this directory lives in the `NYT::NScheduler::NStrategy` sub-namespace.

**Design overview.**

The strategy is a mediator between the scheduler daemon and the remote controller agents. It owns:
- A **pool tree forest** (`IStrategy` / `IPoolTree`): one or more hierarchical trees, each with its own fair-share computation, operation placement, and scheduling policy.
- **Resource accounting**: a lock-split `TResourceTree` that tracks hierarchical resource usage without blocking scheduling on structural changes.
- **Snapshots**: immutable point-in-time views of each pool tree, consumed during heartbeat processing without locks.

Main data flow:
1. **Fair share update** (periodic, `FairShareUpdateInvoker`): each pool tree walks its elements, computes demand/usage/fair-share vectors, starvation statuses, and preemption attributes; the resulting `TPoolTreeSnapshot` is atomically installed. The forest is then re-snapshotted into a `TPoolTreeSetSnapshot`.
2. **Node heartbeat** (node-shard invoker): `IStrategy::CreateNodeHeartbeatStrategyProxy` returns an `INodeHeartbeatStrategyProxy` that picks the matching pool tree snapshot using the node's `TMatchingTreeCookie` (invalidated on topology version bumps), then delegates to `IPoolTree::ProcessSchedulingHeartbeat`, which runs the tree's `ISchedulingPolicy`. The policy schedules / preempts allocations based on dynamic attributes derived from the snapshot.
3. **Allocation updates** (node shard): `IStrategy::ProcessAllocationUpdates` buckets each `TAllocationUpdate` by tree, dispatches every per-tree batch to its policy without yielding in between (the classic policy applies the batch synchronously and returns an already-set future; the GPU policy hands it to the control invoker and returns a pending one), then awaits the futures with `WaitForFast` and applies the resulting resource-usage / postpone / abort decisions to the `TPoolTreeOperationElement`s and the `TResourceTree`. `WaitForFast` is deliberate: on an already-set future it does not reschedule the fiber, so the classic path has no suspension point between the node shard swapping its submit map out and re-adding postponed updates. Under the GPU policy the fiber does suspend, so several batches carrying an update for the same allocation can be in flight at once and do not resume in sweep order; that is why re-adding a postponed update *merges* it (`MergeAllocationUpdates`, declared in `node_shard.h`) field by field in a commutative way instead of inserting it.

**Components:**

**`strategy.h`** — Top-level interfaces.
- `IStrategy` — main strategy interface, created with `CreateStrategy(config, host, feasibleInvokers)` and exposed to the scheduler daemon. Its methods fall into heartbeat / allocation processing (`CreateNodeHeartbeatStrategyProxy`, `ProcessAllocationUpdates`, node registration), operation lifecycle (`RegisterOperation`, `EnableOperation`, `DisableOperation`, `UnregisterOperation[FromTree]`, `RegisterAllocationsFromRevivedOperation`, `OnOperationMaterialized`, runtime parameters), configuration (`UpdatePoolTrees`, `UpdateConfig`, validation of pools, permissions and limits), master interaction (`OnMasterHandshake` / `OnMasterConnected` / `OnMasterDisconnected`, `InitPersistentState`), Orchid / diagnostics, and the periodic entry points (`OnFairShareUpdateAt`, logging, profiling) that the scheduler and the simulator drive.
- `IStrategyHost` — the scheduler's side of the interface, implemented by `TScheduler::TImpl`: invokers, resource-limit queries, node-shard accessors, persistent-state round-trip, metering, alerts, operation control (abort, flush, mark running), permission validation, and the default-pool map.
- `INodeHeartbeatStrategyProxy` — per-node, per-heartbeat proxy whose `ProcessSchedulingHeartbeat(context, skipScheduleAllocations)` drives the scheduling pass.
- `TAllocationUpdate` — value struct carrying one finished / running / resource-updated allocation event from node shards into the strategy.

**`pool_tree.h`** — Pool tree interface and factory.
- `IPoolTree` — one pool tree: the heartbeat path (`ProcessSchedulingHeartbeat`, `ProcessAllocationUpdates` as a thin passthrough to the policy), the fair-share update and logging / profiling hooks, operation placement (register / unregister / enable / disable, pool changes, revival, materialization, running pending operations, stuck detection; the `OperationRunning` signal fires when an operation starts running in this tree), node registration, pool configuration and validation, snapshot accessors, persistence, metering, and Orchid builders.
- `IPoolTreeHost` — minimal callbacks (connectivity check, tree-level alert setter, ephemeral pool name regex).
- `TAccumulatedResourceDistribution` — tracks accumulated fair-share, usage, and usage deficit over time per operation; used for event-log metering.
- `CreatePoolTree(...)` — factory; wires a `TResourceTree`, `TPoolTreeProfileManager`, and the appropriate `NPolicy::ISchedulingPolicy`.

**`pool_tree_element.h`** — The pool tree element hierarchy.
- `TPoolTreeElement` — base class for all elements. Participates in HDRF fair share computation by implementing `NVectorHdrf` element interfaces. Holds a `TResourceTreeElementPtr`, a `TreeIndex` assigned during post-update for O(1) attribute lookup, and `TPersistentAttributes` (survive updates).
- `TPoolTreeCompositeElement` — internal node (pool or root). Holds child elements and aggregated fair-share attributes; manages child preemption settings, FIFO mode, pool limits, and the child-allocation ordering used by the policy.
- `TPoolTreePoolElement` — pool-specific state: weights, guarantees, limits, burst/flow integral resources, ephemeral-pool flags.
- `TPoolTreeOperationElement` — leaf for an operation. Holds the `ISchedulingOperationControllerPtr`, needed resources, scheduling tag filter, gang/slot info, and best-allocation share history.
- `TPoolTreeRootElement` — drives the `PreUpdate → Update → PostUpdate` pipeline for the tree.
- `TPersistentAttributes` — starvation status and timestamps, fair share and usage at starvation start, historic usage, best allocation share, integral resources state, and applied specified-resource-limits state. `ResetOnElementEnabled()` is called when an element transitions from disabled back to enabled.
- `TFairSharePreUpdateContext` / `TFairSharePostUpdateContext` — phase-local contexts; the post-update context carries the unschedulable reasons, the enabled / disabled operation maps, and the pool name → element map that end up in the snapshot.

**`pool_tree_snapshot.h`** — Immutable tree snapshot.
- `TPoolTreeSnapshot` — installed atomically after each fair-share update: root element, enabled / disabled operation maps, pool map, tree and controller config, total resource usage / limits, node addresses, the opaque `SchedulingPolicyState` set by the tree's policy, and resource limits by tag filter. Heartbeats read from their captured snapshot without locking.
- `TPoolTreeSetSnapshot` — snapshot of the forest (`Trees` + `TopologyVersion`); the topology version invalidates per-node `TMatchingTreeCookie`s whenever a tree is added/removed.
- `TResourceUsageSnapshot` — lighter snapshot built more frequently than the full one (alive operation ids, operation and pool usage with and without precommit). Keeps dynamic attributes reasonably fresh without waiting for the next fair-share update.

**`resource_tree.h`** / **`resource_tree_element.h`** — Lock-split resource accounting.
- `TResourceTree` — thread-safe hierarchical resource usage tracker with two lock levels: `StructureLock_` (reader-writer) protects the tree shape and is held as a reader during hierarchical usage propagation and as a writer only during restructuring; a per-element `ResourceUsageLock_` protects the element's local counters.
  - `TryIncreaseHierarchicalResourceUsagePrecommit(...)` reserves resources up the hierarchy and reports which limit, if any, was violated; `CommitHierarchicalResourceUsage(...)` converts the precommit into usage once the allocation has started.
  - `TryIncreaseHierarchicalPreemptedResourceUsagePrecommit` / `CommitHierarchicalPreemptedResourceUsage` — a separate precommit channel used by limits-violation preemption, gated by `UsePrecommitForPreemption`. See `docs/preemption_precommit.md`.
  - `AttachParent` / `ChangeParent` / `ScheduleDetachParent` / `PerformPostponedActions` — structural edits. Detach is deferred via an MPSC stack to avoid holding the structure write lock while a heartbeat might be running.
- `TResourceTreeElement` — per-element counters: `ResourceUsage_`, `ResourceUsagePrecommit_`, `PreemptedResourceUsagePrecommit_`; optional `SpecifiedResourceLimits_` (+ overcommit tolerance) for per-element caps. With the tree option `enable_infinite_resource_limits_overcommit` (requires `use_precommit_for_preemption`) the specified-limits check is skipped in preemptive scheduling stages, and the excess is repaid by the limits-violation preemption pass. `SetNonAlive()` freezes the element; no usage changes may happen afterward.

**`operation.h`** — Strategy-side operation interface.
- `IOperation` — minimal view of an operation the strategy consumes: type / state / start time, spec and runtime parameters, authenticated user, the scheduling operation controller, slot-index bookkeeping, and tree-erasure tracking. Implemented by `TOperation` on the scheduler side.
- `TOperationPoolTreeAttributes` — per-tree bookkeeping pushed back into the `TOperation`: slot index, ephemeral/lightweight pool flags.

**`operation_state.h`** — `TStrategyOperationState` owns the `IOperation` host pointer, a `TOperationController`, the tree → pool name map, and the enabled flag. One per operation per strategy registration.

**`operation_controller.h`** — Operation controller proxy.
- `ISchedulingOperationController` — narrow interface wrapping the remote controller agent: `ScheduleAllocation(...)`, the main call the policy issues during scheduling, plus epoch, needed-resources and preemption-mode accessors.
- `TOperationController` — concrete wrapper with the throttling / backoff machinery the strategy needs: per-node-shard counters of concurrent schedule-allocation calls and exec duration, backoff deadlines, and tentative-tree saturation tracking. `ScheduleAllocation` forwards to the underlying controller with throttling applied; the `OnScheduleAllocation…` callbacks maintain the counters.

**`scheduling_heartbeat_context.h`** / **`scheduling_heartbeat_context_detail.h`** — `CreateSchedulingHeartbeatContext(...)` produces an `NPolicy::ISchedulingHeartbeatContextPtr`; `TSchedulingHeartbeatContextBase` is the concrete implementation that owns a `TExecNode` reference, maintains running / started / preempted allocation lists, tracks free disk and resource quotas with discount bookkeeping, and holds scheduling statistics. Subclasses extend it for GPU-specific contexts.

**`pool_tree_profile_manager.h`** — `TPoolTreeProfileManager` runs on a dedicated profiling invoker and emits per-operation (accumulated resources, job metrics, scheduling stages, preemption reasons), per-pool (fair share, resource usage, starvation intervals), and tree-level counters.

**`persistent_state.h`** — Persistent strategy state: `TPersistentStrategyState` maps tree id to `TPersistentTreeState`, which holds per-pool `AccumulatedResourceVolume` (used for integral-guarantee fairness across restarts) plus the opaque policy blobs `SchedulingPolicyState` and `DryRunGpuSchedulingPolicyState`.

**`pools_config_parser.h`** — `TPoolsConfigParser` parses pool tree YSON config into an ordered sequence of primitive actions (`Keep`, `Create`, `Move`, `Erase`) that is applied to the live tree so that every intermediate state is consistent.

**`field_filter.h`** — `TFieldFilter` (in the outer `NYT::NScheduler` namespace) filters which YSON fields are emitted in progress / orchid outputs, with `ITEM_…_IF_SUITABLE_FOR_FILTER` macros used throughout progress builders.

**`helpers.h`** / **`job_resources_helpers.h`** — Per-tree runtime-parameter lookup, `TSchedulerTreeAlertDescriptor`, available / adjusted resource computations, and miscellaneous resource-formatting and pool-name helpers.

**Notes:**
- **Thread model.** Most control-path operations run on the scheduler's control thread. Fair share update runs on `FairShareUpdateInvoker`. Profiling runs on `FairShareProfilingInvoker` so it never blocks the control thread. Scheduling runs on per-node-shard invokers (multiple threads concurrently). `TResourceTree` and the per-operation state are explicitly designed for concurrent access from those shards.
- **Tree indices.** `TPoolTreeElement::TreeIndex` is used extensively as an O(1) key into flat arrays (`TStaticAttributesList`, `TDynamicAttributesList`) inside the policy. Indices are assigned during post-update; they become invalid once the tree is restructured, so callers must never carry an index across fair-share-update boundaries.
- **Snapshot stability.** `TPoolTreeSnapshot` is atomically swapped via `TAtomicPtr`; a heartbeat handler captures the snapshot it saw at entry and uses it throughout, even if a new snapshot is published mid-heartbeat. `TResourceUsageSnapshot` is refreshed on a faster cadence to keep dynamic attributes close to reality without forcing a full fair-share update.
- **Persistent state** is stored in Cypress under the scheduler node. On master reconnect, `InitPersistentState` restores integral resource volumes and policy state within a grace period; state written after the grace period is discarded.
- **Detach deferral.** `TResourceTree::ScheduleDetachParent` queues detach into an MPSC stack; `PerformPostponedActions` drains it under the structure write lock between heartbeats. This avoids holding the structure writer lock during scheduling.
- **Revival orphan handling.** A postponed allocation update can be orphaned across an operation disable/revive: while `SubmitAllocationsToStrategy` has its submit map swapped out, a concurrent `StartOperationRevival` purges the per-operation index, and a stale update re-added afterwards would be immune to every later purge and crash on replay into the revived shared state. Three layered defenses: (1) the strategy awaits with `WaitForFast`, so the classic path has no suspension point in that window; (2) `SubmitAllocationsToStrategy` re-adds a postponed update only when the operation's controller epoch still matches the one stamped on the update, and keeps the per-operation index mirroring the global map so the entry stays purgeable; (3) the policy answers `EAllocationUpdateStatus::Unexpected` for an allocation unknown to the current state and the caller drops it rather than postponing or crashing — the backstop for the multi-tree / GPU cases, where the awaiting fiber can still yield inside the window. See `../docs/revival.md`.

**See also:**
- `../docs/revival.md` for the cross-layer operation revival protocol and the invariants the strategy and policies must keep across disable / revive / enable.
- `policy/ARCHITECTURE.md` for the scheduling policy layer (classic and GPU policies).
- `policy/gpu/ARCHITECTURE.md` for the GPU assignment planning algorithm.
- `yt/yt/library/vector_hdrf/` for the HDRF fair share computation algorithm.
- `yt/yt/server/scheduler/` (parent directory) for the scheduler daemon that hosts the strategy.
