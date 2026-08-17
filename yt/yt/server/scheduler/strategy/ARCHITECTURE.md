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
3. **Allocation updates** (node shard): `IStrategy::ProcessAllocationUpdates` buckets each `TAllocationUpdate` by tree and processes every per-tree batch (the classic policy applies it synchronously and returns an already-set `TFuture<std::vector<TProcessAllocationUpdateResult>>`; the GPU policy dispatches it to the control invoker and returns a pending future) under a single `TForbidContextSwitchGuard`, then `WaitForFast`s each future and applies the resource-usage / postpone / abort decisions to the relevant `TPoolTreeOperationElement` and `TResourceTree`. `WaitForFast` (not `WaitFor`) is deliberate: for the classic already-set future it takes the set-future fast path and does **not** reschedule the node-shard fiber. A plain `WaitFor` yields even on a set future, and that yield was the only fiber-suspension point between `SubmitAllocationsToStrategy` swapping its submit map out and re-adding the postponed updates — it let a concurrent `StartOperationRevival` interleave in that window and orphan a postponed update (see *Revival orphan handling* below). Processing/enqueuing all batches before any fiber suspension, plus fully draining each batch before the next `SubmitAllocationsToStrategy` call, guarantees that updates for one allocation (one allocation ⇒ one tree) are always applied in drain order — this is the ordering invariant that prevents a stale parked update from overtaking a newer one. The call stays synchronous from the node shard's perspective.

**Components:**

**`strategy.h`** — Top-level interfaces.
- `IStrategy` — main strategy interface, created with `CreateStrategy(config, host, feasibleInvokers)`. Exposed to the scheduler daemon. Groups of methods:
  - Heartbeat / allocations: `CreateNodeHeartbeatStrategyProxy(nodeId, address, tags, cookie)`, `ProcessAllocationUpdates(updates, postpone, abort)` (synchronous; dispatches each tree's batch and `WaitForFast`s it), `RegisterOrUpdateNode`, `UnregisterNode`.
  - Operation lifecycle: `ValidateOperationStart`, `RegisterOperation`, `EnableOperation`, `DisableOperation`, `UnregisterOperation`, `UnregisterOperationFromTree`, `RegisterAllocationsFromRevivedOperation`, `OnOperationMaterialized`, `ApplyJobMetricsDelta`, `ApplyOperationRuntimeParameters`, `InitOperationRuntimeParameters`, `UpdateRuntimeParameters`.
  - Configuration: `UpdatePoolTrees(yson)`, `UpdateUserToDefaultPoolMap`, `UpdateConfig`, `ValidateOperationRuntimeParameters`, `ValidateOperationPoolPermissions`, `ValidatePoolLimitsOnPoolChange`, `GetPoolLimitViolations`.
  - Master interaction: `OnMasterHandshake`, `OnMasterConnected`, `OnMasterDisconnected`, `InitPersistentState`.
  - Orchid / diagnostics: `BuildOperationInfoForEventLog`, `BuildOperationProgress`, `BuildBriefOperationProgress`, `BuildOrchid`, `GetOrchidService`, `BuildSchedulingAttributesForNode`, `GetStuckOperations`, `ScanPendingOperations`, `GetResourceLimitsByTagFilter`, `GetFullFairShareUpdateFinished`.
  - Periodic (also driven from simulator tests): `OnFairShareUpdateAt`, `OnFairShareLoggingAt`, `OnFairShareEssentialLoggingAt`, `OnFairShareProfilingAt`.
- `IStrategyHost` — the scheduler's side of the interface; implemented by `TScheduler::TImpl`. Provides control/fair-share/profiling/background/orchid invokers, resource-limit queries (`GetResourceLimits(filter)`, `GetResourceUsage(filter)`, `GetExecNodeMemoryDistribution`), node-shard accessors (`GetNodeShardInvokers`, `GetNodeShardId`, `AbortAllocationsAtNode`), resource formatting/serialization, medium directory lookups, persistent-state round-trip (`InvokeStoringStrategyState`), metering, alert APIs (`SetSchedulerAlert`, `SetOperationAlert`), operation control (`AbortOperation`, `FlushOperationNode`, `MarkOperationAsRunningInStrategy`), permission validation, and the default-pool map.
- `INodeHeartbeatStrategyProxy` — per-node, per-heartbeat proxy. `ProcessSchedulingHeartbeat(context, skipScheduleAllocations)` drives the scheduling pass. Also exposes `GetMatchingTreeCookie()`, `HasMatchingTree()`, `GetSchedulingHeartbeatComplexity()`, `GetMinSpareResourcesForScheduling()`.
- `TAllocationUpdate` — value struct carrying one finished/running/resource-updated allocation event from node shards into the strategy.

**`pool_tree.h`** — Pool tree interface and factory.
- `IPoolTree` — represents one pool tree. Key methods:
  - Heartbeat path: `ProcessSchedulingHeartbeat`, `ProcessAllocationUpdates(updates) → TFuture<...>` (a thin passthrough to the policy; the strategy awaits the future and converts the results), `GetSchedulingHeartbeatComplexity`.
  - Fair share: `OnFairShareUpdateAt`, `FinishFairShareUpdate`, `UpdateResourceUsages`, `ExtractAccumulatedResourceDistributionForLogging`, `ProfileFairShare`, `LogFairShareAt`, `LogAccumulatedUsage`, `EssentialLogFairShareAt`.
  - Operation placement: `RegisterOperation`, `UnregisterOperation`, `EnableOperation`, `DisableOperation`, `ChangeOperationPool`, `UpdateOperationRuntimeParameters`, `RegisterAllocationsFromRevivedOperation`, `OnOperationMaterialized`, `ProcessActivatableOperations`, `TryRunAllPendingOperations`, `CheckIsOperationStuck`. Signal `OperationRunning(TOperationId)` fires when an operation transitions to running in this tree.
  - Nodes: `RegisterNode`, `UnregisterNode`, `GetNodeTagFilter`, `BuildSchedulingAttributesForNode`, `BuildSchedulingAttributesStringForOngoingAllocations`.
  - Configuration: `UpdatePools(poolsNode, forceUpdate)`, `UpdateConfig`, `UpdateControllerConfig`, `ActualizeEphemeralPoolParents`, `CreatePoolName`, `GetOffloadingSettingsFor`, `ValidatePoolLimits(OnPoolChange)`, `ValidateOperationPoolsCanBeUsed`, `ValidateOperationPoolPermissions`, `EnsureOperationPoolExistence`, `ValidateUserToDefaultPoolMap`.
  - Snapshot accessors: `IsSnapshottedOperationRunningInTree`, `GetSnapshottedConfig`, `GetSnapshottedTotalResourceLimits`, `GetMaybeStateSnapshotForPool`.
  - Persistence / metering / orchid: `BuildPersistentState`, `InitPersistentState`, `BuildResourceMetering`, `GetResourceLimitsByTagFilter`, `BuildOperationAttributes`, `BuildOperationProgress`, `BuildBriefOperationProgress`, `BuildStaticPoolsInformation`, `BuildUserToEphemeralPoolsInDefaultPool`, `BuildFairShareInfo`, `GetOrchidService`.
- `IPoolTreeHost` — minimal callbacks (connectivity check, tree-level alert setter, ephemeral pool name regex).
- `TAccumulatedResourceDistribution` — tracks accumulated fair-share, usage, and usage deficit over time per operation; used for event-log metering.
- `TPoolsUpdateResult`, `TPoolTreeElementStateSnapshot` — small value structs surfaced by the interface.
- `CreatePoolTree(...)` — factory; wires a `TResourceTree`, `TPoolTreeProfileManager`, and the appropriate `NPolicy::ISchedulingPolicy`.

**`pool_tree_element.h`** — The pool tree element hierarchy.
- `TPoolTreeElement` — base class for all elements. Participates in HDRF fair share computation by implementing `NVectorHdrf` element interfaces. Holds a `TResourceTreeElementPtr`, a `TreeIndex` assigned during post-update for O(1) attribute lookup, and `TPersistentAttributes` (survive updates).
- `TPoolTreeCompositeElement` — internal node (pool or root). Holds child elements and aggregated fair-share attributes; manages child preemption settings, FIFO mode, pool limits, and the child-allocation ordering used by the policy.
- `TPoolTreePoolElement` — pool-specific state: weights, guarantees, limits, burst/flow integral resources, ephemeral-pool flags.
- `TPoolTreeOperationElement` — leaf for an operation. Holds the `ISchedulingOperationControllerPtr`, needed resources, scheduling tag filter, gang/slot info, and best-allocation share history.
- `TPoolTreeRootElement` — drives the `PreUpdate → Update → PostUpdate` pipeline for the tree.
- `ESchedulerElementType` — `Root / Pool / Operation`.
- `EStarvationStatus` (`NonStarving`, `Starving`, `AggressivelyStarving`) + `EStarvationChangeReason` (`FairShareDecreased`, `UsageIncreased`).
- `TPersistentAttributes` — `StarvationStatus`, timestamps (`StarvingSince`, `BelowFairShareSince`, `LastNonStarvingTime`), `FairShareOnStarvationStart` / `UsageOnStarvationStart`, historic usage (`TAdjustedExponentialMovingAverage`), `BestAllocationShare` (+ last update time), `TIntegralResourcesState`, and applied specified-resource-limits state. `ResetOnElementEnabled()` is called when an element transitions from disabled back to enabled.
- `TFairSharePreUpdateContext` / `TFairSharePostUpdateContext` — phase-local contexts; the post-update context carries `UnschedulableReasons`, the enabled / disabled operation maps, and the pool name → element map that end up in the snapshot.
- `TResourceDistributionInfo`, `TPoolTreeElementPostUpdateAttributes` — carry the derived attributes (distributed guarantees, satisfaction ratios, starvation intervals, …).
- `IPoolTreeElementHost` — interface exposing `GetResourceTree()` and element-logging helpers to elements.

**`pool_tree_snapshot.h`** — Immutable tree snapshot.
- `TPoolTreeSnapshot` — installed atomically after each fair-share update. Contains: `Id` (GUID), `Now`, `RootElement`, `EnabledOperationMap` / `DisabledOperationMap`, `PoolMap`, `TreeConfig`, `ControllerConfig`, total `ResourceUsage` / `ResourceLimits`, `NodeAddresses`, `SchedulingPolicyState` (opaque, set by the tree's policy), and `ResourceLimitsByTagFilter`. Heartbeats read from their captured snapshot without locking.
- `TPoolTreeSetSnapshot` — snapshot of the forest (`Trees` + `TopologyVersion`); the topology version invalidates per-node `TMatchingTreeCookie`s whenever a tree is added/removed.
- `TResourceUsageSnapshot` — lighter snapshot built more frequently than the full one (`BuildTime`, `AliveOperationIds`, operation and pool usage with and without precommit). Keeps dynamic attributes reasonably fresh without waiting for the next fair-share update.
- `BuildResourceUsageSnapshot(treeSnapshot)` — extracts the current usages from a full tree snapshot.

**`resource_tree.h`** / **`resource_tree_element.h`** — Lock-split resource accounting.
- `TResourceTree` — thread-safe hierarchical resource usage tracker. Two lock levels (with on-demand profiling counters):
  1. `StructureLock_` (reader-writer): protects tree shape (`Parent_` pointers). Held as a reader during hierarchical usage propagation; acquired as a writer only during pool tree restructuring.
  2. Per-element `ResourceUsageLock_` (reader-writer, padded): protects the element's local counters.
  - `TryIncreaseHierarchicalResourceUsagePrecommit(...)` — reserves resources up the hierarchy; returns `EResourceTreeIncreaseResult` indicating success or which limit was violated, with an optional `availableResourceLimitsOutput` explaining how much headroom remained.
  - `CommitHierarchicalResourceUsage(...)` — converts a precommit into actual usage after allocation starts successfully.
  - `TryIncreaseHierarchicalPreemptedResourceUsagePrecommit` / `CommitHierarchicalPreemptedResourceUsage` — separate precommit channel used by preemption logic, gated by `UsePrecommitForPreemption`. The commit takes both `resourceUsageDelta` (negative, applied to `ResourceUsage_`) and `precommittedResources` (positive, subtracted from `PreemptedResourceUsagePrecommit_`); the two are tracked independently. See `docs/preemption_precommit.md` for the full flow.
  - `AttachParent` / `ChangeParent` / `ScheduleDetachParent` / `PerformPostponedActions` — structural edits. Detach is deferred via an MPSC stack (`ElementsToDetachQueue_`) to avoid holding the structure write lock while a heartbeat might be running.
- `TResourceTreeElement` — per-element counters: `ResourceUsage_`, `ResourceUsagePrecommit_`, `PreemptedResourceUsagePrecommit_`; optional `SpecifiedResourceLimits_` (+ overcommit tolerance) for per-element caps; with the tree option `enable_infinite_resource_limits_overcommit` (requires `use_precommit_for_preemption`) the specified-limits check is skipped entirely in preemptive scheduling stages, and the excess is repaid by the limits-violation preemption pass. `SetNonAlive()` freezes the element; no usage changes may happen afterward. `GetDetailedResourceUsage()` exposes base/precommit split for diagnostics.

**`operation.h`** — Strategy-side operation interface.
- `IOperation` — minimal view of an operation the strategy consumes: type / state / start time, spec accessors (`GetStrategySpec`, per-tree variant), runtime parameters, authenticated user, scheduling operation controller (`GetControllerStrategyHost`), slot-index bookkeeping (`FindSlotIndex` / `SetSlotIndex` / `ReleaseSlotIndex`), and tree-erasure tracking (`IsTreeErased`, `EraseTrees`, `UpdatePoolAttributes`). Implemented by `TOperation` on the scheduler side.
- `TOperationPoolTreeAttributes` — per-tree bookkeeping pushed back into the `TOperation`: slot index, ephemeral/lightweight pool flags.

**`operation_state.h`** — Per-operation strategy state.
- `TStrategyOperationState` — owns the `IOperation` host pointer, a `TOperationController`, the tree → pool name map, and the enabled flag. One per operation per strategy registration.

**`operation_controller.h`** — Operation controller proxy.
- `ISchedulingOperationController` — narrow interface wrapping the remote controller agent. `ScheduleAllocation(...)` is the main call the policy issues during scheduling; also exposes `GetEpoch`, `GetNeededResources`, `UpdateGroupedNeededResources`, `GetGroupedNeededResources`, `GetInitialGroupedNeededResources`, `OnNonscheduledAllocationAborted`, and `GetPreemptionMode`.
- `TOperationController` — concrete wrapper around `ISchedulingOperationController` with the throttling / backoff machinery the strategy needs:
  - Per-node-shard state shards (cache-line padded) counting concurrent schedule-allocation calls and exec duration, maintaining an estimate, and holding a backoff deadline.
  - `UpdateConcurrentScheduleAllocationThrottlingLimits`, `CheckMaxScheduleAllocationCallsOverdraft`, `IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated`, `IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated`, `HasRecentScheduleAllocationFailure`, `ScheduleAllocationBackoffObserved`.
  - Tentative-tree saturation tracking (`IsSaturatedInTentativeTree`).
  - `ScheduleAllocation` forwards to the underlying controller with throttling applied; `OnScheduleAllocationStarted` / `OnScheduleAllocationFinished` / `OnScheduleAllocationFailed` maintain the counters. `AbortAllocation` is exposed for now (flagged for a future move to private).

**`scheduling_heartbeat_context.h`** / **`scheduling_heartbeat_context_detail.h`** — Per-heartbeat context.
- `CreateSchedulingHeartbeatContext(nodeShardId, config, node, runningAllocations, mediumDirectory, defaultMinSpareAllocationResources)` — factory producing an `NPolicy::ISchedulingHeartbeatContextPtr`.
- `TSchedulingHeartbeatContextBase` — concrete implementation. Owns a `TExecNode` reference, maintains running / started / preempted allocation lists, tracks free disk and resource quotas with discount bookkeeping, implements `CanStartAllocation` / `CanStartMoreAllocations` / `CanSchedule(filter)` / `ShouldAbortAllocationsSinceResourcesOvercommit`, holds scheduling statistics, schedule-allocation exec-duration estimate, and a heartbeat-timeout flag. Subclasses extend it for GPU-specific contexts.

**`pool_tree_profile_manager.h`** — Profiling.
- `TPoolTreeProfileManager` — runs on a dedicated profiling invoker; emits per-operation (accumulated resources, job metrics, scheduling stages, preemption reasons), per-pool (fair share, resource usage, starvation intervals), and tree-level counters. Call entry points include `ProfileTree`, `ApplyJobMetricsDelta`, `ApplyScheduledAndPreemptedResourcesDelta` (takes four separate maps — scheduled, preempted, preempted resource-time, and improperly-preempted deltas bucketed by stage and reason), and `ProfileStarvationIntervals`.

**`persistent_state.h`** — Persistent strategy state.
- `TPersistentStrategyState` — `THashMap<tree_id, TPersistentTreeStatePtr>`.
- `TPersistentTreeState` — per-tree state: `PoolStates` (`THashMap<pool_name, TPersistentPoolStatePtr>`) plus opaque policy blobs `SchedulingPolicyState` and `DryRunGpuSchedulingPolicyState`.
- `TPersistentPoolState` — per-pool `AccumulatedResourceVolume` (`TResourceVolume`), used for integral-guarantee fairness across restarts.

**`pools_config_parser.h`** — Pool configuration parsing.
- `TPoolsConfigParser` — parses pool tree YSON config and produces an ordered sequence of primitive actions (`EUpdatePoolActionType`: `Keep`, `Create`, `Move`, `Erase`). The ordered sequence is applied to the live tree so that every intermediate state is consistent.
- `TUpdatePoolAction` — one such action (pool name, parent name, config, object id, type).

**`field_filter.h`** — YSON field filtering (in the outer `NYT::NScheduler` namespace, not `NStrategy`).
- `TFieldFilter` — filters which YSON fields are emitted in progress / orchid outputs. Constructed from an `IAttributeDictionary` and queried with `IsFieldSuitable(field)`.
- Macros `ITEM_DO_IF_SUITABLE_FOR_FILTER`, `ITEM_VALUE_IF_SUITABLE_FOR_FILTER`, `ITEM_OPTIONAL_VALUE_IF_SUITABLE_FOR_FILTER` — used throughout progress builders to conditionally emit fields.

**`helpers.h`** / **`job_resources_helpers.h`** — Utilities.
- `GetSchedulingOptionsPerPoolTree(operation, treeId)` — look up runtime parameters.
- `TSchedulerTreeAlertDescriptor`, `ComputeAvailableResources(...)`, `GetAdjustedResourceLimits(...)`, and miscellaneous resource-formatting / pool-name-construction helpers.

**Notes:**
- **Thread model.** Most control-path operations run on the scheduler's control thread. Fair share update runs on `FairShareUpdateInvoker`. Profiling runs on `FairShareProfilingInvoker` so it never blocks the control thread. Scheduling runs on per-node-shard invokers (multiple threads concurrently). `TResourceTree` and the per-operation state are explicitly designed for concurrent access from those shards.
- **Tree indices.** `TPoolTreeElement::TreeIndex` is used extensively as an O(1) key into flat arrays (`TStaticAttributesList`, `TDynamicAttributesList`) inside the policy. Indices are assigned during post-update; they become invalid once the tree is restructured, so callers must never carry an index across fair-share-update boundaries.
- **Snapshot stability.** `TPoolTreeSnapshot` is atomically swapped via `TAtomicPtr`; a heartbeat handler captures the snapshot it saw at entry and uses it throughout, even if a new snapshot is published mid-heartbeat. `TResourceUsageSnapshot` is refreshed on a faster cadence to keep dynamic attributes close to reality without forcing a full fair-share update.
- **Persistent state** is stored in Cypress under the scheduler node. On master reconnect, `InitPersistentState` restores integral resource volumes and policy state within a grace period; state written after the grace period is discarded.
- **Detach deferral.** `TResourceTree::ScheduleDetachParent` queues detach into an MPSC stack; `PerformPostponedActions` drains it under the structure write lock between heartbeats. This avoids holding the structure writer lock during scheduling.
- **Revival orphan handling.** A postponed allocation update can be orphaned across an operation disable/revive (YT-28521). While `SubmitAllocationsToStrategy` has its global submit map swapped out, a concurrent `StartOperationRevival` on the same node-shard invoker clears the per-operation index; the submit then re-adds the postponed update to the global map with no per-op backing — a stale update immune to every later purge, which on replay into the revived (fresh) shared state crashes in `GetAllocationProperties` / on finish. Three defenses, layered: (1) the strategy awaits with `WaitForFast`, so the classic set-future case introduces no fiber-suspension point in the swapped-out window and revival can no longer interleave there; (2) `SubmitAllocationsToStrategy` only re-adds a postponed update its operation still tracks, dropping orphans; (3) the policy's `TOperationSharedState` returns `EAllocationUpdateStatus::Unexpected` for an allocation unknown to the current incarnation and the caller drops it (rather than postponing or crashing) — the backstop for the multi-tree / GPU cases, where not every per-tree future is already set and the awaiting fiber can still yield inside the window.

**See also:**
- `policy/ARCHITECTURE.md` for the scheduling policy layer (classic and GPU policies).
- `policy/gpu/ARCHITECTURE.md` for the GPU assignment planning algorithm.
- `yt/yt/library/vector_hdrf/` for the HDRF fair share computation algorithm.
- `yt/yt/server/scheduler/` (parent directory) for the scheduler daemon that hosts the strategy.
