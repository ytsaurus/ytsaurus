## `yt/yt/server/scheduler/strategy/policy` — Scheduling Policies

Per-pool-tree scheduling policies: allocates resources to operations on individual node heartbeats (classic policy) or via periodic assignment planning (GPU policy). See `gpu/ARCHITECTURE.md` for the GPU-specific policy. Everything outside `gpu/` reaches the GPU policy only through `gpu/scheduling_policy.h`.

Namespace: `NYT::NScheduler::NStrategy::NPolicy` (the GPU policy lives under the nested `NGpu`).

**Design overview.**

Each pool tree has exactly one `ISchedulingPolicy`. The choice between the two implementations is made upstream in `pool_tree.cpp` based on `config->PolicyKind` (`EPolicyKind::Classic` or `EPolicyKind::Gpu`), which dispatches to either `CreateSchedulingPolicy` (this directory) or `NGpu::CreateAllocatingSchedulingPolicy`. Both implementations satisfy the same `ISchedulingPolicy` interface but differ in how they use node heartbeats:
- **Classic policy** (`TSchedulingPolicy`): on each node heartbeat, runs multiple regular + preemptive scheduling stages to greedily pick the best operation and call the controller to start allocations.
- **GPU policy** (`NGpu::TSchedulingPolicy`): uses heartbeats only to apply a pre-computed assignment plan; planning is decoupled into a periodic executor.

Because the snapshot-embedded scheduling state must stay opaque to code that operates generically (e.g. `pool_tree.cpp`), both `TPostUpdateContext` and `TPoolTreeSnapshotState` are abstract bases tagged with a `const EPolicyKind`. `TPoolTreeSnapshotState` declares pure-virtual `CheckIsOperationStuck`, `BuildOperationProgress`, and `BuildElementYson`; each policy implements them on its own state subclass. `TPoolTreeSnapshot` exposes thin facades (`treeSnapshot->CheckIsOperationStuck(...)`) that forward to `SchedulingPolicyState_->...(*this, ...)`, so call sites do not need to thread the policy pointer or downcast.

**Components:**

**`scheduling_policy.h`** — The main policy interface.
- `ISchedulingPolicy` — pure-virtual. Method groups:
  - *Lifecycle*: `Initialize`, `UpdateConfig(config)`.
  - *Nodes*: `RegisterNode(nodeId, address)`, `UnregisterNode(nodeId)`.
  - *Heartbeat*: `ProcessSchedulingHeartbeat(context, treeSnapshot, skipScheduleAllocations)`. Thread affinity: any.
  - *Operations*: `RegisterOperation`, `UnregisterOperation`, `OnOperationMaterialized`, `CheckOperationSchedulingInSeveralTreesAllowed`, `EnableOperation`, `DisableOperation(element, markAsNonAlive)`.
  - *Allocations*: `RegisterAllocationsFromRevivedOperation`; `ProcessAllocationUpdates(treeSnapshot, allocationUpdates) → TFuture<std::vector<TProcessAllocationUpdateResult>>` — the batch entry point: the classic policy applies the whole batch synchronously and returns an already-set future; the GPU policy dispatches it to the control invoker and returns a pending future. Results are index-aligned with the input. The strategy awaits all per-tree futures and turns each `TProcessAllocationUpdateResult` (status + optional postpone/abort/abortReason) into postpone/abort decisions. The `Status` drives that: `Updated` is applied; `Disabled` (operation not enabled) is postponed until the operation re-enables; `Unexpected` (allocation unknown to the current shared state — e.g. a stale update replayed across a disable/revive) is **dropped**, not postponed — the shared-state helpers (`ProcessAllocationUpdate` / `OnAllocationFinished`) return `Unexpected` instead of crashing on a missing allocation, so the stale update is discarded rather than resubmitted forever (see `strategy/ARCHITECTURE.md` → *Revival orphan handling*). Batches are applied in dispatch order (the classic worker synchronously at dispatch, the GPU worker on the FIFO control invoker), and the worker runs under a `TForbidContextSwitchGuard` so it cannot suspend mid-batch. The strategy does **not**, however, drain one batch before the next is swept: under the GPU policy several batches are in flight at once, so a postponed update can be re-added after a newer one has already been applied (see `strategy/ARCHITECTURE.md` → *Allocation updates*). The per-update helper `ProcessAllocationUpdate(treeSnapshot, element, update) → TProcessAllocationUpdateResult` is called in a loop by that worker.
  - *Post-update*: `CreatePostUpdateContext(root) → TPostUpdateContextPtr`, `PostUpdate(fairSharePostUpdateContext, postUpdateContextPtrInOut)`, `CreateSnapshotState(postUpdateContextPtrInOut) → TPoolTreeSnapshotStatePtr`. `OnResourceUsageSnapshotUpdate(treeSnapshot, usageSnapshot)` — runs between full updates to refresh dynamic attributes.
  - *Diagnostics / profiling*: `BuildSchedulingAttributeTagsForNode`, `BuildSchedulingAttributesForNode`, `BuildSchedulingAttributeTagsForOngoingAllocations`, `BuildElementLoggingTags`, `PopulateOrchidService`, `ProfileOperation`.
  - *Persistence*: `InitPersistentState(node)` / `BuildPersistentState() → INodePtr`. The serialized node is embedded in the strategy-level `TPersistentTreeState`.
- `ISchedulingPolicyHost` — tiny host interface (`GetTreeSnapshot`). Kept as a weak pointer inside `TSchedulingPolicy` because the owning tree can outlive async actions. Flagged for removal.
- `TPostUpdateContext` / `TPoolTreeSnapshotState` — abstract bases, each carrying the `EPolicyKind` tag.
- `TPoolTreeSnapshotState::{CheckIsOperationStuck, BuildOperationProgress, BuildElementYson}` — pure-virtual snapshot-state methods invoked through `TPoolTreeSnapshot` facades. Each policy implements the trio on its own state subclass.
- `CreateSchedulingPolicy(treeId, logger, host, treeHost, strategyHost, config, profiler)` — factory for the **classic** policy only; the GPU variants (`CreateAllocatingSchedulingPolicy`, `CreateDryRunOrNoopSchedulingPolicy`) are declared in `gpu/scheduling_policy.h`, and the shared do-nothing policy in `gpu/noop_scheduling_policy.h`.

**`scheduling_policy_detail.h`** — Classic policy implementation.
- `TSchedulingPolicy` — concrete implementation. On each heartbeat it builds a `TScheduleAllocationsContext` and runs two sets of stages:
  - Regular stages (`EAllocationSchedulingStage`): `RegularHighPriority`, `RegularMediumPriority`, `RegularPackingFallback`.
  - Preemptive stages: `PreemptiveNormal`, `PreemptiveAggressive`, `PreemptiveSsdNormal`, `PreemptiveSsdAggressive`, `PreemptiveDefaultGpuFullHost`. The concrete preemptive stage list is built per-heartbeat by `BuildPreemptiveSchedulingStageList`, each element a `{stage, TPreemptiveSchedulingParameters}` pair (target operation preemption priority + minimum allocation preemption level + optional force flag). Regular stages take `TRegularSchedulingParameters` (considered operations, custom min-spare resources, ignore-packing flag, one-allocation-only flag).
  - `SchedulingSegmentsManagementExecutor_` runs `TSchedulingSegmentManager` on a separate periodic schedule to rebalance nodes between segments; `MinNodeResourceLimitsCheckExecutor_` triggers warnings on undersized nodes.
  - Graceful preemption (`PreemptAllocationsGracefully`) runs before the scheduling stages.
  - Per-node state is sharded by node-shard id via `std::array<TNodeStateShard, MaxNodeShardCount>` to avoid contention; each shard is cache-line aligned and owns its own `TNodeStateMap` and last-preemptive-scheduling-time map.
- `TScheduleAllocationsContext` — per-heartbeat driver. Owns the heartbeat context, captured tree snapshot, SSD / default-GPU full-host toggles, and scheduling statistics. Key methods:
  - `PrepareForScheduling()` — snapshots dynamic attributes from the snapshot-embedded list.
  - `PrescheduleAllocation(consideredOps, targetPriority)` — traverses the tree top-down, marking active elements and initializing `TSchedulableChildSet`s.
  - `ShouldContinueScheduling(customMinSpare?)` — cheap per-iteration guard.
  - `FairShareScheduleAllocation(...)` pops the best operation from the root's child heap, checks resource availability, does the packing check, and issues `ScheduleAllocation` to the controller.
  - Preemption: `AnalyzePreemptibleAllocations` / `PreemptAllocationsAfterScheduling` identify running allocations that can be preempted for higher-priority operations. (The class has a standing TODO: it is large and overloaded, and should be refactored.)
- `TDynamicAttributesManager` — maintains the `TDynamicAttributesList` (flat array indexed by tree index). Entry points: `BuildDynamicAttributesListFromSnapshot(...)` (static; builds an initial list from a resource-usage snapshot), `InitializeAttributesAt{CompositeElement,Operation}`, `ActivateOperation` / `DeactivateOperation`, `UpdateOperationResourceUsage`. Hierarchical deltas are propagated via `UpdateAttributesHierarchically`; child heaps are kept consistent through `UpdateAttributesAtCompositeElement`.
- `TSchedulableChildSet` — ordered view of a composite element's schedulable children. Uses a max-heap for pools with many children and a linear scan for small ones; ordering is by `SatisfactionRatio` (lower = more deserving). `GetBestActiveChild()` returns the head.
- `TDynamicAttributes` — per-element attributes updated during a heartbeat: `ResourceUsage` (+ update timestamp), `Alive`, `LocalSatisfactionRatio` (precomputed in snapshot), `SatisfactionRatio`, `Active`, `BestLeafDescendant`, optional `SchedulableChildSet` (for pools), and the element's index inside its parent's child set.
- `TDynamicAttributesListSnapshot` — wraps a `TDynamicAttributesList` for atomic publication; stored inside `TPoolTreeSnapshotStateImpl`.
- `TPostUpdateContextImpl` — classic-policy post-update output carried across phases: `RootElement`, SSD priority preemption media, `SchedulableOperationsPerPriority`, `StaticAttributesList`, `OperationIdToState` / `OperationIdToSharedState`, known scheduling-tag filters, `OperationCountsByPreemptionPriorityParameters`. Later consumed by `CreateSnapshotState` to produce a `TPoolTreeSnapshotStateImpl`.
- `TSchedulingStageProfilingCounters` — per-stage timing / counters (preschedule time, controller schedule time, packing time, analyze time, failure reasons, deactivation reasons, scheduling-index histograms, stage-attempt indices, active-tree/operation summaries).
- `TAllocationWithPreemptionInfo` — `(allocation, status, operationElement)` value + custom `THash`; used during preemption analysis.

**`pool_tree_snapshot_state.h`** — Immutable scheduling state bundled with the tree snapshot (classic policy).
- `TPoolTreeSnapshotStateImpl : TPoolTreeSnapshotState` — holds `StaticAttributesList`, `SchedulableOperationsPerPriority`, `SsdPriorityPreemptionMedia`, `CachedAllocationPreemptionStatuses`, `KnownSchedulingTagFilters`, `OperationCountsByPreemptionPriorityParameters`, plus owning maps `OperationIdToState_` / `OperationIdToSharedState_`. Also stores a mutable `TAtomicIntrusivePtr<TDynamicAttributesListSnapshot>` which is the only non-const part — refreshed between fair-share updates by `OnResourceUsageSnapshotUpdate`.
- `GetOperationState` / `GetOperationSharedState` — general lookup (hashmap). `GetEnabledOperationState` / `GetEnabledOperationSharedState` — fast O(1) lookup via the element's tree index; valid only for enabled operations.
- `TStaticAttributes` — per-element snapshot-time attributes: `SchedulingIndex`, `SchedulingTagFilterIndex`, `EffectiveAggressivePreemptionAllowed`, `EffectivePrioritySchedulingSegmentModuleAssignmentEnabled`, `IsAliveAtUpdate`; operation-only: `SchedulingPriority` (`EOperationSchedulingPriority`), `OperationState`, `OperationSharedState`, `AreRegularAllocationsOnSsdNodesAllowed`.
- `TStaticAttributesList` — final `std::vector<TStaticAttributes>` with `AttributesOf(element)` tree-index accessor.

**`attributes_list.h`** — `TAttributesList<TAttributes>` — generic element-indexed attribute list backed by `std::vector<TAttributes>`, with `AttributesOf(element)` access and range iteration.

**`operation_shared_state.h`** — Mutable per-operation state accessed concurrently from multiple node shards.
- `TOperationSharedState` — Tracks all running allocations for one operation, bucketed by `EAllocationPreemptionStatus` (`NonPreemptible` / `AggressivelyPreemptible` / `Preemptible`). Preemption status is recomputed against the published fair share (`PublishFairShare`) via `UpdatePreemptibleAllocationsList`, periodically throttled with `UpdateStateShardsBackoff_`. Diagnostic counters (deactivation reasons, schedule-allocation attempts, min-needed-resources unsatisfied counts, preemption-status statistics) use per-node-shard state shards (cache-line padded) and are periodically consolidated for Orchid / profiling.
- Main operations: `OnAllocationStarted`, `OnAllocationFinished`, `ProcessAllocationUpdate`, `ProcessAllocationPreemption`, `Disable` / `Enable`, `SetPreemptible`, `GetTotalDiskQuota`. Counters: `GetRunningAllocationCount`, `GetPreemptibleAllocationCount`, `GetAggressivelyPreemptibleAllocationCount`, `GetAllocationPreemptionStatus`, `GetAllocationPreemptionStatusMap`, `GetLastScheduleAllocationSuccessTime`.
- Packing statistics: `RecordPackingHeartbeat` / `CheckPacking` — forwarded to an inner `TPackingStatistics`.

**`structs.h`** — Shared data structures.
- `TRunningAllocationStatistics` — totals of preemptible / total CPU·s and GPU·s for one node.
- `TAllocationState` (YSON-serializable lite) — lightweight record per allocation: `OperationId`, `ResourceLimits`, `PreemptionStatus`.
- `TNodeState` — per-exec-node state: `Descriptor`, `SchedulingSegment` (+ specified override), `LastPreemptive`/`LastNonPreemptiveHeartbeatStatistics`, `RunningAllocationStatistics` (+ update timestamp + force flag), running allocations map.
- `TOperationState` — per-operation state: spec pointer, gang flag, `AggregatedInitialMinNeededResources`, single-allocation-vanilla flag, `SchedulingSegment` / `SchedulingSegmentModule`, `NetworkPriority`, optional specified module set, and timestamps tracking failures to schedule at / assign to a module.

**`scheduling_segment_manager.h`** — Scheduling segment balancer (classic policy only).
- `TSchedulingSegmentManager` — periodically rebalances exec nodes across *scheduling segments* (e.g., `large-gpu`, `default`) so that each segment gets its fair share of nodes. Supports module-aware assignment (data-center or InfiniBand cluster granularity). Assigns operations to modules based on their demand relative to module capacity; may preempt lower-priority operations from a module to make room for high-priority ones.
- `TUpdateSchedulingSegmentsContext` — carries state for one rebalancing cycle: current/fair resource amounts and fair share per segment, per-module capacities, operation/node state copies, resulting node moves, and the persistent-state blob produced alongside.
- `TNodeMovePenalty` — two-level penalty (`PriorityPenalty` / `RegularPenalty`) used to order which nodes are cheapest to move between segments. Also sums via `operator+=`.
- `TSetNodeSchedulingSegmentOptions`, `TNodeWithMovePenalty` — small carriers for individual moves and move candidates.

**`packing.h`** — Allocation packing heuristic.
- `TPackingNodeResourcesSnapshot` — `(usage, limits, diskQuota)` with a `Free()` accessor.
- `TPackingHeartbeatSnapshot` — `(cpu instant, node resources snapshot)`. `CanSchedule` checks resource feasibility given the snapshot.
- `TPackingStatistics` — sliding window (`std::deque<TPackingHeartbeatSnapshot>`, spin-lock-protected). `RecordHeartbeat` appends and `CheckPacking` returns false (block scheduling) if the current node doesn't look like a good fit based on recent history. `CreateHeartbeatSnapshot(context)` is a free function that builds a snapshot from an `ISchedulingHeartbeatContext`.

**`persistent_state.h`** — Persistent scheduling segment state.
- `TPersistentNodeSchedulingSegmentState` / `TPersistentOperationSchedulingSegmentState` — per-node / per-operation blobs (segment, module, address for diagnostics). Non-YSON-struct so both YTree and pull-parser deserializations are hand-written.
- `TPersistentSchedulingSegmentsState` (YSON struct) — wraps the two maps.
- `TPersistentState` (YSON struct) — wraps the segments state. Serialized as the opaque `SchedulingPolicyState` blob in the strategy-level `TPersistentTreeState`.

**`scheduling_heartbeat_context.h`** — Per-heartbeat context interface.
- `ISchedulingHeartbeatContext` — interface that policy code uses to query node resources, issue allocations, and record statistics: resource / disk accessors; discount bookkeeping for second-chance scheduling (`ResetDiscount` / `IncreaseDiscount` / `GetDiscount` / `GetNodeFreeResources{Without,With}Discount`); allocation list accessors; feasibility checks (`CanStartAllocation`, `CanStartMoreAllocations(customMinSpare?)`, `CanSchedule(filter)`, `ShouldAbortAllocationsSinceResourcesOvercommit`); mutators (`StartAllocation(...)`, `PreemptAllocation(...)`); scheduling statistics (`Get`/`SetSchedulingStatistics`); exec-duration estimate passing (`StoreScheduleAllocationExecDurationEstimate`/`Extract…`); heartbeat-timeout flag. Implemented by `strategy/scheduling_heartbeat_context_detail.h::TSchedulingHeartbeatContextBase`.
- `TScheduleAllocationsStatistics` — per-heartbeat summary: controller schedule-allocation counters, attempts per stage, max non-preemptive scheduling index, preemption statistics, resource limits/usage/discount, SSD priority preemption state.
- `TStartedAllocation` / `TPreemptedAllocation` — value structs recording what the heartbeat decided.

**Cross-cutting enums** (`public.h`):
- `EAllocationPreemptionStatus` — `NonPreemptible` / `AggressivelyPreemptible` / `Preemptible`.
- `EAllocationSchedulingStage` — the 8 stages listed above.
- `EOperationPreemptionPriority` — `None` / `Normal` / `Aggressive` / `SsdNormal` / `SsdAggressive` / `DefaultGpuFullHost`.
- `EAllocationPreemptionReason` — wide enum covering regular and special-case preemptions (normal/aggressive/SSD/default-GPU-full-host, graceful, resource overcommit or limits violation, incompatible scheduling segment, eviction from module, node unschedulable, operation unregistered, unexpected allocation).
- `EAllocationUpdateStatus` — `Updated` / `Disabled` / `Unexpected` / `Preempted`.
- `FullHostGpuAllocationGpuDemand = 8` — threshold for classifying an allocation as full-host GPU.

**Notes:**
- **State sharding.** `TSchedulingPolicy::TNodeStateShard` and the per-shard state inside `TOperationSharedState` are cache-line-aligned arrays sized by `MaxNodeShardCount`. Any metric that has to be touched on every schedule-allocation attempt lives here.
- **Tree indices.** The `TreeIndex` assigned during fair-share post-update is the key for both `TStaticAttributesList::AttributesOf(element)` and the dynamic attributes. The index is invalidated on tree restructure, so these attributes must be rebuilt after any structural change.
- **Dynamic attributes stability.** `TPoolTreeSnapshotStateImpl` owns an atomically-published `TDynamicAttributesListSnapshot`; it is refreshed from `TResourceUsageSnapshot` via `OnResourceUsageSnapshotUpdate` between full fair-share updates so that scheduling decisions see usage deltas without a full post-update cycle.
- **Scheduling index** (`TStaticAttributes::SchedulingIndex`) is assigned in post-update and determines the preemption ordering between operations.
- **Policy-kind dispatch.** Code outside this directory calls the three policy-aware Orchid/diagnostic methods through `TPoolTreeSnapshot` facades (e.g. `treeSnapshot->BuildOperationProgress(...)`), which forward to the snapshot-state's virtual implementation. No manual `EPolicyKind` switching is required at call sites.

**See also:**
- `gpu/ARCHITECTURE.md` for the GPU-specific scheduling policy (periodic assignment planning, per-heartbeat plan application).
- `strategy/pool_tree_element.h` for the element hierarchy the policy operates on.
- `strategy/pool_tree_snapshot.h` for `TPoolTreeSnapshot`, the immutable tree snapshot passed to each heartbeat.
