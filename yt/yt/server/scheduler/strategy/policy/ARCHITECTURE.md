## `yt/yt/server/scheduler/strategy/policy` — Scheduling Policies

Per-pool-tree scheduling policies: allocates resources to operations on individual node heartbeats (classic policy) or via periodic assignment planning (GPU policy). See `gpu/ARCHITECTURE.md` for the GPU-specific policy. Everything outside `gpu/` reaches the GPU policy only through `gpu/scheduling_policy.h`.

Namespace: `NYT::NScheduler::NStrategy::NPolicy` (the GPU policy lives under the nested `NGpu`).

**Design overview.**

Each pool tree has exactly one primary `ISchedulingPolicy`. The choice between the two implementations is made upstream in `pool_tree.cpp` based on `config->PolicyKind` (`EPolicyKind::Classic` or `EPolicyKind::Gpu`), which dispatches to either `CreateSchedulingPolicy` (this directory) or `NGpu::CreateAllocatingSchedulingPolicy`. Both implementations satisfy the same `ISchedulingPolicy` interface but differ in how they use node heartbeats:
- **Classic policy** (`TSchedulingPolicy`): on each node heartbeat, runs multiple regular + preemptive scheduling stages to greedily pick the best operation and call the controller to start allocations.
- **GPU policy** (`NGpu::TSchedulingPolicy`): uses heartbeats only to apply a pre-computed assignment plan; planning is decoupled into a periodic executor.

Because the snapshot-embedded scheduling state must stay opaque to code that operates generically (e.g. `pool_tree.cpp`), both `TPostUpdateContext` and `TPoolTreeSnapshotState` are abstract bases tagged with a `const EPolicyKind`. `TPoolTreeSnapshotState` declares pure-virtual `CheckIsOperationStuck`, `BuildOperationProgress`, and `BuildElementYson`; each policy implements them on its own state subclass, and `TPoolTreeSnapshot` exposes thin facades that forward to them, so call sites need neither the policy pointer nor a downcast.

**Components:**

**`scheduling_policy.h`** — The main policy interface.
- `ISchedulingPolicy` — pure-virtual. Method groups:
  - *Lifecycle*: `Initialize`, `UpdateConfig(config)`.
  - *Nodes*: `RegisterNode(nodeId, address)`, `UnregisterNode(nodeId)`.
  - *Heartbeat*: `ProcessSchedulingHeartbeat(context, treeSnapshot, skipScheduleAllocations)`. Thread affinity: any.
  - *Operations*: `RegisterOperation`, `UnregisterOperation`, `OnOperationMaterialized`, `CheckOperationSchedulingInSeveralTreesAllowed`, `EnableOperation`, `DisableOperation(element, markAsNonAlive)`.
  - *Allocations*: `RegisterAllocationsFromRevivedOperation`; `ProcessAllocationUpdates(treeSnapshot, allocationUpdates) → TFuture<std::vector<TProcessAllocationUpdateResult>>`, the batch entry point. The classic policy applies the whole batch synchronously and returns an already-set future; the GPU policy dispatches it to the control invoker and returns a pending one. Results are index-aligned with the input, and each carries a status plus optional postpone / abort instructions: `Updated` is applied; `Disabled` (operation not enabled) is postponed until the operation re-enables; `Unexpected` (allocation unknown to the current state, e.g. a stale update replayed across a disable/revive) is **dropped**, not postponed. The worker runs under a `TForbidContextSwitchGuard`, so a batch is never suspended midway, but under the GPU policy several batches can be in flight at once (see `strategy/ARCHITECTURE.md` → *Allocation updates*).
  - *Post-update*: `CreatePostUpdateContext(root)`, `PostUpdate(...)`, `CreateSnapshotState(...)`; `OnResourceUsageSnapshotUpdate(treeSnapshot, usageSnapshot)` runs between full updates to refresh dynamic attributes.
  - *Diagnostics / profiling*: scheduling attribute and logging-tag builders for nodes, allocations and elements, `PopulateOrchidService`, `ProfileOperation`.
  - *Persistence*: `InitPersistentState(node)` / `BuildPersistentState() → INodePtr`. The serialized node is embedded in the strategy-level `TPersistentTreeState`.
- `ISchedulingPolicyHost` — tiny host interface (`GetTreeSnapshot`). Kept as a weak pointer inside `TSchedulingPolicy` because the owning tree can outlive async actions. Flagged for removal.
- `TPostUpdateContext` / `TPoolTreeSnapshotState` — abstract bases, each carrying the `EPolicyKind` tag.
- `CreateSchedulingPolicy(...)` — factory for the **classic** policy only; the GPU variants (`CreateAllocatingSchedulingPolicy`, `CreateDryRunOrNoopSchedulingPolicy`) are declared in `gpu/scheduling_policy.h`, and the shared do-nothing policy in `gpu/noop_scheduling_policy.h`.

**`scheduling_policy_detail.h`** — Classic policy implementation.
- `TSchedulingPolicy` — concrete implementation. On each heartbeat it builds a `TScheduleAllocationsContext`, runs graceful preemption (`PreemptAllocationsGracefully`), then the regular stages (`RegularHighPriority`, `RegularMediumPriority`, `RegularPackingFallback`) and the preemptive stages (`PreemptiveNormal`, `PreemptiveAggressive`, `PreemptiveSsdNormal`, `PreemptiveSsdAggressive`, `PreemptiveDefaultGpuFullHost`), the latter list built per heartbeat with per-stage `TPreemptiveSchedulingParameters`. `SchedulingSegmentsManagementExecutor_` runs `TSchedulingSegmentManager` on a separate periodic schedule to rebalance nodes between segments; `MinNodeResourceLimitsCheckExecutor_` warns about undersized nodes. Per-node state is sharded by node-shard id in cache-line-aligned shards to avoid contention.
- `TScheduleAllocationsContext` — per-heartbeat driver. Owns the heartbeat context, the captured tree snapshot, SSD / default-GPU full-host toggles, and scheduling statistics. It prepares dynamic attributes from the snapshot, preschedules (traverses the tree top-down marking active elements and initializing `TSchedulableChildSet`s), pops the best operation from the root's child heap, checks resource availability and packing, and issues `ScheduleAllocation` to the controller; `AnalyzePreemptibleAllocations` / `PreemptAllocationsAfterScheduling` identify running allocations that can be preempted for higher-priority operations. (The class has a standing TODO: it is large and overloaded, and should be refactored.)
- `TDynamicAttributesManager` — maintains the `TDynamicAttributesList` (flat array indexed by tree index): builds the initial list from a resource-usage snapshot, activates / deactivates operations, applies usage updates, and propagates hierarchical deltas while keeping child heaps consistent.
- `TSchedulableChildSet` — ordered view of a composite element's schedulable children. Uses a max-heap for pools with many children and a linear scan for small ones; ordering is by `SatisfactionRatio` (lower = more deserving).
- `TDynamicAttributes` — per-element attributes updated during a heartbeat: resource usage, liveness, satisfaction ratios, activity, best leaf descendant, and the optional child set for pools.
- `TDynamicAttributesListSnapshot` — wraps a `TDynamicAttributesList` for atomic publication; stored inside `TPoolTreeSnapshotStateImpl`.
- `TPostUpdateContextImpl` — classic-policy post-update output carried across phases (root element, SSD priority preemption media, schedulable operations per priority, static attributes, operation states, known scheduling-tag filters); consumed by `CreateSnapshotState` to produce a `TPoolTreeSnapshotStateImpl`.
- `TSchedulingStageProfilingCounters` — per-stage timing and counters.
- `TAllocationWithPreemptionInfo` — `(allocation, status, operationElement)` value used during preemption analysis.

**`pool_tree_snapshot_state.h`** — Immutable scheduling state bundled with the tree snapshot (classic policy).
- `TPoolTreeSnapshotStateImpl : TPoolTreeSnapshotState` — holds the static attributes list, schedulable operations per priority, SSD priority preemption media, cached allocation preemption statuses, known scheduling-tag filters, and the owning maps of operation states and shared states. Its only mutable part is an atomically published `TDynamicAttributesListSnapshot`, refreshed between fair-share updates by `OnResourceUsageSnapshotUpdate`.
- `GetOperationState` / `GetOperationSharedState` — general lookup (hashmap). `GetEnabledOperationState` / `GetEnabledOperationSharedState` — fast O(1) lookup via the element's tree index; valid only for enabled operations.
- `TStaticAttributes` / `TStaticAttributesList` — per-element snapshot-time attributes (scheduling index, tag filter index, effective preemption and module-assignment flags, liveness; for operations also scheduling priority and the state pointers), accessed by tree index.

**`attributes_list.h`** — `TAttributesList<TAttributes>` — generic element-indexed attribute list backed by `std::vector<TAttributes>`, with `AttributesOf(element)` access and range iteration.

**`operation_shared_state.h`** — Mutable per-operation state accessed concurrently from multiple node shards.
- `TOperationSharedState` — tracks all running allocations for one operation, bucketed by `EAllocationPreemptionStatus` (`NonPreemptible` / `AggressivelyPreemptible` / `Preemptible`). Preemption status is recomputed against the published fair share via `UpdatePreemptibleAllocationsList`, periodically throttled. Main operations: `OnAllocationStarted`, `OnAllocationFinished`, `ProcessAllocationUpdate`, `ProcessAllocationPreemption`, `Disable` / `Enable`, `SetPreemptible`. Diagnostic counters use per-node-shard state shards (cache-line padded) and are periodically consolidated for Orchid / profiling.
- Packing statistics: `RecordPackingHeartbeat` / `CheckPacking` — forwarded to an inner `TPackingStatistics`.

**`structs.h`** — Shared data structures.
- `TRunningAllocationStatistics` — totals of preemptible / total CPU·s and GPU·s for one node.
- `TAllocationState` (YSON-serializable lite) — lightweight record per allocation: `OperationId`, `ResourceLimits`, `PreemptionStatus`.
- `TNodeState` — per-exec-node state: descriptor, scheduling segment (+ specified override), last heartbeat statistics, running allocation statistics and the running allocations map.
- `TOperationState` — per-operation state: spec pointer, gang flag, aggregated initial min needed resources, single-allocation-vanilla flag, scheduling segment and module, network priority, optional specified module set, and timestamps tracking failures to schedule at / assign to a module.

**`scheduling_segment_manager.h`** — Scheduling segment balancer (classic policy only).
- `TSchedulingSegmentManager` — periodically rebalances exec nodes across *scheduling segments* (e.g., `large-gpu`, `default`) so that each segment gets its fair share of nodes. Supports module-aware assignment (data-center or InfiniBand cluster granularity). Assigns operations to modules based on their demand relative to module capacity; may preempt lower-priority operations from a module to make room for high-priority ones.
- `TUpdateSchedulingSegmentsContext` — carries state for one rebalancing cycle: current/fair resource amounts and fair share per segment, per-module capacities, operation/node state copies, resulting node moves, and the persistent-state blob produced alongside.
- `TNodeMovePenalty` — two-level penalty (`PriorityPenalty` / `RegularPenalty`) used to order which nodes are cheapest to move between segments.

**`packing.h`** — Allocation packing heuristic.
- `TPackingStatistics` — a spin-lock-protected sliding window of `TPackingHeartbeatSnapshot`s (cpu instant + node resources snapshot). `RecordHeartbeat` appends and `CheckPacking` returns false (block scheduling) if the current node doesn't look like a good fit based on recent history.

**`persistent_state.h`** — Persistent scheduling segment state.
- `TPersistentNodeSchedulingSegmentState` / `TPersistentOperationSchedulingSegmentState` — per-node / per-operation blobs (segment, module, address for diagnostics). Non-YSON-struct so both YTree and pull-parser deserializations are hand-written.
- `TPersistentState` (YSON struct) — wraps the segments state. Serialized as the opaque `SchedulingPolicyState` blob in the strategy-level `TPersistentTreeState`.

**`scheduling_heartbeat_context.h`** — Per-heartbeat context interface.
- `ISchedulingHeartbeatContext` — interface that policy code uses to query node resources (with discount bookkeeping for second-chance scheduling), check feasibility (`CanStartAllocation`, `CanStartMoreAllocations`, `CanSchedule(filter)`, overcommit detection), issue `StartAllocation` / `PreemptAllocation`, and record scheduling statistics. Implemented by `strategy/scheduling_heartbeat_context_detail.h::TSchedulingHeartbeatContextBase`.
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
