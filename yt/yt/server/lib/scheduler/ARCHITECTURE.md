## `yt/yt/server/lib/scheduler` — Shared Server-Side Scheduler Types

The cross-binary protocol layer shared between the scheduler daemon (`yt/yt/server/scheduler`), the controller agent daemon (`yt/yt/server/controller_agent`), the exec-node allocation-heartbeat path (`yt/yt/server/node/exec_node`), and the scheduler simulator/tests. Everything here is server-only — no client code depends on this library. Code in both daemons reaches the same `TAllocationState`, `TExperimentConfig`, `TMeteringStatistics`, etc. through this directory, so bumping a protocol version here is usually a cross-binary change.

Namespace: `NYT::NScheduler` (with `NYT::NScheduler::NProto` for proto messages).

**Components:**

**`public.h`** — The server-side enum zoo and refcounted forward declarations.
- `EAllocationState` — `Scheduled / Waiting / Running / Finishing / Finished` (dense, used as `TEnumIndexedArray` key).
- `ESchedulerAlertType` — ~24 alerts the scheduler can raise (`UpdatePools`, `UpdateFairShare`, `UpdateArchiveVersion`, `TooFewControllerAgentsAlive`, …).
- `EOperationAlertType` — ~50 operation-level alerts (`UnusedTmpfsSpace`, `LowGpuUsage`, `MemoryOverconsumption`, `OperationTooLong`, …), with `DEFINE_ENUM_UNKNOWN_VALUE(..., Unknown)` for forward-compat.
- `EAgentToSchedulerOperationEventType` / `ESchedulerToAgentOperationEventType` — the two outbox/inbox event vocabularies flowing between the two daemons.
- `EControlQueue` — the scheduler control-thread fair-share buckets (`UserRequest`, `MasterConnector`, `StaticOrchid`, `DynamicOrchid`, `Strategy`, `EventLog`, `AgentTracker`, `NodeTracker`, `OperationsCleaner`, `GpuAssignmentPlanUpdate`, …). Keyed on an `IEnumIndexedFairShareActionQueue` in `TBootstrap`.
- `ENodeState` (`Unknown`/`Offline`/`Online`), `EPolicyKind` (`Classic`/`Gpu`), `EGpuSchedulingPolicyMode` (`Noop`/`DryRun`/`Allocating`), `EGpuSchedulingModuleType` (+ `ESchedulingSegmentModuleType` alias), `ESegmentedSchedulingMode`, `ESchedulingSegmentModuleAssignmentHeuristic`, `ESchedulingSegmentModulePreemptionHeuristic`, `EOperationPreemptionPriorityScope`, `EControllerAgentPickStrategy`, `EOperationManagementAction`.
- Constants: `MaxNodeShardCount = 64`, `CommittedAttribute`, `DefaultTreeAttributeName`, `TreeConfigAttributeName`, `IdAttributeName`, `ParentIdAttributeName`; Cypress paths `StrategyStatePath`, `OldSegmentsStatePath`, `LastMeteringLogTimePath`; profiling keys `ProfilingPoolTreeKey`, `ExeNodeProfilingPoolTreeKey`, `InfinibandClusterNameKey`; `DefaultOperationTag`.
- `TNetworkPriority = i8` typedef (the value type pushed into `TAllocationAttributes`).
- Forward declarations for every `T*Config` in `config.h` and every refcounted struct in the rest of the library.

**`config.h`** — Scheduler-side YSON configs (≈1200 lines).
- **Root:** `TSchedulerConfig` (inherits `TStrategyConfig` + `TSingletonsDynamicConfig`) — the scheduler's full dynamic config. Covers heartbeat throttling (`HardConcurrentHeartbeatLimit`, `SoftConcurrentHeartbeatLimit`, `HeartbeatProcessBackoff`, `SchedulingHeartbeatComplexityLimit`), node lifecycle (`NodeHeartbeatTimeout`, `NodeRegistrationTimeout`, `MaxOfflineNodeAge`), update cadences (`NodesAttributesUpdatePeriod`, `WatchersUpdatePeriod`, `ProfilingUpdatePeriod`, `OperationsUpdatePeriod`, `AlertsUpdatePeriod`, `AllocationsLoggingPeriod`, …), thread-pool sizes (`OrchidWorkerThreadCount`, `FairShareUpdateThreadCount`, `BackgroundThreadCount`, `NodeShardCount`), revival (`AllocationRevivalAbortTimeout`, `SkipOperationsWithMalformedSpecDuringRevival`, `MinRequiredArchiveVersion`), response keeper config, `ControllerAgentTracker` config, `OperationsCleaner` config, the `Experiments` map, resource-metering config, and testing options.
- **Bootstrap:** `TSchedulerBootstrapConfig`, `TSchedulerProgramConfig` — binary-level.
- **Strategy root:** `TStrategyConfig` (inherits operation-controller config + testing) — the fair-share-strategy subtree.
- **Per-tree:** `TStrategyTreeConfig` — a single pool tree's config: scheduling thresholds, preemption parameters, integral-guarantee knobs, scheduling-segments config, GPU policy config, SSD priority preemption config, default-GPU-full-host preemption config, batch operation config, pool tree template.
- **Operation throttling:** `TStrategyOperationControllerConfig` — the per-operation schedule-allocation throttling/backoff config used by `TOperationController` in `strategy/`. `TStrategyControllerThrottling` is the matching runtime helper.
- **Scheduling segments:** `TStrategySchedulingSegmentsConfig`, `TModuleShareAndNetworkPriority`.
- **GPU policy:** `TGpuSchedulingPolicyConfig` — the config read by `strategy/policy/gpu/` (see its `ARCHITECTURE.md`). Carries `TGpuSchedulingPolicyTestingOptions` (`testing_options`), a GPU-policy-only testing subconfig holding `DelayInsideProcessAllocationUpdates` (relocated here from `TTreeTestingOptions` so the classic policy keeps its no-suspension allocation-update ordering invariant unconditionally). `TTreeTestingOptions` still carries a **sync-only** counterpart, `SyncDelayInsideProcessAllocationUpdates` (a plain millisecond `TDuration`, not a `TDelayConfig`): a synchronous sleep performs no context switch, so it is legal under the classic policy's `TForbidContextSwitchGuard` and preserves the invariant. It thread-blocks the node shard to widen the revival-orphan window in tests (see `strategy/ARCHITECTURE.md` → *Revival orphan handling*).
- **Preemption specifics:** `TStrategySsdPriorityPreemptionConfig`, `TStrategyDefaultGpuFullHostPreemptionConfig`.
- **Batch:** `TBatchOperationSchedulingConfig`.
- **Templates & integral guarantees:** `TPoolTreesTemplateConfig` (apply a pool config to trees matching a filter), `TSchedulerIntegralGuaranteesConfig`.
- **Archiver:** `TOperationsCleanerConfig`, `TAliveControllerAgentThresholds` + `TControllerAgentTrackerConfig`.

**`structs.h`** — Cross-service value types.
- `TAllocationAttributes` — passed into `ScheduleAllocation`: nested `TDiskRequest`, `WaitingForResourcesOnNodeTimeout?`, `CudaToolkitVersion?`, `AllowIdleCpuPolicy`, `PortCount`, `EnableMultipleJobs`, `AllocateJobProxyRpcServerPort`, `NetworkPriority`. Has proto conversions for the controller-agent/scheduler protocol.
- `TAllocationStartDescriptor` — `(AllocationId, ResourceLimits, AllocationAttributes)`. Returned from a successful controller `ScheduleAllocation`; carried inside `TControllerScheduleAllocationResult::StartDescriptor` and consumed by the node shard to actually start the allocation.
- `TControllerScheduleAllocationResult` (refcounted) — the controller's response: `StartDescriptor?`, a `TEnumIndexedArray<EScheduleFailReason, int> Failed`, `Duration`, `NextDurationEstimate?`, `IncarnationId`, `ControllerEpoch`. Helpers: `RecordFail(reason)`, `IsBackoffNeeded()`, `IsScheduleStopNeeded()`.
- `TOperationControllerInitializeAttributes` — `BriefSpec`/`FullSpec`/`UnrecognizedSpec` YSON strings written back to the operation after `Initialize`.
- `TPoolTreeControllerSettings` + `TPoolTreeControllerSettingsMap` — per-tree settings the controller needs when an operation is registered in a tree: scheduling tag filter, tentative/probing/offloading flags, main resource, idle-CPU-policy allow-flag.
- `TPreemptedFor` — `(AllocationId, OperationId)` identifying which operation's allocation caused a given preemption.
- `TCompositeNeededResources` — `DefaultResources` plus per-tree override map; arithmetic (+/-/unary -), `VerifyNonNegative`, `Persist`, `GetNeededResourcesForTree(treeId)`. Used by the controller to report demand per pool tree.
- `TAllocationGroupResources` — `(MinNeededResources, AllocationCount)` for one allocation group (one kind of job the operation can schedule). `TAllocationGroupResourcesMap = TCompactFlatMap<string, …, 8>` keyed on allocation group name.

**`helpers.h`** — Protocol utilities.
- **Allocation id encoding:** `GenerateAllocationId(cellTag, nodeId)`, `NodeIdFromAllocationId(id)`, `EntropyFromAllocationId(id)`. Allocation ids encode the cell tag and node id in their GUID bits so the scheduler can route allocation events back without a lookup.
- `MakeOperationArtifactAcl(acl)` — computes the ACL attached to an operation's artifact dirs from the operation's own ACL.
- `ValidateGpuSchedulingModuleName(name)` — sanity-check for scheduling module names.
- **Testing delays:** `Delay(duration, EDelayType)`, `MaybeDelay(maybeDuration, ...)`, `MaybeDelay(TDelayConfigPtr, logger?)` — injects configurable sleeps at instrumented points. `EDelayType::Sync` blocks inline; `EDelayType::Async` yields via a `WaitFor`.
- `TAllocationToAbort` — `(AllocationId, optional<EAbortReason>)` carried in node heartbeat responses (`NProto::NNode::TAllocationToAbort`). Scheduler emits; node acts.

**`scheduling_tag.h`** — `TSchedulingTagFilter`.
- Wraps a `TBooleanFormula` over node tags with a cached hash; `CanSchedule(nodeTags)` evaluates, `IsEmpty()` / `GetHash()` / `GetBooleanFormula()`. Supports `&`/`|`/`!`. YSON and proto round-trip. Hashable, used as a map key in pool tree snapshots and profiling tag sets. `EmptySchedulingTagFilter` is the identity filter.

**`exec_node_descriptor.h`** — `TExecNodeDescriptor` / `TRefCountedExecNodeDescriptorMap`.
- Immutable `TRefCounted` snapshot of a live `TExecNode` passed across threads and persisted (`Persist(context)`). Fields: id, addresses, DC, IO weight, online, resource usage + limits, disk resources, tags, infiniband cluster, and a generic `IAttributeDictionaryPtr` of scheduling options. `CanSchedule(filter)` combines the filter check. `TRefCountedExecNodeDescriptorMap` is a refcounted hashmap alias — cheap to pass by pointer.

**`scheduling_segment_map.h`** — Typed segment/module storage.
- `ESchedulingSegment` (`Default`/`LargeGpu` — lives in `ytlib/scheduler`), `TSchedulingSegmentModule = std::optional<std::string>`, `IsModuleAwareSchedulingSegment(segment)`.
- `TModuleAwareValue<T>` — either a scalar value or a per-module map (controlled by `IsMultiModule_`). `Get`/`Set` / `GetAt`/`SetAt`/`MutableAt`, `GetModules()`, `GetTotal()`.
- `TSchedulingSegmentMap<T>` — `TEnumIndexedArray<ESchedulingSegment, TModuleAwareValue<T>>`. `TSegmentToResourceAmount`, `TSegmentToFairShare` are `TSchedulingSegmentMap<double>` aliases.
- Full YSON serialization on both layers (scalar-vs-multi-module branches handled in `-inl.h`).

**`job_metrics.h`** — `TJobMetrics`.
- `EJobMetricName` — the built-in aggregate metrics (`UserJobIoReads`/`Writes`/`Total`, `UserJobBytesRead`/`Written`, CPU-usage aggregates `*X100` for fixed-point, time breakdowns, `TotalTimeCompleted/Aborted`, `MainResourceConsumptionOperation{Completed,Failed,Aborted}`, volume preparation times).
- `ESummaryValueType` — `Sum`/`Min`/`Max`/`Last` — how user-defined metrics aggregate.
- `TCustomJobMetricDescription` — `(TStatisticPath, ProfilingName, SummaryValueType, JobStateFilter?)` identifying a custom metric. Hashable, YSON-serializable.
- `TJobMetrics` — `TEnumIndexedArray<EJobMetricName, i64>` + `THashMap<TCustomJobMetricDescription, i64>`. `FromJobStatistics(jobStats, controllerStats, timeStats, jobState, customMetrics, considerNonMonotonicMetrics)` builds from raw statistics. Arithmetic, `Max`, `Dominates`, `Profile(writer)`, `Persist(context)`.
- `TTreeTaggedJobMetrics = (treeId, metrics)`; `TOperationJobMetrics = vector<…>`; `TOperationIdToOperationJobMetrics = map<…>`. Flows from controller agent → scheduler → profiling.

**`event_log.h`** — Structured event log entry points.
- `ELogEventType` — the full set of events the scheduler writes to its event log: scheduler/master lifecycle, job/operation lifecycle, `FairShareInfo`, `ClusterInfo`, `NodesInfo`, `PoolsInfo`, `RuntimeParametersInfo`, `AccumulatedUsageInfo`, `OperationStarvationStarted/Finished`.
- `IEventLogHost` + `TEventLogHostBase` — an event-log-emitting mixin. `TScheduler::TImpl` inherits `TEventLogHostBase` (see `server/scheduler/ARCHITECTURE.md`); callers use `LogEventFluently(type)` to append a YSON-structured event.

**`resource_metering.h`** — `TMeteringStatistics` / `TMeteringKey` / `TMeteringMap`.
- `TMeteringStatistics` — `StrongGuaranteeResources`, `ResourceFlow`, `BurstGuaranteeResources`, `AllocatedResources`, `AccumulatedResourceUsage`. Arithmetic and `DiscountChild(child)` for subtracting a child subtree's contribution when aggregating.
- `TMeteringKey` — `(AbcId, TreeId, PoolId, meteringTags)` identifying a billable entity. Negative `AbcId` is the sentinel for pools without an ABC service (personal/experimental pools).
- `TMeteringMap = THashMap<TMeteringKey, TMeteringStatistics>` — the output of one metering pass, logged to the event log.

**`experiments.h`** — A/B experiment framework.
- `TExperimentEffectConfig` — per-group spec patches, applied at different stages:
  - Scheduler-side: `SchedulerSpecTemplatePatch` (pre-user-spec), `SchedulerSpecPatch` (post-user-spec), `SchedulerOptionsPatch`.
  - Controller-agent-side: `ControllerUserJobSpecTemplatePatch`/`Patch`, `ControllerJobIOTemplatePatch`/`Patch`, `ControllerOptionsPatch`.
  - Optional `ControllerAgentTag` restriction.
- `TExperimentGroupConfig : TExperimentEffectConfig` — adds `Fraction`.
- `TExperimentConfig` — `Ticket` (required), optional `Filter` (YP-like query over operation attributes), `Dimension` (groups from the same dimension are mutually exclusive), `Fraction`, `Groups` map, or a shorthand `AbTreatmentGroup` that synthesizes a control group.
- `TExperimentAssignment` — the finalized `(experiment, group)` record stored on each operation.
- `TExperimentAssigner` — matches an operation against experiments and draws a random group.
- `ValidateExperiments(experiments)` — checks fraction invariants (per-dimension totals ≤ 1.0 ± 1e-6, group fractions per experiment = 1.0 unless `AbTreatmentGroup`).

**`transactions.h`** — Operation transaction bookkeeping.
- `TRichTransactionId` — `(Id, ParentId, Cluster)` — a transaction id tagged with its owning cluster, for remote-copy inputs. `operator<=>`, hash, YSON, proto.
- `TControllerTransactionIds` — the six transactions every operation carries (`AsyncId`, `InputId`, `OutputId`, `DebugId`, `OutputCompletionId`, `DebugCompletionId`) plus a vector `InputIds` of per-cluster remote inputs. `ToCypressAttributes` / `FromCypressAttributes` read/write to a `IAttributeDictionary` (Cypress round-trip). `AttributeKeys` is the list of Cypress keys it occupies.
- `TOperationTransactions` — the live `ITransactionPtr` counterpart; keep the field order aligned with `TControllerTransactionIds`.
- `AttachControllerTransactions(callback, ids)` — materialize `TOperationTransactions` from ids using a caller-supplied attach callback (which knows which native client to call per cluster).

**`message_queue.h`** / **`message_queue-inl.h`** — Outbox/inbox templates.
- `TMessageQueueOutbox<TItem>` (refcounted) — producer side. `Enqueue(item)` / `Enqueue(items)` from any thread (uses an `TMpscStack` and a ring queue, protected per-invoker). `BuildOutcoming(protoMessage, itemBuilder, itemLimit?)` drains on the owning invoker and emits into a proto message; `HandleStatus(protoStatus)` acknowledges how much the peer has consumed. Optional per-item `TTraceContextPtr` propagation if `supportTracing=true`.
- `TMessageQueueInbox` — consumer side. `HandleIncoming(protoMessage, itemConsumer)` deserializes and calls the consumer for each item in order, tracking `NextExpectedItemId_`. `ReportStatus(statusMessage)` tells the peer the next id it expects.
- `TMessageQueueItemId = i64` is the sequence number. These are the primitives behind the scheduler ↔ agent outbox/inbox pattern described in `yt/yt/server/scheduler/ARCHITECTURE.md`.

**`allocation_tracker_service_proxy.h`** — `TAllocationTrackerServiceProxy` (`AllocationTrackerService`). One RPC: `Heartbeat`. Used by exec nodes to stream allocation state updates to the scheduler.

**`controller_agent_tracker_service_proxy.h`** — `TControllerAgentTrackerServiceProxy` (`ControllerAgentTrackerService`, protocol version 29). RPCs: `Handshake`, `Heartbeat`, `ScheduleAllocationHeartbeat`. Used by controller agents to register and stream events. The protocol-version number is the cross-binary lock: bump when wire formats or enum domains change in either direction.

**`proto/`** — Just the two RPC proto files above: `allocation_tracker_service.proto`, `controller_agent_tracker_service.proto`. All other proto messages (resources, schedule-allocation, patches, …) come from `yt/yt/ytlib/scheduler` or `yt/yt/ytlib/controller_agent`.

**Notes:**
- **Protocol version is load-bearing.** `TControllerAgentTrackerServiceProxy` pins `.SetProtocolVersion(29)`; adding or removing fields in shared messages, or adding entries to shared enums such as `EAbortReason` / `EAllocationState` / `EOperationAlertType` / `ESchedulerToAgentOperationEventType`, is a **cross-binary** change and must bump this version (see the EAbortReason comment in `yt/yt/client/scheduler/public.h`). Rolling the scheduler and controller agent out of lockstep without bumping will produce silent mis-parses.
- **Allocation id encoding** (helpers.h) is a three-way contract between the scheduler, the node, and the agent — the node derives the scheduler's cell tag and its own node id from any allocation id in an event, without a separate lookup. Changing the bit layout breaks all three.
- **Message queues are at-least-once-ish**: the outbox holds items until the peer reports having processed past them in `HandleStatus`. If the peer crashes and reconnects with an earlier "next expected" id, the outbox replays. Consumers must be idempotent (see e.g. `AssignmentHandler::RemoveAssignment(checkForExistence=true)` in GPU policy).
- **Experiment `Dimension`** — orthogonality across dimensions is by construction (each is sampled independently). Within a dimension, `ValidateExperiments` enforces total fractions ≤ 1.0.
- **`TExecNodeDescriptor`** is immutable by convention, not by compiler enforcement (it's a struct with public fields). Every handler that receives a descriptor assumes it will not change; build a new one rather than mutating a shared snapshot.
- **`TControllerTransactionIds` vs `TOperationTransactions`**: keep the two field lists aligned. The former is the serializable id form used on the wire and in Cypress; the latter is the live-client form. `AttachControllerTransactions` is the one place where they are reconciled, and it has to handle per-cluster `InputIds` correctly (remote copy).

**See also:** `yt/yt/client/scheduler` (public vocabulary), `yt/yt/ytlib/scheduler` (server-tier shared types built on top of this library and client types), `yt/yt/server/scheduler` + `yt/yt/server/controller_agent` (the two primary consumers).
