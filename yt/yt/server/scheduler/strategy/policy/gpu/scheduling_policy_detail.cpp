#include "scheduling_policy_detail.h"

#include "helpers.h"

#include "pool_tree_snapshot_state.h"

#include <yt/yt/server/scheduler/strategy/policy/pool_tree_snapshot_state.h>

#include <yt/yt/server/scheduler/common/exec_node.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GetNodeModule(
    const std::optional<std::string>& nodeDataCenter,
    const std::optional<std::string>& nodeInfinibandCluster,
    ESchedulingSegmentModuleType moduleType)
{
    switch (moduleType) {
        case ESchedulingSegmentModuleType::DataCenter:
            return nodeDataCenter;
        case ESchedulingSegmentModuleType::InfinibandCluster:
            return nodeInfinibandCluster;
    }
}

////////////////////////////////////////////////////////////////////////////////

TAllocationInfoMap CollectRunningAllocationInfos(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    const TPoolTreeSnapshotPtr& treeSnapshot)
{
    TAllocationInfoMap runningAllocationInfos;

    runningAllocationInfos.reserve(std::size(schedulingHeartbeatContext->RunningAllocations()));

    for (const auto& allocation : schedulingHeartbeatContext->RunningAllocations()) {
        runningAllocationInfos.emplace(
            allocation->GetId(),
            TAllocationInfo{
                .Allocation = allocation,
                .OperationElement = treeSnapshot->FindEnabledOperationElement(allocation->GetOperationId()),
            });
    }

    return runningAllocationInfos;
}

////////////////////////////////////////////////////////////////////////////////

TGpuScheduleAllocationsStatisticsPtr GetScheduleAllocationsStatistics(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext)
{
    auto statistics = DynamicPointerCast<TGpuScheduleAllocationsStatistics>(schedulingHeartbeatContext->GetSchedulingStatistics());
    YT_VERIFY(statistics);

    return statistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TModuleProfilingCounters::TModuleProfilingCounters(const NProfiling::TProfiler& profiler)
    : TotalModuleNodes(profiler.Gauge("/total_nodes_count"))
    , ModuleUnreservedNodes(profiler.Gauge("/unreserved_nodes_count"))
    , ModuleFullHostModuleBoundOperations(profiler.Gauge("/full_host_module_bound_operations_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

TGpuSchedulingProfilingCounters::TGpuSchedulingProfilingCounters(const NProfiling::TProfiler& profiler)
    : PlannedAssignments(profiler.Counter("/planned_assignments_count"))
    , PreemptedAssignments(profiler.Counter("/preempted_assignments_count"))
    , Assignments(profiler.Gauge("/assignments_count"))
    , TotalPlanningTime(profiler.Timer("/total_planning_time"))
    , OperationResourcesUpdateTime(profiler.Timer("/operation_resources_update_time"))
    , FullHostPlanningTime(profiler.Timer("/full_host_planning_time"))
    , RegularPlanningTime(profiler.Timer("/regular_planning_time"))
    , ExtraPlanningTime(profiler.Timer("/extra_planning_time"))
    , EnabledOperations(profiler.Gauge("/enabled_operations_count"))
    , FullHostModuleBoundOperations(profiler.Gauge("/full_host_module_bound_operations_count"))
    , AssignedGpu(profiler.Gauge("/assigned_gpu_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

TSchedulingPolicy::TSchedulingPolicy(
    TWeakPtr<ISchedulingPolicyHost> host,
    IStrategyHost* strategyHost,
    const std::string& treeId,
    TGpuSchedulingPolicyConfigPtr config,
    NProfiling::TProfiler profiler)
    : Host_(std::move(host))
    , StrategyHost_(strategyHost)
    , Logger(GetLogger(treeId))
    , Config_(std::move(config))
    , PlanUpdateExecutor_(New<TPeriodicExecutor>(
        StrategyHost_->GetControlInvoker(EControlQueue::GpuAssignmentPlanUpdate),
        BIND(&TSchedulingPolicy::UpdateAssignmentPlan, MakeWeak(this)),
        Config_->PlanUpdatePeriod))
    , AssignmentHandler_(GetLogger(treeId))
    , Profiler_(std::move(profiler))
    , ProfilingCounters_(Profiler_)
{ }

void TSchedulingPolicy::Initialize()
{
    PlanUpdateExecutor_->Start();
}

void TSchedulingPolicy::RegisterNode(TNodeId nodeId, const std::string& nodeAddress)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto node = EmplaceOrCrash(Nodes_, nodeId, New<TNode>(nodeId, nodeAddress))->second;

    ReviveNodeState(node);
    RevivePendingAllocations(node);

    YT_LOG_DEBUG("Node registered (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);
}

void TSchedulingPolicy::UnregisterNode(TNodeId nodeId)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto it = GetIteratorOrCrash(Nodes_, nodeId);
    const auto& node = it->second;
    auto nodeAddress = node->Address();

    PreemptAllNodeAssignments(
        node,
        EAllocationPreemptionReason::NodeUnschedulable,
        "Node unregistered");

    Nodes_.erase(it);

    YT_LOG_DEBUG("Node unregistered (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);
}

void TSchedulingPolicy::ProcessSchedulingHeartbeat(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    const TPoolTreeSnapshotPtr& treeSnapshot,
    bool skipScheduleAllocations)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto processSchedulingHeartbeatFuture = BIND(
        &TSchedulingPolicy::DoProcessSchedulingHeartbeat,
        MakeWeak(this),
        schedulingHeartbeatContext,
        treeSnapshot,
        skipScheduleAllocations)
        .AsyncVia(StrategyHost_->GetControlInvoker(EControlQueue::Strategy))
        .Run();

    Y_UNUSED(WaitFor(processSchedulingHeartbeatFuture));
}

void TSchedulingPolicy::RegisterOperation(const TPoolTreeOperationElement* element)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto operation = New<TOperation>(
        element->GetOperationId(),
        element->GetOperationType(),
        element->IsGang(),
        element->Spec()->SchedulingModules,
        element->GetSchedulingTagFilter());

    ReviveOperationState(operation);

    EmplaceOrCrash(DisabledOperations_, operation->GetId(), operation);

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::OperationRegistered)
        .Item("operation_id").Value(operation->GetId())
        .Item("type").Value(operation->GetType())
        .Item("gang").Value(operation->IsGang())
        .Item("specified_scheduling_modules").Value(operation->SpecifiedSchedulingModules());

    YT_LOG_DEBUG("Operation registered (OperationId: %v, OperationType: %v, Gang: %v, SpecifiedSchedulingModules: %v)",
        operation->GetId(),
        operation->GetType(),
        operation->IsGang(),
        operation->SpecifiedSchedulingModules());

    if (const auto& specifiedModules = operation->SpecifiedSchedulingModules()) {
        std::vector<std::string> unknownModules;
        for (const auto& module : *specifiedModules) {
            if (!Config_->Modules.contains(module)) {
                unknownModules.push_back(module);
            }
        }

        YT_LOG_WARNING_UNLESS(unknownModules.empty(),
            "Unknown scheduling modules specified for operation (OperationId: %v, UnknownModules: %v)",
            operation->GetId(),
            unknownModules);
    }
}

void TSchedulingPolicy::UnregisterOperation(const TPoolTreeOperationElement* element)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto it = GetIteratorOrCrash(DisabledOperations_, element->GetOperationId());
    const auto& operation = it->second;

    // NB(yaishenka): DisableOperation(true) removes all assignments before UnregisterOperation.
    // However, if DisableOperation(false) was called (e.g. controller agent failure) and the
    // operation was never re-enabled, Reviving assignments may remain.
    for (const auto& assignment : GetItems(operation->Assignments())) {
        RemoveAssignment(assignment);
    }

    DisabledOperations_.erase(it);

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::OperationUnregistered)
        .Item("operation_id").Value(element->GetOperationId());

    YT_LOG_DEBUG("Operation unregistered (OperationId: %v)", element->GetOperationId());
}

TError TSchedulingPolicy::OnOperationMaterialized(const TPoolTreeOperationElement* element)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto operation = GetOrCrash(DisabledOperations_, element->GetOperationId());
    operation->Initialize(element->GetInitialGroupedNeededResources());

    YT_LOG_DEBUG("Operation materialized (OperationId: %v, InitialGroupedNeededResources: %v)",
        operation->GetId(),
        element->GetInitialGroupedNeededResources());

    return {};
}

TError TSchedulingPolicy::CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* element) const
{
    auto operation = GetOrDefault(EnabledOperations_, element->GetOperationId());

    if (!operation) {
        return {};
    }

    if (operation->IsFullHostModuleBound() && !element->IsSingleAllocationVanillaOperation()) {
        // NB: This error will be propagated to operation's failure only if operation is launched in several trees.
        return TError(
            "Scheduling in several trees is forbidden for operations in module-aware scheduling segments, "
            "specify a single tree or use the \"schedule_in_single_tree\" spec option");
    }

    return {};
}

void TSchedulingPolicy::EnableOperation(const TPoolTreeOperationElement* element)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto operationIt = GetIteratorOrCrash(DisabledOperations_, element->GetOperationId());
    auto operation = operationIt->second;
    DisabledOperations_.erase(operationIt);

    YT_VERIFY(operation->IsInitialized());

    EmplaceOrCrash(EnabledOperations_, operation->GetId(), operation);

    operation->SetEnabled(true);

    // Strip any Reviving assignment that wasn't rescued by
    // RegisterAllocationsFromRevivedOperation (e.g. revival path didn't run because no
    // snapshot was loaded, or only some of the operation's allocations were in the snapshot).
    for (const auto& assignment : GetItems(operation->Assignments())) {
        YT_VERIFY(!IsAssignmentPreliminary(assignment));
        if (assignment->Reviving) {
            RemoveAssignment(assignment);
        }
    }

    YT_LOG_DEBUG("Operation enabled (OperationId: %v)", operation->GetId());
}

void TSchedulingPolicy::DisableOperation(TPoolTreeOperationElement* element, bool markAsNonAlive)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto operationIt = EnabledOperations_.find(element->GetOperationId());
    if (operationIt == EnabledOperations_.end()) {
        YT_LOG_DEBUG("Operation was not enabled (OperationId: %v)", element->GetOperationId());
        return;
    }

    auto operation = operationIt->second;
    EnabledOperations_.erase(operationIt);

    EmplaceOrCrash(DisabledOperations_, operation->GetId(), operation);

    operation->SetEnabled(false);

    if (markAsNonAlive) {
        // NB(yaishenka): Do not preempt - allocations are already aborted by strategy.
        RemoveAllOperationAssignments(operation);
    } else {
        // NB(yaishenka): Remove preliminary assignments (no allocation ever ran — nothing to revive).
        // Preserve non-preliminary assignments for potential revival, marking them so the
        // planner ignores them until the operation is re-enabled.
        for (const auto& assignment : GetItems(operation->Assignments())) {
            if (IsAssignmentPreliminary(assignment)) {
                RemoveAssignment(assignment);
            } else {
                assignment->Reviving = true;
            }
        }
    }

    DropPendingAllocationsForOperation(operation);
    operation->RemoveAllAllocations();
    element->ReleaseResources(markAsNonAlive);

    YT_LOG_DEBUG("Operation disabled (OperationId: %v, MarkAsNonAlive: %v)",
        operation->GetId(),
        markAsNonAlive);
}

void TSchedulingPolicy::RegisterAllocationsFromRevivedOperation(
    TPoolTreeOperationElement* element,
    std::vector<TAllocationPtr> allocations)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto operation = GetOrCrash(DisabledOperations_, element->GetOperationId());

    for (const auto& allocation : allocations) {
        ReviveAllocation(element, operation, allocation);
    }
}

void TSchedulingPolicy::ReviveAllocation(
    TPoolTreeOperationElement* element,
    const TOperationPtr& operation,
    const TAllocationPtr& allocation)
{
    auto allocationId = allocation->GetId();
    auto nodeId = allocation->GetRevivalNodeId();
    const auto& resourceUsage = allocation->ResourceUsage();

    element->IncreaseHierarchicalResourceUsage(resourceUsage);

    auto node = GetOrDefault(Nodes_, nodeId);

    if (!node) {
        auto allocationState = New<TAllocationState>(
            allocationId,
            nodeId,
            /*assignment*/ TWeakPtr<TAssignment>{},
            resourceUsage);
        operation->AddOrphanAllocation(allocationState);
        EmplaceOrCrash(
            PendingRevivedAllocations_[nodeId],
            allocationId,
            TPendingRevivedAllocation{
                .Operation = MakeWeak(operation),
                .AllocationGroupName = allocation->AllocationGroupName(),
            });

        YT_LOG_DEBUG("Allocation revived as orphan (OperationId: %v, AllocationId: %v, NodeId: %v)",
            operation->GetId(),
            allocationId,
            nodeId);
        return;
    }

    TJobResourcesWithQuota assignmentResources(resourceUsage);
    assignmentResources.DiskQuota() = allocation->DiskQuota();

    if (auto assignment = GetOrDefault(operation->AllocationIdToAssignment(), allocationId)) {
        YT_VERIFY(assignment->Reviving);
        YT_VERIFY(assignment->Node == node.Get());

        assignment->Reviving = false;
        auto allocationState = New<TAllocationState>(allocationId, nodeId, assignment, resourceUsage);
        operation->AddRevivedAllocation(allocationState, assignment);
    } else {
        auto newAssignment = New<TAssignment>(
            allocation->AllocationGroupName(),
            assignmentResources,
            operation.Get(),
            node.Get());
        operation->AddAssignment(newAssignment);
        node->AddAssignment(newAssignment);

        auto allocationState = New<TAllocationState>(allocationId, nodeId, newAssignment, resourceUsage);
        newAssignment->AddAllocation(allocationState);
    }

    YT_LOG_DEBUG(
        "Allocation revived (OperationId: %v, AllocationId: %v, NodeId: %v)",
        operation->GetId(),
        allocationId,
        nodeId);
}

TProcessAllocationUpdateResult TSchedulingPolicy::ProcessAllocationUpdate(
    const TPoolTreeSnapshotPtr& treeSnapshot,
    TPoolTreeOperationElement* element,
    const TAllocationUpdate& allocationUpdate)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(Config_->Mode == EGpuSchedulingPolicyMode::Allocating);

    auto processAllocationUpdate = BIND(
        &TSchedulingPolicy::DoProcessAllocationUpdate,
        MakeStrong(this),
        treeSnapshot,
        MakeStrong(element),
        allocationUpdate)
        .AsyncVia(StrategyHost_->GetControlInvoker(EControlQueue::Strategy))
        .Run();

    return WaitFor(processAllocationUpdate)
        .ValueOrThrow();
}

// TODO(YT-27647): Save node info by NodeShards and don't switch to control here.
void TSchedulingPolicy::BuildSchedulingAttributesStringForNode(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    TNodeId nodeId,
    TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    Y_UNUSED(WaitFor(BIND(
        &TSchedulingPolicy::DoBuildSchedulingAttributesStringForNode,
        MakeWeak(this),
        schedulingHeartbeatContext,
        nodeId,
        std::addressof(delimitedBuilder))
        .AsyncVia(StrategyHost_->GetControlInvoker(EControlQueue::Strategy))
        .Run()));
}

// TODO(YT-27647): Save node info by NodeShards and don't switch to control here.
void TSchedulingPolicy::BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    Y_UNUSED(WaitFor(BIND(
        &TSchedulingPolicy::DoBuildSchedulingAttributesForNode,
        MakeWeak(this),
        nodeId,
        fluent)
        .AsyncVia(StrategyHost_->GetControlInvoker(EControlQueue::Strategy))
        .Run()));
}

void TSchedulingPolicy::BuildSchedulingAttributesStringForOngoingAllocations(
    const TPoolTreeSnapshotPtr& treeSnapshot,
    const std::vector<TAllocationPtr>& allocations,
    TInstant now,
    TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    if (!treeSnapshot) {
        return;
    }

    const auto* state = GetPoolTreeSnapshotState(treeSnapshot);

    std::vector<TAllocationId> preemptibleAllocationIds;
    std::vector<TAllocationId> nonPreemptibleAllocationIds;
    std::vector<TAllocationId> unknownAllocationIds;
    for (const auto& allocation : allocations) {
        auto allocationId = allocation->GetId();
        auto it = state->AllocationStates().find(allocationId);
        if (it == state->AllocationStates().end()) {
            unknownAllocationIds.push_back(allocationId);
        } else if (it->second.Preemptible) {
            preemptibleAllocationIds.push_back(allocationId);
        } else {
            nonPreemptibleAllocationIds.push_back(allocationId);
        }
    }

    delimitedBuilder->AppendFormat(
        "PreemptibleAllocationIds: %v, NonPreemptibleAllocationIds: %v, "
        "UnknownAllocationIds: %v, TimeSinceSnapshotSeconds: %v",
        preemptibleAllocationIds,
        nonPreemptibleAllocationIds,
        unknownAllocationIds,
        (now - state->GetSnapshotTime()).SecondsFloat());
}

void TSchedulingPolicy::BuildElementLoggingStringAttributes(
    const TPoolTreeSnapshotPtr& treeSnapshot,
    const TPoolTreeElement* element,
    TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    if (!treeSnapshot || element->GetType() != ESchedulerElementType::Operation) {
        return;
    }

    const auto* operationElement = static_cast<const TPoolTreeOperationElement*>(element);
    const auto* state = GetPoolTreeSnapshotState(treeSnapshot);
    auto it = state->OperationStates().find(operationElement->GetOperationId());
    if (it == state->OperationStates().end()) {
        return;
    }

    const auto& operationState = it->second;
    delimitedBuilder->AppendFormat(
        "Preemptible: %v, Starving: %v, Enabled: %v, SchedulingModule: %v, "
        "RealizedAssignments: %v, PreliminaryAssignments: %v",
        operationState.Preemptible,
        operationState.Starving,
        operationState.Enabled,
        operationState.SchedulingModule,
        operationState.RealizedAssignmentCount,
        operationState.PreliminaryAssignmentCount);
}

void TSchedulingPolicy::PopulateOrchidService(const TCompositeMapServicePtr& orchidService) const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    orchidService->AddChild("gpu_assignment_plan", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
        YT_ASSERT_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::DynamicOrchid));

        BuildYsonFluently(consumer).BeginMap()
            .Item("nodes").DoMapFor(Nodes_, [] (TFluentMap fluent, const auto& item) {
                const auto& [_, node] = item;

                fluent
                    .Item(node->Address()).Value(node);
            })
            .Item("operations").DoMap([&] (TFluentMap fluent) {
                for (const auto& [operationId, operation] : EnabledOperations_) {
                    fluent
                        .Item(ToString(operationId)).Value(operation);
                }
                for (const auto& [operationId, operation] : DisabledOperations_) {
                    fluent
                        .Item(ToString(operationId)).Value(operation);
                }
            })
        .EndMap();
    })));
}

void TSchedulingPolicy::ProfileOperation(
    const TPoolTreeOperationElement* /*element*/,
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    NProfiling::ISensorWriter* /*writer*/) const
{ }

TPostUpdateContextPtr TSchedulingPolicy::CreatePostUpdateContext(TPoolTreeRootElement* /*rootElement*/)
{
    return nullptr;
}

void TSchedulingPolicy::PostUpdate(
    TFairSharePostUpdateContext* /*fairSharePostUpdateContext*/,
    TPostUpdateContextPtr* /*postUpdateContext*/)
{ }

TPoolTreeSnapshotStatePtr TSchedulingPolicy::CreateSnapshotState(TPostUpdateContextPtr* /*postUpdateContext*/)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    TOperationSnapshotStateMap operationStates;
    operationStates.reserve(EnabledOperations_.size() + DisabledOperations_.size());
    TAllocationSnapshotStateMap allocationStates;

    auto snapshotOperations = [&] (const TOperationMap& operations) {
        for (const auto& [operationId, operation] : operations) {
            for (const auto& [allocationId, allocation] : operation->AllocationIdToAllocationState()) {
                EmplaceOrCrash(allocationStates, allocationId, allocation->BuildSnapshotInfo(operationId));
            }
            EmplaceOrCrash(operationStates, operationId, operation->BuildSnapshotInfo());
        }
    };
    snapshotOperations(EnabledOperations_);
    snapshotOperations(DisabledOperations_);

    TNodeSnapshotStateMap nodeStates;
    nodeStates.reserve(Nodes_.size());
    for (const auto& [nodeId, node] : Nodes_) {
        EmplaceOrCrash(nodeStates, nodeId, node->BuildSnapshotInfo());
    }

    return New<TPoolTreeSnapshotStateImpl>(
        std::move(operationStates),
        std::move(nodeStates),
        std::move(allocationStates),
        TInstant::Now());
}

void TSchedulingPolicy::OnResourceUsageSnapshotUpdate(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const TResourceUsageSnapshotPtr& /*resourceUsageSnapshot*/) const
{ }

void TSchedulingPolicy::UpdateConfig(TStrategyTreeConfigPtr treeConfig)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    const auto& config = treeConfig->GpuSchedulingPolicy;
    if (Config_->Mode != config->Mode) {
        YT_LOG_WARNING("Scheduling policy config update failed because mode has changed (OldMode: %v, NewMode: %v)",
            Config_->Mode,
            config->Mode);
        return;
    }

    Config_ = config;

    PlanUpdateExecutor_->SetPeriod(Config_->PlanUpdatePeriod);
}

void TSchedulingPolicy::InitPersistentState(INodePtr persistentState)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (persistentState) {
        try {
            InitialPersistentState_ = ConvertTo<TPersistentStatePtr>(persistentState);
        } catch (const std::exception& ex) {
            InitialPersistentState_ = New<TPersistentState>();

            // TODO(eshcherbin): Should we set scheduler alert instead? It'll be more visible this way,
            // but it'll have to be removed manually
            YT_LOG_WARNING(ex, "Failed to deserialize GPU scheduling policy persistent state; will ignore it");
        }
    } else {
        InitialPersistentState_ = New<TPersistentState>();
    }

    auto now = TInstant::Now();
    InitializationFromPersistentStateDeadline_ = now + Config_->InitializationTimeout;

    YT_LOG_DEBUG(
        "Initialized GPU scheduling policy persistent state (InitializationFromPersistentStateDeadline: %v)",
        InitializationFromPersistentStateDeadline_);
}

INodePtr TSchedulingPolicy::BuildPersistentState() const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto persistentState = PersistentState_
        ? PersistentState_
        : InitialPersistentState_;

    return ConvertToNode(persistentState);
}

void TSchedulingPolicy::UpdateNodeDescriptor(const TNodePtr& node, TExecNodeDescriptorPtr descriptor)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    TForbidContextSwitchGuard contextSwitchGuard;

    YT_VERIFY(node->GetId() == descriptor->Id);

    YT_LOG_ALERT_UNLESS(
        node->Address() == descriptor->GetDefaultAddress(),
        "Node address differs from descriptor default address "
        "(NodeId: %v, Address: %v, DefaultAddress: %v)",
        node->GetId(),
        node->Address(),
        descriptor->GetDefaultAddress());

    bool wasSchedulable = node->IsSchedulable();

    node->SetDescriptor(std::move(descriptor));

    // TODO(eshcherbin): Rework how modules are configured.
    auto oldModule = node->SchedulingModule();
    node->SchedulingModule() = GetNodeModule(
        node->Descriptor()->DataCenter,
        node->Descriptor()->InfinibandCluster,
        Config_->ModuleType);

    if (!wasSchedulable && node->IsSchedulable()) {
        YT_LOG_DEBUG("Node has become schedulable (NodeAddress: %v, SchedulingModule: %v, ResourceLimits: %v)",
            node->Address(),
            node->SchedulingModule(),
            node->Descriptor()->ResourceLimits);
    } else if (!node->IsSchedulable() && wasSchedulable) {
        YT_LOG_DEBUG("Node has become unschedulable (NodeAddress: %v, SchedulingModule: %v)",
            node->Descriptor()->GetDefaultAddress(),
            node->SchedulingModule());

        PreemptAllNodeAssignments(
            node,
            EAllocationPreemptionReason::NodeUnschedulable,
            "Node is unschedulable");
    } else if (oldModule && oldModule != node->SchedulingModule()) {
        YT_LOG_DEBUG("Node's scheduling module has changed (NodeAddress: %v, OldModule: %v, NewModule: %v)",
            node->Descriptor()->GetDefaultAddress(),
            oldModule,
            node->SchedulingModule());

        PreemptAllNodeAssignments(
            node,
            EAllocationPreemptionReason::NodeUnschedulable,
            "Node's scheduling module has changed");
    }
}

void TSchedulingPolicy::UpdateAssignmentPlan()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&AssignmentPlanUpdateLock_))
        .ValueOrThrow();

    TForbidContextSwitchGuard contextSwitchGuard;

    if (auto now = TInstant::Now(); now <= InitializationFromPersistentStateDeadline_) {
        YT_LOG_DEBUG(
            "Skipping the update cycle during initialization (Now: %v, Deadline: %v)",
            now,
            InitializationFromPersistentStateDeadline_);

        return;
    }

    auto host = Host_.Lock();
    if (!host) {
        return;
    }

    auto treeSnapshot = host->GetTreeSnapshot();
    if (!treeSnapshot) {
        YT_LOG_DEBUG("Could not get tree snapshot, skipping the update");
        return;
    }

    TAssignmentPlanUpdateContext updateContext(
        Logger,
        EnabledOperations_,
        Nodes_,
        treeSnapshot,
        AssignmentHandler_,
        Config_->Mode);

    updateContext.UpdatePreemptionStatuses();
    updateContext.FillOperationUsage();
    updateContext.PreemptLimitViolatingOperations();

    for (const auto& [_, operation] : EnabledOperations_) {
        updateContext.UpdateOperationResources(operation);
    }

    for (const auto& [_, operation] : DisabledOperations_) {
        updateContext.ResetOperationResources(operation);
    }

    updateContext.GetStatistics()->UpdatingOperationResourcesDuration = updateContext.GetStatistics()->Timer.GetElapsedTime();

    TGpuAllocationAssignmentPlanUpdateExecutor updateExecutor(
        &updateContext,
        TInstant::Now(),
        Config_,
        Logger);
    updateExecutor.Run();

    // NB(severovv): limits might have been violated during preemptive scheduling
    updateContext.PreemptLimitViolatingOperations();

    LogSnapshotEvent(updateContext.GetStatistics());
    ProfileAssignmentPlanUpdating(updateContext.GetStatistics());
    UpdatePersistentState();
}

void TSchedulingPolicy::PreemptAllNodeAssignments(
    const TNodePtr& node,
    EAllocationPreemptionReason preemptionReason,
    const std::string& preemptionDescription)
{
    // NB(eshcherbin): Copy assignments with |GetItems|, because the set will be modified.
    for (const auto& assignment : GetItems(node->Assignments())) {
        PreemptAssignment(assignment, preemptionReason, preemptionDescription);
    }
}

void TSchedulingPolicy::PreemptAllOperationAssignments(
    const TOperationPtr& operation,
    EAllocationPreemptionReason preemptionReason,
    const std::string& preemptionDescription)
{
    for (const auto& assignment : GetItems(operation->Assignments())) {
        PreemptAssignment(assignment, preemptionReason, preemptionDescription);
    }
}

void TSchedulingPolicy::RemoveAllOperationAssignments(const TOperationPtr& operation)
{
    for (const auto& assignment : GetItems(operation->Assignments())) {
        RemoveAssignment(assignment, /*strict*/ true);
    }
}

void TSchedulingPolicy::PreemptAssignment(
    const TAssignmentPtr& assignment,
    EAllocationPreemptionReason preemptionReason,
    const std::string& preemptionDescription)
{
    AssignmentHandler_.PreemptAssignment(assignment, preemptionReason, preemptionDescription);
}

void TSchedulingPolicy::RemoveAssignment(const TAssignmentPtr& assignment, bool strict)
{
    AssignmentHandler_.RemoveAssignment(assignment, strict);
}

void TSchedulingPolicy::ReviveNodeState(const TNodePtr& node)
{
    auto maybeState = FindInitialNodePersistentState(node->GetId());
    if (!maybeState) {
        return;
    }

    if (maybeState->SchedulingModule) {
        node->SchedulingModule() = std::move(maybeState->SchedulingModule);
    }

    YT_LOG_DEBUG(
        "Node state revived "
        "(NodeId: %v, NodeAddress: %v, SchedulingModule: %v)",
        node->GetId(),
        node->Address(),
        node->SchedulingModule());
}

void TSchedulingPolicy::ReviveOperationState(TOperationPtr operation)
{
    auto maybeState = FindInitialOperationPersistentState(operation->GetId());
    if (!maybeState) {
        return;
    }

    if (maybeState->SchedulingModule) {
        operation->SchedulingModule() = std::move(maybeState->SchedulingModule);
    }
    if (maybeState->NetworkPriority) {
        operation->NetworkPriority() = std::move(maybeState->NetworkPriority);
    }

    YT_LOG_DEBUG(
        "Operation state revived "
        "(OperationId: %v, SchedulingModule: %v, NetworkPriority: %v)",
        operation->GetId(),
        operation->SchedulingModule(),
        operation->NetworkPriority());
}

void TSchedulingPolicy::RevivePendingAllocations(const TNodePtr& node)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto pendingIt = PendingRevivedAllocations_.find(node->GetId());
    if (pendingIt == PendingRevivedAllocations_.end()) {
        return;
    }

    const auto& [_, pendingAllocations] = *pendingIt;

    for (const auto& [allocationId, pendingAllocation] : pendingAllocations) {
        auto operation = pendingAllocation.Operation.Lock();
        if (!operation) {
            YT_LOG_WARNING(
                "Pending revived allocation references expired operation, skipping "
                "(AllocationId: %v, NodeId: %v)",
                allocationId,
                node->GetId());
            continue;
        }

        auto orphan = GetOrDefault(operation->AllocationIdToAllocationState(), allocationId);
        if (!orphan) {
            YT_LOG_WARNING(
                "Pending revived allocation has no orphan allocation, skipping "
                "(OperationId: %v, AllocationId: %v, NodeId: %v)",
                operation->GetId(),
                allocationId,
                node->GetId());
            continue;
        }

        YT_VERIFY(orphan->Assignment() == nullptr);

        auto currentUsage = orphan->ResourceUsage();

        // DiskQuota is not considered with non-preliminary assignments.
        auto assignment = New<TAssignment>(
            pendingAllocation.AllocationGroupName,
            TJobResourcesWithQuota(currentUsage),
            operation.Get(),
            node.Get());
        operation->AddAssignment(assignment);
        node->AddAssignment(assignment);

        orphan->SetAssignment(MakeWeak(assignment));
        assignment->AddAllocation(orphan);

        YT_LOG_DEBUG(
            "Pending revived allocation adopted "
            "(OperationId: %v, AllocationId: %v, NodeId: %v)",
            operation->GetId(),
            allocationId,
            node->GetId());
    }

    PendingRevivedAllocations_.erase(pendingIt);
}

void TSchedulingPolicy::DropPendingAllocationsForOperation(const TOperationPtr& operation)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    for (const auto& [allocationId, allocationState] : operation->AllocationIdToAllocationState()) {
        if (allocationState->Assignment() != nullptr) {
            // Not an orphan.
            continue;
        }

        RemovePendingRevivedAllocation(allocationState->GetNodeId(), allocationId);
    }
}

//! Returns false if Now > InitializationFromPersistentStateDeadline_ and drops persistentState
//! Returns false if InitialPersistentState_ is empty
//! Returns true otherwise
bool TSchedulingPolicy::CheckInitializationTimeout()
{
    if (!InitialPersistentState_) {
        return false;
    }

    if (InitialPersistentState_->NodeStates.empty() && InitialPersistentState_->OperationStates.empty()) [[likely]] {
        return false;
    }

    if (TInstant::Now() > InitializationFromPersistentStateDeadline_) {
        InitialPersistentState_.Reset();
        return false;
    }

    return true;
}

std::optional<TPersistentNodeState> TSchedulingPolicy::FindInitialNodePersistentState(TNodeId nodeId)
{
    std::optional<TPersistentNodeState> maybeState;

    if (!CheckInitializationTimeout()) {
        return maybeState;
    }

    auto it = InitialPersistentState_->NodeStates.find(nodeId);

    if (it != InitialPersistentState_->NodeStates.end()) {
        maybeState = std::move(it->second);
        InitialPersistentState_->NodeStates.erase(it);
    }

    return maybeState;
}

std::optional<TPersistentOperationState> TSchedulingPolicy::FindInitialOperationPersistentState(TOperationId operationId)
{
    std::optional<TPersistentOperationState> maybeState;

    if (!CheckInitializationTimeout()) {
        return maybeState;
    }

    auto it = InitialPersistentState_->OperationStates.find(operationId);

    if (it != InitialPersistentState_->OperationStates.end()) {
        maybeState = std::move(it->second);
        InitialPersistentState_->OperationStates.erase(it);
    }

    return maybeState;
}

void TSchedulingPolicy::UpdatePersistentState()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
    PersistentState_ = New<TPersistentState>();

    for (const auto& [nodeId, node] : Nodes_) {
        auto& nodePersistentState = PersistentState_->NodeStates[nodeId];
        nodePersistentState.SchedulingModule = node->SchedulingModule();
        nodePersistentState.Address = node->Address();

        YT_LOG_DEBUG("Updated node persistent state (NodeId: %v)", nodeId);
    }

    auto updateOperationPersistentState = [&] (auto it) {
        const auto& [operationId, operation] = it;
        auto& operationPersistentState = PersistentState_->OperationStates[operationId];
        operationPersistentState.SchedulingModule = operation->SchedulingModule();
        operationPersistentState.NetworkPriority = operation->NetworkPriority();

        YT_LOG_DEBUG("Updated operation persistent state (OperationId: %v, SchedulingModule: %v, NetworkPriority: %v, Enabled: %v)",
            operationId,
            operation->SchedulingModule(),
            operation->NetworkPriority(),
            operation->IsEnabled());
    };

    std::ranges::for_each(DisabledOperations_, updateOperationPersistentState);
    std::ranges::for_each(EnabledOperations_, updateOperationPersistentState);
}

void TSchedulingPolicy::LogSnapshotEvent(const TGpuPlanUpdateStatisticsPtr& statistics) const
{
    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::ModulesInfo)
        .Item("modules").DoMapFor(statistics->ModuleStatistics, [] (TFluentMap fluent, const auto& item) {
            const auto& [module, moduleStatistic] = item;
            fluent.Item(module).Value(moduleStatistic);
        });

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::NodesInfo)
        .Item("nodes").DoMapFor(Nodes_, [] (TFluentMap fluent, const auto& item) {
            const auto& [_, node] = item;
            fluent.Item(node->Address()).Value(node);
        });

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::OperationsInfo)
        .Item("operations").DoMap([&] (TFluentMap fluent) {
            for (const auto& [operationId, operation] : EnabledOperations_) {
                fluent
                    .Item(ToString(operationId)).Value(operation);
            }
            for (const auto& [operationId, operation] : DisabledOperations_) {
                fluent
                    .Item(ToString(operationId)).Value(operation);
            }
        });
}

void TSchedulingPolicy::ProfileAssignmentPlanUpdating(const TGpuPlanUpdateStatisticsPtr& statistics)
{
    ProfilingCounters_.PlannedAssignments.Increment(statistics->PlannedAssignments);
    ProfilingCounters_.PreemptedAssignments.Increment(statistics->PreemptedAssignments);

    ProfilingCounters_.TotalPlanningTime.Record(statistics->Timer.GetElapsedTime());
    ProfilingCounters_.OperationResourcesUpdateTime.Record(statistics->UpdatingOperationResourcesDuration);
    ProfilingCounters_.FullHostPlanningTime.Record(statistics->FullHostPlanningDuration);
    ProfilingCounters_.RegularPlanningTime.Record(statistics->RegularPlanningDuration);
    ProfilingCounters_.ExtraPlanningTime.Record(statistics->ExtraPlanningDuration);

    ProfilingCounters_.EnabledOperations.Update(std::ssize(EnabledOperations_));

    for (const auto& [module, moduleStatistic] : statistics->ModuleStatistics) {
        auto it = ProfilingCounters_.ModuleCounters.find(module);
        if (it == ProfilingCounters_.ModuleCounters.end()) {
            it = ProfilingCounters_.ModuleCounters.emplace(module, Profiler_.WithPrefix("/module").WithTag("module", module)).first;
        }

        const auto& moduleCounters = it->second;
        moduleCounters.TotalModuleNodes.Update(moduleStatistic.TotalNodes);
        moduleCounters.ModuleUnreservedNodes.Update(moduleStatistic.UnreservedNodes);
        moduleCounters.ModuleFullHostModuleBoundOperations.Update(moduleStatistic.FullHostModuleBoundOperations);
    }

    int assignments = 0;
    int assignedGpu = 0;
    int fullHostModuleBoundOperations = 0;

    for (const auto& [_, operation] : EnabledOperations_) {
        assignments += std::ssize(operation->Assignments());
        assignedGpu += operation->AssignedResourceUsage().GetGpu();

        if (operation->IsFullHostModuleBound()) {
            fullHostModuleBoundOperations += 1;
        }
    }

    ProfilingCounters_.Assignments.Update(assignments);
    ProfilingCounters_.AssignedGpu.Update(assignedGpu);
    ProfilingCounters_.FullHostModuleBoundOperations.Update(fullHostModuleBoundOperations);
}

TLogger TSchedulingPolicy::MakeNodeLogger(const TExecNodeDescriptorPtr& nodeDescriptor)
{
    YT_VERIFY(nodeDescriptor);

    return Logger.WithTag(
        "NodeId: %v, NodeAddress: %v",
        nodeDescriptor->Id,
        nodeDescriptor->GetDefaultAddress());
}

void TSchedulingPolicy::DoProcessSchedulingHeartbeat(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    const TPoolTreeSnapshotPtr& treeSnapshot,
    bool skipScheduleAllocations)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&AssignmentPlanUpdateLock_))
        .ValueOrThrow();

    const auto& nodeDescriptor = schedulingHeartbeatContext->GetNodeDescriptor();
    const auto Logger = MakeNodeLogger(nodeDescriptor);
    auto node = GetOrDefault(Nodes_, nodeDescriptor->Id);

    if (!node) {
        YT_LOG_WARNING("Skipping scheduling heartbeat because node is not registered");
        return;
    }

    UpdateNodeDescriptor(node, nodeDescriptor);

    if (Config_->Mode == EGpuSchedulingPolicyMode::DryRun) {
        return;
    }

    const auto statistics = New<TGpuScheduleAllocationsStatistics>();
    schedulingHeartbeatContext->SetSchedulingStatistics(statistics);

    PreemptAllocations(node, schedulingHeartbeatContext, treeSnapshot);

    if (!skipScheduleAllocations && node->IsSchedulable()) {
        ScheduleAllocations(node, schedulingHeartbeatContext, treeSnapshot);
    }

    statistics->ScheduledAllocationCount = std::ssize(schedulingHeartbeatContext->StartedAllocations());
    statistics->PreemptedAllocationCount = std::ssize(schedulingHeartbeatContext->PreemptedAllocations());
    statistics->PreemptibleAllocationCount = std::ranges::count_if(
        node->AllocationIdToAssignment() | std::views::values,
        &TAssignment::Preemptible);

    statistics->ResourceLimits = schedulingHeartbeatContext->ResourceLimits();
    statistics->ResourceUsage = schedulingHeartbeatContext->ResourceUsage();
    node->LastSchedulingHeartbeatStatistics() = statistics;
}

void TSchedulingPolicy::PreemptAllocations(
    const TNodePtr& node,
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    const TPoolTreeSnapshotPtr& treeSnapshot)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    const auto Logger = MakeNodeLogger(node->Descriptor());

    auto runningAllocationInfos = CollectRunningAllocationInfos(schedulingHeartbeatContext, treeSnapshot);

    THashSet<TAllocationId> preemptedAllocations;
    for (auto allocationId : node->AllocationsToPreempt()) {
        auto it = runningAllocationInfos.find(allocationId);
        if (it == runningAllocationInfos.end()) {
            YT_LOG_DEBUG("No running allocation to preempt (AllocationId: %v)", allocationId);
            continue;
        }

        const auto& [runningAllocation, operationElement] = it->second;
        if (!operationElement) {
            YT_LOG_WARNING("Dangling allocation found (AllocationId: %v)", allocationId);
            continue;
        }

        if (!PreemptAllocation(runningAllocation, operationElement, schedulingHeartbeatContext)) {
            continue;
        }

        runningAllocationInfos.erase(it);
        InsertOrCrash(preemptedAllocations, allocationId);
    }

    for (const auto& allocationId : preemptedAllocations) {
        node->PreemptAllocation(allocationId);
    }

    for (const auto& [allocationId, allocationInfo] : runningAllocationInfos) {
        auto assignment = GetOrDefault(node->AllocationIdToAssignment(), allocationId);
        if (assignment) {
            continue;
        }

        if (node->PreemptedAllocations().contains(allocationId)) {
            continue;
        }

        const auto& [runningAllocation, operationElement] = allocationInfo;

        if (!operationElement) {
            YT_LOG_WARNING("Dangling allocation found (AllocationId: %v)", allocationId);
            continue;
        }

        YT_LOG_WARNING("Found allocation without assignment (OperationId: %v, AllocationId: %v)",
            runningAllocation->GetOperationId(),
            runningAllocation->GetId());

        PreemptAllocation(
            runningAllocation,
            operationElement,
            schedulingHeartbeatContext);
    }
}

void TSchedulingPolicy::ScheduleAllocations(
    const TNodePtr& node,
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    const TPoolTreeSnapshotPtr& treeSnapshot)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    const auto NodeLogger = MakeNodeLogger(node->Descriptor());

    auto nodeShardId = StrategyHost_->GetNodeShardId(node->GetId());
    const auto& nodeShardInvoker = StrategyHost_->GetNodeShardInvokers()[nodeShardId];

    // NB(yaishenka): Copy assignments with |GetItems|, because the set will be modified.
    for (const auto& assignment : GetItems(node->Assignments())) {
        // NB(yaishenka): Node can be unregistered after wait in DoScheduleAllocation.
        if (!Nodes_.contains(node->GetId())) {
            const auto& Logger = NodeLogger;
            YT_LOG_WARNING("Node was unregistered while scheduling allocations");
            return;
        }

        if (!IsAssignmentPreliminary(assignment)) {
            continue;
        }

        auto operationId = assignment->OperationId;
        const auto Logger = NodeLogger.WithTag("OperationId: %v", operationId);

        auto operationElement = treeSnapshot->FindEnabledOperationElement(operationId);
        auto operation = GetOrDefault(EnabledOperations_, operationId);

        if (!operationElement || !operation) {
            YT_LOG_WARNING("Cannot schedule allocation because operation is %v",
                treeSnapshot->FindDisabledOperationElement(operationId)
                    ? "disabled"
                    : "missing in snapshot");

            RemoveAssignment(assignment);
            continue;
        }

        if (!operation->Assignments().contains(assignment)) {
            YT_LOG_WARNING("Assignment does not belong to operation anymore");
            RemoveAssignment(assignment, /*strict*/ true);
            continue;
        }

        // NB(yaishenka): At this moment we are certain that operation is enabled,
        // node is registered, assignment exists in node and operation.
        TJobResources availableResourceLimits;
        auto increaseResult = operationElement->TryIncreaseHierarchicalResourceUsagePrecommit(
            assignment->ResourceUsage,
            /*allowLimitsOvercommit*/ false,
            /*additionalLocalResourceLimits*/ {},
            &availableResourceLimits);
        auto precommittedResources = assignment->ResourceUsage;

        auto availableResources = Min(availableResourceLimits, schedulingHeartbeatContext->GetNodeFreeResourcesWithDiscount());

        if (increaseResult != EResourceTreeIncreaseResult::Success) {
            YT_LOG_WARNING("Failed to increase operation resource usage precommit (IncreaseResult: %v)", increaseResult);
            RemoveAssignment(assignment);
            continue;
        }

        nodeShardInvoker->Invoke(BIND(
            &TPoolTreeOperationElement::OnScheduleAllocationStarted,
            MakeWeak(operationElement),
            schedulingHeartbeatContext));

        auto scheduleAllocationResult = DoScheduleAllocation(
            node,
            operation,
            operationElement,
            assignment,
            schedulingHeartbeatContext,
            treeSnapshot,
            availableResources);

        // TODO(yaishenka): Set operation alert if timeout.
        if (!scheduleAllocationResult->StartDescriptor) {
            YT_LOG_DEBUG(
                "Failed to schedule allocation, removing assignment "
                "(OperationId: %v, AllocationGroupName: %v, AssignmentResourceUsage: %v, Reasons: %v)",
                operationId,
                assignment->AllocationGroupName,
                assignment->ResourceUsage,
                scheduleAllocationResult->Failed);

            RemoveAssignment(assignment, /*strict*/ false);

            // NB(yaishenka): Operation can be disabled while we wait for CA to schedule allocation.
            if (operation->IsEnabled()) {
                operationElement->DecreaseHierarchicalResourceUsagePrecommit(precommittedResources);
            }

            nodeShardInvoker->Invoke(BIND(
                &TPoolTreeOperationElement::OnScheduleAllocationFailed,
                MakeWeak(operationElement),
                schedulingHeartbeatContext,
                operationElement->GetTreeId(),
                scheduleAllocationResult));

            nodeShardInvoker->Invoke(BIND(
                &TPoolTreeOperationElement::OnScheduleAllocationFinished,
                MakeWeak(operationElement),
                schedulingHeartbeatContext));
            continue;
        }

        auto allocationId = scheduleAllocationResult->StartDescriptor->Id;
        auto allocation = New<TAllocationState>(
            allocationId,
            node->GetId(),
            assignment,
            assignment->ResourceUsage);
        assignment->AddAllocation(allocation);

        LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::AllocationScheduled)
            .Item("operation_id").Value(operationElement->GetOperationId())
            .Item("allocation_id").Value(allocationId)
            .Item("node_address").Value(node->Address())
            .Item("allocation_group_name").Value(assignment->AllocationGroupName)
            .Item("resource_usage").Value(allocation->ResourceUsage())
            .Item("preemptible").Value(assignment->Preemptible);

        operationElement->CommitHierarchicalResourceUsage(
            allocation->ResourceUsage(),
            allocation->ResourceUsage());

        // TODO(yaishenka): Scheduling index and stage type are irrelevant for this policy. Do not store it in TAllocation.
        schedulingHeartbeatContext->StartAllocation(
            operationElement->GetTreeId(),
            operationElement->GetOperationId(),
            scheduleAllocationResult->IncarnationId,
            scheduleAllocationResult->ControllerEpoch,
            *scheduleAllocationResult->StartDescriptor,
            operationElement->Spec()->PreemptionMode,
            /*schedulingIndex*/ 0,
            /*stageType*/ EAllocationSchedulingStage::RegularMediumPriority,
            operation->NetworkPriority());

        nodeShardInvoker->Invoke(BIND(
            &TPoolTreeOperationElement::OnScheduleAllocationFinished,
            MakeWeak(operationElement),
            schedulingHeartbeatContext));

        YT_LOG_DEBUG(
            "Allocation scheduled "
            "(AllocationId: %v, AllocationResourceLimits: %v, "
            "ControllerDuration: %v, ControllerNextDurationEstimate: %v)",
            allocationId,
            StrategyHost_->FormatResources(scheduleAllocationResult->StartDescriptor->ResourceLimits),
            scheduleAllocationResult->Duration,
            scheduleAllocationResult->NextDurationEstimate);
    }
}

bool TSchedulingPolicy::PreemptAllocation(
    const TAllocationPtr& allocation,
    TPoolTreeOperationElement* element,
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    MaybeDelay(element->Spec()->TestingOperationOptions->DelayBeforeAllocationPreemption);

    auto operation = GetOrDefault(EnabledOperations_, allocation->GetOperationId());
    if (!operation) {
        YT_LOG_WARNING("Operation not found (OperationId: %v)", allocation->GetOperationId());
        return false;
    }

    auto allocationState = GetOrDefault(operation->AllocationIdToAllocationState(), allocation->GetId());

    schedulingHeartbeatContext->ResourceUsage() -= allocation->ResourceUsage();
    allocation->ResourceUsage() = TJobResources();

    YT_LOG_WARNING_UNLESS(allocationState,
        "Allocation not found in operation during preemption "
        "(OperationId: %v, AllocationId: %v)",
        element->GetOperationId(),
        allocation->GetId());

    auto preemptionReason = EAllocationPreemptionReason::UnexpectedAllocation;
    TJobResources usageToPreempt;
    if (allocationState) {
        if (const auto& preemptionInfo = allocationState->PreemptionInfo()) {
            preemptionReason = preemptionInfo->Reason;
            allocation->SetPreemptionReason(preemptionInfo->Description);
            if (preemptionInfo->PreemptedForOperationId) {
                allocation->SetPreemptedFor(TPreemptedFor{.OperationId = preemptionInfo->PreemptedForOperationId});
            }
        }

        usageToPreempt = allocationState->UpdateResourceUsage(TJobResources());
    }

    if (usageToPreempt != TJobResources()) {
        element->IncreaseHierarchicalResourceUsage(usageToPreempt);
    }

    YT_LOG_DEBUG(
        "Preempting allocation (OperationId: %v, AllocationId: %v, PreemptionReason: %v, PreemptedUsage: %v)",
        element->GetOperationId(),
        allocation->GetId(),
        preemptionReason,
        usageToPreempt);

    schedulingHeartbeatContext->PreemptAllocation(
        allocation,
        element->GetEffectiveAllocationPreemptionTimeout(),
        preemptionReason);

    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::AllocationPreempted)
        .Item("operation_id").Value(element->GetOperationId())
        .Item("allocation_id").Value(allocation->GetId())
        .Item("node_address").Value(allocation->GetNode()->GetDefaultAddress())
        .Item("reason").Value(preemptionReason)
        .Item("preempted_usage").Value(-usageToPreempt)
        .DoIf(allocationState != nullptr, [&] (auto fluent) {
            fluent.OptionalItem("preemption_info", allocationState->PreemptionInfo());
        });

    return true;
}

// TODO(YT-27867): Add diagnostics like in regular policy.
TControllerScheduleAllocationResultPtr TSchedulingPolicy::DoScheduleAllocation(
    const TNodePtr& node,
    const TOperationPtr& operation,
    TPoolTreeOperationElement* operationElement,
    const TAssignmentPtr& assignment,
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    const TPoolTreeSnapshotPtr& treeSnapshot,
    const TJobResources& availableResources)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    const auto NodeLogger = MakeNodeLogger(node->Descriptor());
    const auto Logger = NodeLogger.WithTag("OperationId: %v", operationElement->GetOperationId());

    auto nodeShardId = StrategyHost_->GetNodeShardId(node->GetId());
    const auto& nodeShardInvoker = StrategyHost_->GetNodeShardInvokers()[nodeShardId];

    auto scheduleAllocationFuture = BIND(
        &TPoolTreeOperationElement::ScheduleAllocation,
        MakeStrong(operationElement),
        schedulingHeartbeatContext,
        availableResources,
        schedulingHeartbeatContext->GetNodeFreeDiskResourcesWithDiscount(assignment->ResourceUsage),
        treeSnapshot->ControllerConfig()->ScheduleAllocationTimeLimit,
        operationElement->GetTreeId(),
        assignment->AllocationGroupName)
        .AsyncVia(nodeShardInvoker)
        .Run();
    ++GetScheduleAllocationsStatistics(schedulingHeartbeatContext)->ControllerScheduleAllocationCount;

    auto scheduleAllocationResult = WaitFor(scheduleAllocationFuture)
        .ValueOrThrow();

    if (!scheduleAllocationResult->StartDescriptor) {
        if (scheduleAllocationResult->Failed[NControllerAgent::EScheduleFailReason::Timeout] > 0) {
            YT_LOG_WARNING("Allocation scheduling timed out");
            ++GetScheduleAllocationsStatistics(schedulingHeartbeatContext)->ControllerScheduleAllocationTimedOutCount;

            YT_UNUSED_FUTURE(StrategyHost_->SetOperationAlert(
                operationElement->GetOperationId(),
                EOperationAlertType::ScheduleJobTimedOut,
                TError("Allocation scheduling timed out: either scheduler is under heavy load or operation is too heavy"),
                treeSnapshot->ControllerConfig()->ScheduleAllocationTimeoutAlertResetTime));
        }

        return scheduleAllocationResult;
    }

    YT_VERIFY(scheduleAllocationResult->StartDescriptor->AllocationGroupName == assignment->AllocationGroupName);

    auto allocationId = scheduleAllocationResult->StartDescriptor->Id;

    // NB(yaishenka): After wait above node can be unregistered, operation can be disabled (and maybe enabled again)
    if (!node->Assignments().contains(assignment) || !operation->IsEnabled()) {
        YT_LOG_DEBUG(
            "Aborting allocation with deleted assignment "
            "(AllocationId: %v, AllocationResources: %v, NodeAssignedResources: %v, NodeResourceLimits: %v)",
            allocationId,
            FormatResources(scheduleAllocationResult->StartDescriptor->ResourceLimits.ToJobResources()),
            FormatResources(node->AssignedResourceUsage()),
            FormatResources(node->Descriptor()->ResourceLimits));

        operationElement->AbortAllocation(
            allocationId,
            EAbortReason::SchedulingResourceOvercommit,
            scheduleAllocationResult->ControllerEpoch);

        scheduleAllocationResult = New<TControllerScheduleAllocationResult>();
        scheduleAllocationResult->RecordFail(NControllerAgent::EScheduleFailReason::NoCandidateTasks);

        return scheduleAllocationResult;
    }

    auto delta = assignment->UpdateResourceUsage(scheduleAllocationResult->StartDescriptor->ResourceLimits);
    auto increaseResult = operationElement->TryIncreaseHierarchicalResourceUsagePrecommit(
        delta,
        /*allowLimitsOvercommit*/ false);

    switch (increaseResult) {
        case EResourceTreeIncreaseResult::Success:
            break;
        case EResourceTreeIncreaseResult::AdditionalResourceLimitExceeded:
            // NB(yaishenka): We do not provide additional resource limits, so we do not expect this value.
            YT_ABORT();
        case EResourceTreeIncreaseResult::ResourceLimitExceeded: {
            YT_LOG_DEBUG(
                "Aborting allocation with resource overcommit "
                "(AllocationId: %v, AllocationResources: %v, NodeAssignedResources: %v, NodeResourceLimits: %v)",
                allocationId,
                FormatResources(scheduleAllocationResult->StartDescriptor->ResourceLimits.ToJobResources()),
                FormatResources(node->AssignedResourceUsage()),
                FormatResources(node->Descriptor()->ResourceLimits));

            operationElement->AbortAllocation(
                allocationId,
                EAbortReason::SchedulingResourceOvercommit,
                scheduleAllocationResult->ControllerEpoch);

            scheduleAllocationResult = New<TControllerScheduleAllocationResult>();
            scheduleAllocationResult->RecordFail(NControllerAgent::EScheduleFailReason::ResourceOvercommit);

            return scheduleAllocationResult;
        }
        case EResourceTreeIncreaseResult::ElementIsNotAlive: {
            YT_LOG_DEBUG("Aborting allocation as operation is not alive in tree anymore (AllocationId: %v)", allocationId);

            operationElement->AbortAllocation(
                allocationId,
                EAbortReason::SchedulingOperationIsNotAlive,
                scheduleAllocationResult->ControllerEpoch);

            scheduleAllocationResult = New<TControllerScheduleAllocationResult>();
            scheduleAllocationResult->RecordFail(NControllerAgent::EScheduleFailReason::OperationIsNotAlive);

            return scheduleAllocationResult;
        }
    }

    if (Dominates(node->AssignedResourceUsage(), node->Descriptor()->ResourceLimits)) {
        YT_LOG_DEBUG(
            "Aborting allocation with resource overcommit (AllocationId: %v, AllocationResources: %v)",
            allocationId,
            FormatResources(scheduleAllocationResult->StartDescriptor->ResourceLimits.ToJobResources()));

        operationElement->AbortAllocation(
            allocationId,
            EAbortReason::SchedulingResourceOvercommit,
            scheduleAllocationResult->ControllerEpoch);

        scheduleAllocationResult = New<TControllerScheduleAllocationResult>();
        scheduleAllocationResult->RecordFail(NControllerAgent::EScheduleFailReason::ResourceOvercommit);
    }

    return scheduleAllocationResult;
}

TProcessAllocationUpdateResult TSchedulingPolicy::DoProcessAllocationUpdate(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    TPoolTreeOperationElementPtr element,
    const TAllocationUpdate& allocationUpdate)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    TForbidContextSwitchGuard contextSwitchGuard;

    auto node = GetOrDefault(Nodes_, allocationUpdate.NodeId);

    auto operation = GetOrDefault(EnabledOperations_, element->GetOperationId());
    if (!operation) {
        YT_LOG_DEBUG(
            "No enabled operation found for allocation (OperationId: %v, AllocationId: %v)",
            element->GetOperationId(),
            allocationUpdate.AllocationId);

        return TProcessAllocationUpdateResult{
            .Status = EAllocationUpdateStatus::Disabled,
            .NeedToPostpone = true,
        };
    }

    auto allocation = GetOrDefault(operation->AllocationIdToAllocationState(), allocationUpdate.AllocationId);

    if (!allocation) {
        YT_LOG_DEBUG(
            "Found unexpected allocation, aborting it (OperationId: %v, AllocationId: %v)",
            element->GetOperationId(),
            allocationUpdate.AllocationId);

        if (allocationUpdate.Finished) {
            return TProcessAllocationUpdateResult{
                .Status = EAllocationUpdateStatus::Unexpected,
            };
        }

        return TProcessAllocationUpdateResult{
            .Status = EAllocationUpdateStatus::Unexpected,
            .NeedToPostpone = false,
            .NeedToAbort = true,
            .AbortReason = EAbortReason::Abandoned,
        };
    }

    auto assignment = allocation->Assignment().Lock();

    if (allocationUpdate.Finished) {
        return DoProcessFinishedAllocation(
            element,
            allocationUpdate,
            node,
            operation,
            assignment,
            allocation);
    }

    YT_VERIFY(allocationUpdate.ResourceUsageUpdated || allocationUpdate.PreemptibleProgressStartTime);

    auto delta = allocation->UpdateResourceUsage(allocationUpdate.AllocationResources);
    if (delta != TJobResources()) {
        element->IncreaseHierarchicalResourceUsage(delta);
    }

    if (!assignment) {
        // NB(yaishenka): All preempted but not finished allocations go here.
        YT_LOG_DEBUG(
            "Found allocation without assignment (OperationId: %v, AllocationId: %v)",
            element->GetOperationId(),
            allocationUpdate.AllocationId);

        return TProcessAllocationUpdateResult{
            .Status = EAllocationUpdateStatus::Updated,
        };
    }

    if (allocationUpdate.PreemptibleProgressStartTime) {
        assignment->PreemptibleProgressStartTime = allocationUpdate.PreemptibleProgressStartTime;
    }

    if (assignment->Node->Descriptor() &&
        Dominates(assignment->Node->AssignedResourceUsage(), assignment->Node->Descriptor()->ResourceLimits))
    {
        YT_LOG_DEBUG(
            "Preempting assignment with resource overcommit "
            "(OperationId: %v, AllocationId: %v, AllocationResources: %v)",
            operation->GetId(),
            assignment->AllocationId,
            FormatResources(assignment->ResourceUsage));

        PreemptAssignment(
            assignment,
            EAllocationPreemptionReason::ResourceOvercommit,
            /*preemptionDescription*/ "Preempted due to node resource overcommit");
    }

    return TProcessAllocationUpdateResult{
        .Status = EAllocationUpdateStatus::Updated,
    };
}

TProcessAllocationUpdateResult TSchedulingPolicy::DoProcessFinishedAllocation(
    const TPoolTreeOperationElementPtr& element,
    const TAllocationUpdate& allocationUpdate,
    const TNodePtr& node,
    const TOperationPtr& operation,
    const TAssignmentPtr& assignment,
    const TAllocationStatePtr& allocation)
{
    YT_LOG_DEBUG(
        "Allocation finished (OperationId: %v, AllocationId: %v)",
        element->GetOperationId(),
        allocationUpdate.AllocationId);

    operation->RemoveAllocation(allocationUpdate.AllocationId);
    element->IncreaseHierarchicalResourceUsage(-allocation->ResourceUsage());

    RemovePendingRevivedAllocation(allocation->GetNodeId(), allocation->GetId());

    if (assignment) {
        RemoveAssignment(assignment);

        return TProcessAllocationUpdateResult{
            .Status = EAllocationUpdateStatus::Updated,
        };
    }

    if (!node) {
        YT_LOG_DEBUG(
            "No registered node found for allocation (OperationId: %v, AllocationId: %v, NodeId: %v)",
            element->GetOperationId(),
            allocationUpdate.AllocationId,
            allocationUpdate.NodeId);

        return TProcessAllocationUpdateResult{
            .Status = EAllocationUpdateStatus::Updated,
        };
    }

    if (node->AllocationsToPreempt().contains(allocationUpdate.AllocationId)) {
        YT_LOG_DEBUG(
            "Found finished allocation waiting for preemption (OperationId: %v, AllocationId: %v)",
            element->GetOperationId(),
            allocationUpdate.AllocationId);

        node->PreemptAllocation(allocationUpdate.AllocationId);
        node->RemovePreemptedAllocation(allocationUpdate.AllocationId);
    } else if (node->PreemptedAllocations().contains(allocationUpdate.AllocationId)) {
        YT_LOG_DEBUG(
            "Found finished preempted allocation (OperationId: %v, AllocationId: %v)",
            element->GetOperationId(),
            allocationUpdate.AllocationId);

        node->RemovePreemptedAllocation(allocationUpdate.AllocationId);
    }

    return TProcessAllocationUpdateResult{
        .Status = EAllocationUpdateStatus::Updated,
    };
}

// TODO(YT-27867): consider to add more info here
void TSchedulingPolicy::DoBuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto node = GetOrDefault(Nodes_, nodeId);
    if (!node) {
        return;
    }

    fluent
        .Item("module").Value(node->SchedulingModule())
        .Item("assignments").List(node->Assignments())
        .OptionalItem("last_heartbeat_statistics", node->LastSchedulingHeartbeatStatistics());
}

// TODO(YT-27867): consider to add more info here
void TSchedulingPolicy::DoBuildSchedulingAttributesStringForNode(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    TNodeId nodeId,
    TDelimitedStringBuilderWrapper* builderWrapper) const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto node = GetOrDefault(Nodes_, nodeId);
    if (!node) {
        return;
    }

    auto& builder = *builderWrapper;
    builder->AppendFormat(
        "SchedulingModule: %v, AssignedUsage: %v",
        node->SchedulingModule(),
        node->AssignedResourceUsage());

    auto statistics = DynamicPointerCast<TGpuScheduleAllocationsStatistics>(schedulingHeartbeatContext->GetSchedulingStatistics());
    if (statistics) {
        builder->AppendFormat(
            "ControllerScheduleAllocationCount: %v, ControllerScheduleAllocationTimedOutCount: %v",
            statistics->ControllerScheduleAllocationCount,
            statistics->ControllerScheduleAllocationTimedOutCount);
    }
}

void TSchedulingPolicy::RemovePendingRevivedAllocation(TNodeId nodeId, TAllocationId allocationId)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto it = PendingRevivedAllocations_.find(nodeId);
    if (it == PendingRevivedAllocations_.end()) {
        return;
    }

    auto& [_, allocations] = *it;
    allocations.erase(allocationId);
    if (allocations.empty()) {
        PendingRevivedAllocations_.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

TNoopSchedulingPolicy::TNoopSchedulingPolicy(const std::string& treeId)
    : Logger(GetLogger(treeId))
{ }

void TNoopSchedulingPolicy::Initialize()
{ }

void TNoopSchedulingPolicy::RegisterNode(TNodeId /*nodeId*/, const std::string& /*nodeAddress*/)
{ }

void TNoopSchedulingPolicy::UnregisterNode(TNodeId /*nodeId*/)
{ }

void TNoopSchedulingPolicy::ProcessSchedulingHeartbeat(
    const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    bool /*skipScheduleAllocations*/)
{ }

void TNoopSchedulingPolicy::RegisterOperation(const TPoolTreeOperationElement* /*element*/)
{ }

void TNoopSchedulingPolicy::UnregisterOperation(const TPoolTreeOperationElement* /*element*/)
{ }

TError TNoopSchedulingPolicy::OnOperationMaterialized(const TPoolTreeOperationElement* /*element*/)
{
    return {};
}

TError TNoopSchedulingPolicy::CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* /*element*/) const
{
    return {};
}

void TNoopSchedulingPolicy::EnableOperation(const TPoolTreeOperationElement* /*element*/)
{ }

void TNoopSchedulingPolicy::DisableOperation(TPoolTreeOperationElement* /*element*/, bool /*markAsNonAlive*/)
{ }

void TNoopSchedulingPolicy::RegisterAllocationsFromRevivedOperation(
    TPoolTreeOperationElement* /*element*/,
    std::vector<TAllocationPtr> /*allocations*/)
{ }

TProcessAllocationUpdateResult TNoopSchedulingPolicy::ProcessAllocationUpdate(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    TPoolTreeOperationElement* /*element*/,
    const TAllocationUpdate& /*allocationUpdate*/)
{
    YT_UNIMPLEMENTED();
}

void TNoopSchedulingPolicy::BuildSchedulingAttributesStringForNode(
    const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
    TNodeId /*nodeId*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{ }

void TNoopSchedulingPolicy::BuildSchedulingAttributesForNode(TNodeId /*nodeId*/, TFluentMap /*fluent*/) const
{ }

void TNoopSchedulingPolicy::BuildSchedulingAttributesStringForOngoingAllocations(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const std::vector<TAllocationPtr>& /*allocations*/,
    TInstant /*now*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{ }

void TNoopSchedulingPolicy::BuildElementLoggingStringAttributes(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const TPoolTreeElement* /*element*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{ }

void TNoopSchedulingPolicy::PopulateOrchidService(const TCompositeMapServicePtr& /*orchidService*/) const
{ }

void TNoopSchedulingPolicy::ProfileOperation(
    const TPoolTreeOperationElement* /*element*/,
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    NProfiling::ISensorWriter* /*writer*/) const
{ }

TPostUpdateContextPtr TNoopSchedulingPolicy::CreatePostUpdateContext(TPoolTreeRootElement* /*rootElement*/)
{
    return nullptr;
}

void TNoopSchedulingPolicy::PostUpdate(
    TFairSharePostUpdateContext* /*fairSharePostUpdateContext*/,
    TPostUpdateContextPtr* /*postUpdateContext*/)
{ }

TPoolTreeSnapshotStatePtr TNoopSchedulingPolicy::CreateSnapshotState(TPostUpdateContextPtr* /*postUpdateContext*/)
{
    return nullptr;
}

void TNoopSchedulingPolicy::OnResourceUsageSnapshotUpdate(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const TResourceUsageSnapshotPtr& /*resourceUsageSnapshot*/) const
{ }

void TNoopSchedulingPolicy::UpdateConfig(TStrategyTreeConfigPtr config)
{
    if (EGpuSchedulingPolicyMode::Noop != config->GpuSchedulingPolicy->Mode) {
        YT_LOG_WARNING("GPU scheduling policy config update failed because mode has changed (OldMode: %v, NewMode: %v)",
            EGpuSchedulingPolicyMode::Noop,
            config->GpuSchedulingPolicy->Mode);
        return;
    }
}

void TNoopSchedulingPolicy::InitPersistentState(INodePtr /*persistentState*/)
{ }

INodePtr TNoopSchedulingPolicy::BuildPersistentState() const
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
