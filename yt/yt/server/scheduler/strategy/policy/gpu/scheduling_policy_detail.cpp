#include "scheduling_policy_detail.h"

#include "helpers.h"

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
    , ReguralPlanningTime(profiler.Timer("/regular_planning_time"))
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
    : TAssignmentPlanContextBase(GetLogger(treeId))
    , Host_(std::move(host))
    , StrategyHost_(strategyHost)
    , Logger(GetLogger(treeId))
    , Config_(std::move(config))
    , PlanUpdateExecutor_(New<TPeriodicExecutor>(
        StrategyHost_->GetControlInvoker(EControlQueue::GpuAssignmentPlanUpdate),
        BIND(&TSchedulingPolicy::UpdateAssignmentPlan, MakeWeak(this)),
        Config_->PlanUpdatePeriod))
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

    EmplaceOrCrash(Nodes_, nodeId, New<TNode>(nodeAddress));

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

void TSchedulingPolicy::UpdateNodeDescriptor(TNodeId nodeId, TExecNodeDescriptorPtr descriptor)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    TForbidContextSwitchGuard contextSwitchGuard;

    YT_VERIFY(nodeId == descriptor->Id);

    auto nodeIt = Nodes_.find(nodeId);
    if (nodeIt == Nodes_.end()) {
        YT_LOG_DEBUG("Can't update node descriptor because node is missing (NodeId: %v)", nodeId);
        return;
    }
    const auto& node = nodeIt->second;

    YT_VERIFY(node->Address() == descriptor->GetDefaultAddress());

    bool wasSchedulable = node->IsSchedulable();

    node->SetDescriptor(std::move(descriptor));

    ReviveNodeState(nodeId, node);

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

void TSchedulingPolicy::ProcessSchedulingHeartbeat(
    const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    bool /*skipScheduleAllocations*/)
{
    YT_UNIMPLEMENTED();
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

    PreemptAllOperationAssignments(
        operation,
        EAllocationPreemptionReason::OperationUnregistered,
        "Node unregistered");

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

TError TSchedulingPolicy::CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* /*element*/) const
{
    YT_UNIMPLEMENTED();
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

    YT_LOG_DEBUG("Operation enabled (OperationId: %v)", operation->GetId());
}

void TSchedulingPolicy::DisableOperation(TPoolTreeOperationElement* element, bool /*markAsNonAlive*/)
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

    YT_LOG_DEBUG("Operation disabled (OperationId: %v)", operation->GetId());
}

void TSchedulingPolicy::RegisterAllocationsFromRevivedOperation(
    TPoolTreeOperationElement* /*element*/,
    std::vector<TAllocationPtr> /*allocations*/) const
{
    YT_UNIMPLEMENTED();
}

bool TSchedulingPolicy::ProcessAllocationUpdate(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    TPoolTreeOperationElement* /*element*/,
    TAllocationId /*allocationId*/,
    const TJobResources& /*allocationResources*/,
    bool /*resetPreemptibleProgress*/,
    const std::optional<std::string>& /*allocationDataCenter*/,
    const std::optional<std::string>& /*allocationInfinibandCluster*/,
    std::optional<EAbortReason>* /*maybeAbortReason*/) const
{
    YT_UNIMPLEMENTED();
}

bool TSchedulingPolicy::ProcessFinishedAllocation(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    TPoolTreeOperationElement* /*element*/,
    TAllocationId /*allocationId*/) const
{
    YT_UNIMPLEMENTED();
}

void TSchedulingPolicy::BuildSchedulingAttributesStringForNode(
    TNodeId /*nodeId*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{
    YT_UNIMPLEMENTED();
}

void TSchedulingPolicy::BuildSchedulingAttributesForNode(TNodeId /*nodeId*/, TFluentMap /*fluent*/) const
{
    YT_UNIMPLEMENTED();
}

void TSchedulingPolicy::BuildSchedulingAttributesStringForOngoingAllocations(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const std::vector<TAllocationPtr>& /*allocations*/,
    TInstant /*now*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{
    YT_UNIMPLEMENTED();
}

void TSchedulingPolicy::BuildElementLoggingStringAttributes(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const TPoolTreeElement* /*element*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{
    YT_UNIMPLEMENTED();
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
{
    YT_UNIMPLEMENTED();
}

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
    return nullptr;
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

const TOperationMap& TSchedulingPolicy::Operations() const
{
    return EnabledOperations_;
}

const TNodeMap& TSchedulingPolicy::Nodes() const
{
    return Nodes_;
}

TGpuPlanUpdateStatisticsPtr TSchedulingPolicy::Statistics() const
{
    return Statistics_;
}

void TSchedulingPolicy::UpdateAssignmentPlan()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

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

    Statistics_ = New<TGpuPlanUpdateStatistics>();

    {
        TForbidContextSwitchGuard contextSwitchGuard;
        auto treeSnapshot = host->GetTreeSnapshot();

        for (const auto& [_, operation] : EnabledOperations_) {
            UpdateOperationResources(operation, treeSnapshot);
        }

        for (const auto& [_, operation] : DisabledOperations_) {
            ResetOperationResources(operation);
        }

        Statistics_->UpdatingOperationResourcesDuration = Statistics_->Timer.GetElapsedTime();
    }

    TGpuAllocationAssignmentPlanUpdateExecutor updateExecutor(
        this,
        TInstant::Now(),
        Config_,
        Logger);
    updateExecutor.Run();

    LogSnapshotEvent();
    ProfileAssignmentPlanUpdating();
    UpdatePersistentState();
}

// TODO(eshcherbin): Optimize not to recalculate preemptible assignments and ready to assign resources from scratch.
void TSchedulingPolicy::UpdateOperationResources(
    const TOperationPtr& operation,
    const TPoolTreeSnapshotPtr& treeSnapshot)
{
    YT_VERIFY(operation->IsInitialized());

    // Reset disabled operation.
    const auto* operationElement = treeSnapshot->FindEnabledOperationElement(operation->GetId());
    if (!operationElement) {
        ResetOperationResources(operation);
        return;
    }

    operation->SetStarving(operationElement->GetStarvationStatus() != EStarvationStatus::NonStarving);
    if (operation->IsStarving()) {
        YT_LOG_DEBUG("Operation is starving (OperationId: %v)", operation->GetId());
    }

    auto convertToShare = [&] (const TJobResources& allocationResources) -> TResourceVector {
        return TResourceVector::FromJobResources(allocationResources, operationElement->GetTotalResourceLimits());
    };

    const auto& fairShare = operationElement->Attributes().FairShare.Total;

    // Update preemptible allocations.
    if (operation->IsFullHostModuleBound()) {
        operation->SetPreemptible(Dominates(TResourceVector::Epsilon(), fairShare));
    } else {
        auto sortedAssignments = GetItems(operation->Assignments());
        // TODO(eshcherbin): Sort assignments by allocation start time.
        std::ranges::sort(sortedAssignments, std::less<>(), [] (const TAssignmentPtr& assignment) { return assignment->AllocationGroupName; });

        TResourceVector usageShare;
        for (const auto& assignment : sortedAssignments) {
            // NB(yaishenka): Assignment is preemptible if total resource usage (including current assignment) is higher than fair share.
            usageShare += convertToShare(assignment->ResourceUsage);
            bool previousStatus = std::exchange(
                assignment->Preemptible,
                !Dominates(fairShare + TResourceVector::Epsilon(), usageShare));

            if (previousStatus != assignment->Preemptible) {
                YT_LOG_DEBUG(
                    "Changed assignment preemptible status (OperationId: %v, Preemptible: %v, FairShare: %v, UsageShare: %v)",
                    operation->GetId(),
                    assignment->Preemptible,
                    fairShare,
                    usageShare);
            }
        }
    }

    // Update ready to assign resources.
    const auto assignedUsageShare = convertToShare(operation->AssignedResourceUsage());
    TResourceVector readyToAssignShare;
    operation->ReadyToAssignGroupedNeededResources().clear();

    TResourceVector extraShare;
    operation->ExtraGroupedNeededResources().clear();

    // Preemptible FHMB operations do not deserve resources.
    if (operation->IsFullHostModuleBound() && operation->IsPreemptible()) {
        YT_LOG_DEBUG("Skipping FHMB operation because it is preemptible (OperationId: %v, FairShare: %v)", operation->GetId(), fairShare);
        return;
    }

    // For an operation we want to maintain the following invariant:
    //     ResourceUsage + EmptyAssignmentResources + ReadyToAssignResources ~= FairShare.
    // Note that ResourceUsage + EmptyAssignmentResources == AssignedResourceUsage.
    for (const auto& [neededAllocationGroupName, neededAllocationGroupResources] : GetGroupedNeededResources(operation, operationElement)) {
        auto readyToAssignIt = EmplaceOrCrash(
            operation->ReadyToAssignGroupedNeededResources(),
            neededAllocationGroupName,
            TAllocationGroupResources{.MinNeededResources = neededAllocationGroupResources.MinNeededResources});
        auto& readyToAssignResources = readyToAssignIt->second;

        auto extraIt = operation->ExtraGroupedNeededResources().emplace(
            neededAllocationGroupName,
            TAllocationGroupResources{.MinNeededResources = neededAllocationGroupResources.MinNeededResources}).first;
        auto& extraResources = extraIt->second;

        const auto allocationUsageShare = convertToShare(neededAllocationGroupResources.MinNeededResources);
        const auto emptyAssignmentCount = GetOrDefault(operation->EmptyAssignmentCountPerGroup(), neededAllocationGroupName);

        YT_LOG_DEBUG(
            "Updating operation resources for allocation group "
            "(OperationId: %v, AllocationGroup: %v, NeededAllocationCount: %v, MinNeededResources: %v, "
            "EmptyAssignmentCount: %v, FairShare: %v, AllocationUsageShare: %v)",
            operation->GetId(),
            neededAllocationGroupName,
            neededAllocationGroupResources.AllocationCount,
            neededAllocationGroupResources.MinNeededResources,
            emptyAssignmentCount,
            fairShare,
            allocationUsageShare);

        while (emptyAssignmentCount + readyToAssignResources.AllocationCount + extraResources.AllocationCount < neededAllocationGroupResources.AllocationCount) {
            auto sumOfUsageShare = assignedUsageShare + readyToAssignShare + extraShare + allocationUsageShare;
            bool belowFairShare = Dominates(fairShare + TResourceVector::Epsilon(), sumOfUsageShare);

            YT_LOG_DEBUG(
                "Checking if fair share is exceeded before adding another assignment "
                "(OperationId: %v, AllocationGroup: %v, AssignedUsageShare: %v, "
                "ReadyToAssignShare: %v, FairShare: %v, ExtraShare: %v, SumOfUsageShare %v, BelowFairShare: %v)",
                operation->GetId(),
                neededAllocationGroupName,
                assignedUsageShare,
                readyToAssignShare,
                fairShare,
                extraShare,
                sumOfUsageShare,
                belowFairShare);

            if (belowFairShare) {
                ++readyToAssignResources.AllocationCount;
                readyToAssignShare += allocationUsageShare;
            } else {
                if (operation->IsFullHostModuleBound()) {
                    break;
                }
                ++extraResources.AllocationCount;
                extraShare += allocationUsageShare;
            }
        }
    }
}

TAllocationGroupResourcesMap TSchedulingPolicy::GetGroupedNeededResources(
    const TOperationPtr& operation,
    const TPoolTreeOperationElement* operationElement) const
{
    // TODO(eshcherbin): In full mode just return operation's grouped needed resources.

    // NB(eshcherbin): This is a temporary hack. In dry-run mode operation's needed resources are not consistent
    // with the policy's percieved resource usage (because there are no allocations in the dry-run policy).
    // Thus, we need to approximate the known total resource demand by fake grouped needed resources.
    YT_VERIFY(operation->InitialGroupedNeededResources());

    if (operation->InitialGroupedNeededResources()->empty()) {
        return *operation->InitialGroupedNeededResources();
    }

    // For vanilla operations we pretend that they always want what they wanted in the very beginning.
    if (operation->GetType() == EOperationType::Vanilla) {
        return *operation->InitialGroupedNeededResources();
    }

    // For all other operations we pretend that all their allocations are uniform.
    const auto& [allocationGroupName, allocationGroup] = *operation->InitialGroupedNeededResources()->begin();
    auto approximateAllocationGroup = allocationGroup;
    approximateAllocationGroup.AllocationCount = 0;

    TJobResources approximateResourceDemand;
    while (!Dominates(approximateResourceDemand, operationElement->ResourceDemand()) &&
        approximateAllocationGroup.AllocationCount < allocationGroup.AllocationCount)
    {
        approximateResourceDemand += approximateAllocationGroup.MinNeededResources;
        ++approximateAllocationGroup.AllocationCount;
    }

    return TAllocationGroupResourcesMap{{allocationGroupName, approximateAllocationGroup}};
}

void TSchedulingPolicy::ResetOperationResources(const TOperationPtr& operation)
{
    if (!operation->IsInitialized()) {
        return;
    }

    operation->ReadyToAssignGroupedNeededResources().clear();

    if (operation->IsFullHostModuleBound()) {
        operation->SetPreemptible(true);
    } else {
        for (const auto& assignment : operation->Assignments()) {
            assignment->Preemptible = true;
        }
    }
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

void TSchedulingPolicy::ReviveNodeState(TNodeId nodeId, const TNodePtr& node)
{
    auto maybeState = FindInitialNodePersistentState(nodeId);
    if (!maybeState) {
        return;
    }

    if (maybeState->SchedulingModule) {
        node->SchedulingModule() = std::move(maybeState->SchedulingModule);
    }

    for (auto assignmentState : maybeState->AssignmentStates) {
        TOperationPtr operation = GetOrDefault(DisabledOperations_, assignmentState->OperationId);
        if (!operation) {
            operation = GetOrDefault(EnabledOperations_, assignmentState->OperationId);
        }

        if (operation) {
            auto assignment = New<TAssignment>(
                std::move(assignmentState->AllocationGroupName),
                std::move(assignmentState->ResourceUsage),
                operation.Get(),
                node.Get());
            assignment->Preemptible = assignmentState->Preemptible;

            node->AddAssignment(assignment);
            operation->AddAssignment(assignment);
            continue;
        }

        EmplaceOrCrash(InitialOperationAssignments_[assignmentState->OperationId], assignmentState);
    }

    YT_LOG_DEBUG(
        "Node state revived "
        "(NodeId: %v, NodeAddress: %v, SchedulingModule: %v)",
        nodeId,
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

    for (auto assignmentState : InitialOperationAssignments_[operation->GetId()]) {
        TNodePtr node = GetOrDefault(Nodes_, assignmentState->NodeId);
        if (!node) {
            YT_LOG_DEBUG(
                "Dropped assignment because node is missing "
                "(OperationId: %v, NodeId: %v, AllocationGroupName: %v)",
                operation->GetId(),
                assignmentState->NodeId,
                assignmentState->AllocationGroupName);

            continue;
        }

        if (operation->SchedulingModule() && node->SchedulingModule() != operation->SchedulingModule()) {
            YT_LOG_DEBUG(
                "Dropped assignment because node's scheduling module has changed "
                "(OperationId: %v, NodeId: %v, OldModule: %v, NewModule: %v)",
                operation->GetId(),
                assignmentState->NodeId,
                operation->SchedulingModule(),
                node->SchedulingModule());
            operation->SchedulingModule().reset();
            continue;
        }

        auto assignment = New<TAssignment>(
            std::move(assignmentState->AllocationGroupName),
            std::move(assignmentState->ResourceUsage),
            operation.Get(),
            node.Get());
        assignment->Preemptible = assignmentState->Preemptible;

        node->AddAssignment(assignment);
        operation->AddAssignment(assignment);
    }

    YT_LOG_DEBUG(
        "Operation state revived "
        "(OperationId: %v,SchedulingModule: %v)",
        operation->GetId(),
        operation->SchedulingModule());
}

//! Returns false if Now > InitializationFromPersistentStateDeadline_ and drops persistentState
//! Returns false if InitialPersistentState_ is empty
//! Returns true otherwise
bool TSchedulingPolicy::CheckInitializationTimeout()
{
    if (!InitialPersistentState_) {
        return false;
    }

    if (Y_LIKELY(InitialPersistentState_->NodeStates.empty() && InitialPersistentState_->OperationStates.empty())) {
        return false;
    }

    if (TInstant::Now() > InitializationFromPersistentStateDeadline_) {
        InitialPersistentState_.Reset();
        InitialOperationAssignments_.clear();
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

        for (const auto& assignment : node->Assignments()) {
            auto assignmentPersistentState = New<TPersistentAssignmentState>();
            assignmentPersistentState->NodeId = nodeId;
            assignmentPersistentState->OperationId = assignment->Operation->GetId();
            assignmentPersistentState->AllocationGroupName = assignment->AllocationGroupName;
            assignmentPersistentState->ResourceUsage = assignment->ResourceUsage;
            assignmentPersistentState->CreationTime = assignment->CreationTime;
            assignmentPersistentState->Preemptible = assignment->Preemptible;

            nodePersistentState.AssignmentStates.push_back(std::move(assignmentPersistentState));
        }

        YT_LOG_DEBUG("Updated node persistent state (NodeId: %v)", nodeId);
    }

    auto updateOperationPersistentState = [&] (auto it) {
        const auto& [operationId, operation] = it;
        auto& operationPersistentState = PersistentState_->OperationStates[operationId];
        operationPersistentState.SchedulingModule = operation->SchedulingModule();

        YT_LOG_DEBUG(
            "Updated operation persistent state (OperationId: %v, SchedulingModule %v,  Enabled %v)",
            operationId,
            operation->SchedulingModule(),
            operation->IsEnabled());
    };

    std::ranges::for_each(DisabledOperations_, updateOperationPersistentState);
    std::ranges::for_each(EnabledOperations_, updateOperationPersistentState);
}

void TSchedulingPolicy::LogSnapshotEvent() const
{
    LogStructuredGpuEventFluently(EGpuSchedulingLogEventType::ModulesInfo)
        .Item("modules").DoMapFor(Statistics_->ModuleStatistics, [] (TFluentMap fluent, const auto& item) {
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

void TSchedulingPolicy::ProfileAssignmentPlanUpdating()
{
    ProfilingCounters_.PlannedAssignments.Increment(Statistics_->PlannedAssignments);
    ProfilingCounters_.PreemptedAssignments.Increment(Statistics_->PreemptedAssignments);

    ProfilingCounters_.TotalPlanningTime.Record(Statistics_->Timer.GetElapsedTime());
    ProfilingCounters_.OperationResourcesUpdateTime.Record(Statistics_->UpdatingOperationResourcesDuration);
    ProfilingCounters_.FullHostPlanningTime.Record(Statistics_->FullHostPlanningDuration);
    ProfilingCounters_.ReguralPlanningTime.Record(Statistics_->ReguralPlanningDuration);
    ProfilingCounters_.ExtraPlanningTime.Record(Statistics_->ExtraPlanningDuration);

    ProfilingCounters_.EnabledOperations.Update(std::ssize(EnabledOperations_));

    for (const auto& [module, moduleStatistic] : Statistics_->ModuleStatistics) {
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

void TNoopSchedulingPolicy::UpdateNodeDescriptor(TNodeId /*nodeId*/, TExecNodeDescriptorPtr /*descriptor*/)
{ }

void TNoopSchedulingPolicy::ProcessSchedulingHeartbeat(
    const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    bool /*skipScheduleAllocations*/)
{
    YT_UNIMPLEMENTED();
}

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
    YT_UNIMPLEMENTED();
}

void TNoopSchedulingPolicy::EnableOperation(const TPoolTreeOperationElement* /*element*/)
{ }

void TNoopSchedulingPolicy::DisableOperation(TPoolTreeOperationElement* /*element*/, bool /*markAsNonAlive*/)
{ }

void TNoopSchedulingPolicy::RegisterAllocationsFromRevivedOperation(
    TPoolTreeOperationElement* /*element*/,
    std::vector<TAllocationPtr> /*allocations*/) const
{
    YT_UNIMPLEMENTED();
}

bool TNoopSchedulingPolicy::ProcessAllocationUpdate(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    TPoolTreeOperationElement* /*element*/,
    TAllocationId /*allocationId*/,
    const TJobResources& /*allocationResources*/,
    bool /*resetPreemptibleProgress*/,
    const std::optional<std::string>& /*allocationDataCenter*/,
    const std::optional<std::string>& /*allocationInfinibandCluster*/,
    std::optional<EAbortReason>* /*maybeAbortReason*/) const
{
    YT_UNIMPLEMENTED();
}

bool TNoopSchedulingPolicy::ProcessFinishedAllocation(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    TPoolTreeOperationElement* /*element*/,
    TAllocationId /*allocationId*/) const
{
    YT_UNIMPLEMENTED();
}

void TNoopSchedulingPolicy::BuildSchedulingAttributesStringForNode(
    TNodeId /*nodeId*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{
    YT_UNIMPLEMENTED();
}

void TNoopSchedulingPolicy::BuildSchedulingAttributesForNode(TNodeId /*nodeId*/, TFluentMap /*fluent*/) const
{
    YT_UNIMPLEMENTED();
}

void TNoopSchedulingPolicy::BuildSchedulingAttributesStringForOngoingAllocations(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const std::vector<TAllocationPtr>& /*allocations*/,
    TInstant /*now*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{
    YT_UNIMPLEMENTED();
}

void TNoopSchedulingPolicy::BuildElementLoggingStringAttributes(
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    const TPoolTreeElement* /*element*/,
    TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const
{
    YT_UNIMPLEMENTED();
}

void TNoopSchedulingPolicy::PopulateOrchidService(const TCompositeMapServicePtr& /*orchidService*/) const
{ }

void TNoopSchedulingPolicy::ProfileOperation(
    const TPoolTreeOperationElement* /*element*/,
    const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
    NProfiling::ISensorWriter* /*writer*/) const
{
    YT_UNIMPLEMENTED();
}

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
