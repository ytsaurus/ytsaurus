#include "scheduling_policy.h"

#include "private.h"
#include "assignment_plan_update.h"

#include <yt/yt/server/scheduler/strategy/policy/scheduling_heartbeat_context.h>
#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>
// TODO(eshcherbin): (!) Remove this include after TPostUpdateContext is not present in ISchedulingPolicy.
#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy_detail.h>

#include <yt/yt/server/scheduler/strategy/helpers.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

#include <yt/yt/server/scheduler/common/allocation.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TLogger GetLogger(const std::string& treeId)
{
    return GpuSchedulingPolicyLogger().WithTag("TreeId: %v", treeId);
}

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

class TSchedulingPolicy
    : public ISchedulingPolicy
    , public TAssignmentPlanContextBase
{
public:
    TSchedulingPolicy(
        TWeakPtr<ISchedulingPolicyHost> host,
        IStrategyHost* strategyHost,
        const std::string& treeId,
        TGpuSchedulingPolicyConfigPtr config)
        : TAssignmentPlanContextBase(GetLogger(treeId))
        , Host_(std::move(host))
        , StrategyHost_(strategyHost)
        , Logger(GetLogger(treeId))
        , Config_(std::move(config))
        , PlanUpdateExecutor_(New<TPeriodicExecutor>(
            StrategyHost_->GetControlInvoker(EControlQueue::GpuAssignmentPlanUpdate),
            BIND(&TSchedulingPolicy::UpdateAssignmentPlan, MakeWeak(this)),
            Config_->PlanUpdatePeriod))
    { }

    void Initialize() override
    {
        PlanUpdateExecutor_->Start();
    }

    void RegisterNode(TNodeId nodeId, const std::string& nodeAddress) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        EmplaceOrCrash(Nodes_, nodeId, New<TNode>(nodeAddress));

        YT_LOG_DEBUG("Node registered (NodeId: %v, NodeAddress: %v)",
            nodeId,
            nodeAddress);
    }

    void UnregisterNode(TNodeId nodeId) override
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

    void UpdateNodeDescriptor(TNodeId nodeId, TExecNodeDescriptorPtr descriptor) override
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

    void ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        bool /*skipScheduleAllocations*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void RegisterOperation(const TPoolTreeOperationElement* element) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = New<TOperation>(
            element->GetOperationId(),
            element->GetOperationType(),
            element->IsGang(),
            element->Spec()->SchedulingModules,
            element->GetSchedulingTagFilter());

        EmplaceOrCrash(DisabledOperations_, operation->GetId(), operation);

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

    void UnregisterOperation(const TPoolTreeOperationElement* element) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto it = GetIteratorOrCrash(DisabledOperations_, element->GetOperationId());
        const auto& operation = it->second;

        PreemptAllOperationAssignments(
            operation,
            EAllocationPreemptionReason::OperationUnregistered,
            "Node unregistered");

        DisabledOperations_.erase(it);

        YT_LOG_DEBUG("Operation unregistered (OperationId: %v)", element->GetOperationId());
    }

    TError OnOperationMaterialized(const TPoolTreeOperationElement* element) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = GetOrCrash(DisabledOperations_, element->GetOperationId());
        operation->Initialize(element->GetInitialGroupedNeededResources());

        YT_LOG_DEBUG("Operation materialized (OperationId: %v, InitialGroupedNeededResources: %v)",
            operation->GetId(),
            element->GetInitialGroupedNeededResources());

        return {};
    }

    TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* /*element*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void EnableOperation(const TPoolTreeOperationElement* element) override
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

    void DisableOperation(TPoolTreeOperationElement* element, bool /*markAsNonAlive*/) override
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

    void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* /*element*/,
        std::vector<TAllocationPtr> /*allocations*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    bool ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        TPoolTreeOperationElement* /*element*/,
        TAllocationId /*allocationId*/,
        const TJobResources& /*allocationResources*/,
        bool /*resetPreemptibleProgress*/,
        const std::optional<std::string>& /*allocationDataCenter*/,
        const std::optional<std::string>& /*allocationInfinibandCluster*/,
        std::optional<EAbortReason>* /*maybeAbortReason*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    bool ProcessFinishedAllocation(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        TPoolTreeOperationElement* /*element*/,
        TAllocationId /*allocationId*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildSchedulingAttributesStringForNode(
        TNodeId /*nodeId*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildSchedulingAttributesForNode(TNodeId /*nodeId*/, TFluentMap /*fluent*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildSchedulingAttributesStringForOngoingAllocations(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const std::vector<TAllocationPtr>& /*allocations*/,
        TInstant /*now*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const TPoolTreeElement* /*element*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void PopulateOrchidService(const TCompositeMapServicePtr& orchidService) const override
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

    void ProfileOperation(
        const TPoolTreeOperationElement* /*element*/,
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        NProfiling::ISensorWriter* /*writer*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    TPostUpdateContext CreatePostUpdateContext(TPoolTreeRootElement* /*rootElement*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void PostUpdate(
        TFairSharePostUpdateContext* /*fairSharePostUpdateContext*/,
        TPostUpdateContext* /*postUpdateContext*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContext* /*postUpdateContext*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void OnResourceUsageSnapshotUpdate(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const TResourceUsageSnapshotPtr& /*resourceUsageSnapshot*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void UpdateConfig(TStrategyTreeConfigPtr treeConfig) override
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

    void InitPersistentState(INodePtr /*persistentState*/) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_UNIMPLEMENTED();
    }

    INodePtr BuildPersistentState() const override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_UNIMPLEMENTED();
    }

    const TOperationMap& Operations() const override
    {
        return EnabledOperations_;
    }

    const TNodeMap& Nodes() const override
    {
        return Nodes_;
    }

private:
    const TWeakPtr<ISchedulingPolicyHost> Host_;
    IStrategyHost* const StrategyHost_;

    const NLogging::TLogger Logger;

    TGpuSchedulingPolicyConfigPtr Config_;

    TPeriodicExecutorPtr PlanUpdateExecutor_;

    TNodeMap Nodes_;
    TOperationMap EnabledOperations_;
    TOperationMap DisabledOperations_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void UpdateAssignmentPlan()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto host = Host_.Lock();
        if (!host) {
            return;
        }

        {
            TForbidContextSwitchGuard contextSwitchGuard;
            auto treeSnapshot = host->GetTreeSnapshot();

            for (const auto& [_, operation] : EnabledOperations_) {
                UpdateOperationResources(operation, treeSnapshot);
            }

            for (const auto& [_, operation] : DisabledOperations_) {
                ResetOperationResources(operation);
            }
        }

        TGpuAllocationAssignmentPlanUpdateExecutor updateExecutor(
            this,
            TInstant::Now(),
            Config_,
            Logger);
        updateExecutor.Run();
    }

    // TODO(eshcherbin): Optimize not to recalculate preemptible assignments and ready to assign resources from scratch.
    void UpdateOperationResources(
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

        auto convertToShare = [&] (const TJobResources& allocationResources) -> TResourceVector {
            return TResourceVector::FromJobResources(allocationResources, operationElement->GetTotalResourceLimits());
        };

        const auto& fairShare = operationElement->Attributes().FairShare.Total;

        // Update preemptible allocations.
        if (operation->IsFullHostModuleBound()) {
            operation->SetPreemptible(!operationElement->IsDemandFullySatisfied());
        } else {
            auto sortedAssignments = GetItems(operation->Assignments());
            // TODO(eshcherbin): Sort assignments by allocation start time.
            std::ranges::sort(sortedAssignments, std::less<>(), [] (const TAssignmentPtr& assignment) { return assignment->AllocationGroupName; });

            TResourceVector prefixUsageShare;
            for (const auto& assignment : sortedAssignments) {
                // NB(eshcherbin): Assignment is preemptible if the total resource usage of all assignments before it is lower than fair share.
                // In particular, the assignment which crosses the fair share boundary is not considered preemptible.
                // TODO(eshcherbin): (!) For map operations, the assignment on the boundary should be preemptible.
                assignment->Preemptible = Dominates(prefixUsageShare + TResourceVector::Epsilon(), fairShare);
                prefixUsageShare += convertToShare(assignment->ResourceUsage);
            }
        }

        // Update ready to assign resources.
        const auto assignedUsageShare = convertToShare(operation->AssignedResourceUsage());
        TResourceVector readyToAssignShare;
        operation->ReadyToAssignGroupedNeededResources().clear();

        // Preemptible FHMB operations do not deserve resources.
        if (operation->IsFullHostModuleBound() && operation->IsPreemptible()) {
            return;
        }

        // TODO(eshcherbin): Add grouped resource demand and use it instead of initial grouped needed resources.
        // Currently, this is a fallback, so that we are not blocked by this.
        for (const auto& [allocationGroupName, allocationGroupResources] : *operation->InitialGroupedNeededResources()) {
            auto it = operation->ReadyToAssignGroupedNeededResources().emplace(
                allocationGroupName,
                TAllocationGroupResources{.MinNeededResources = allocationGroupResources.MinNeededResources}).first;
            auto& readyToAssignResources = it->second;

            auto assignmentCount = GetOrDefault(operation->EmptyAssignmentCountPerGroup(), allocationGroupName);
            auto allocationUsageShare = convertToShare(allocationGroupResources.MinNeededResources);

            YT_LOG_DEBUG(
                "Updating operation resources for allocation group"
                "(OperationId: %v, AllocationGroup %v, MinNeededResources %v, FairShare: %v, AllocationUsageShare: %v)",
                operation->GetId(),
                allocationGroupName,
                allocationGroupResources.MinNeededResources,
                fairShare,
                allocationUsageShare);
            while (true) {
                bool noMoreAssignmentsNeeded = assignmentCount + readyToAssignResources.AllocationCount >= allocationGroupResources.AllocationCount;
                bool fairShareExceeded = Dominates(assignedUsageShare + readyToAssignShare + TResourceVector::Epsilon(), fairShare);

                // XXX(eshcherbin): This is a dirty hack, which we use to factor in the real demand of the operation.
                // Remove it after honest grouped resource demand is added.
                bool demandExceeded = Dominates(
                    assignedUsageShare + readyToAssignShare + allocationUsageShare,
                    operationElement->Attributes().DemandShare + TResourceVector::Epsilon());

                YT_LOG_DEBUG(
                    "Calculating allocationCount for allocationGroup"
                    "(OperationId %v, AllocationGroup %v, NoMoreAssignmentsNeeded %v, FairShareExceeded %v, DemandExceeded %v, ReadyToAssignShare %v)",
                    operation->GetId(),
                    allocationGroupName,
                    noMoreAssignmentsNeeded,
                    fairShareExceeded,
                    demandExceeded,
                    readyToAssignShare);

                if (noMoreAssignmentsNeeded || fairShareExceeded || demandExceeded) {
                    break;
                }

                ++readyToAssignResources.AllocationCount;
                readyToAssignShare += allocationUsageShare;
            }
        }
    }

    void ResetOperationResources(const TOperationPtr& operation)
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

    void PreemptAllNodeAssignments(
        const TNodePtr& node,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription)
    {
        // NB(eshcherbin): Copy assignments with |GetItems|, because the set will be modified.
        for (const auto& assignment : GetItems(node->Assignments())) {
            PreemptAssignment(assignment, preemptionReason, preemptionDescription);
        }
    }

    void PreemptAllOperationAssignments(
        const TOperationPtr& operation,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription)
    {
        for (const auto& assignment : GetItems(operation->Assignments())) {
            PreemptAssignment(assignment, preemptionReason, preemptionDescription);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TSchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

class TNoopSchedulingPolicy
    : public ISchedulingPolicy
{
public:
    explicit TNoopSchedulingPolicy(const std::string& treeId)
        : Logger(GetLogger(treeId))
    { }

    void Initialize() override
    { }

    void RegisterNode(TNodeId /*nodeId*/, const std::string& /*nodeAddress*/) override
    { }

    void UnregisterNode(TNodeId /*nodeId*/) override
    { }

    void UpdateNodeDescriptor(TNodeId /*nodeId*/, TExecNodeDescriptorPtr /*descriptor*/) override
    { }

    void ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        bool /*skipScheduleAllocations*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void RegisterOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void UnregisterOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    TError OnOperationMaterialized(const TPoolTreeOperationElement* /*element*/) override
    {
        return {};
    }

    TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* /*element*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void EnableOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void DisableOperation(TPoolTreeOperationElement* /*element*/, bool /*markAsNonAlive*/) override
    { }

    void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* /*element*/,
        std::vector<TAllocationPtr> /*allocations*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    bool ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        TPoolTreeOperationElement* /*element*/,
        TAllocationId /*allocationId*/,
        const TJobResources& /*allocationResources*/,
        bool /*resetPreemptibleProgress*/,
        const std::optional<std::string>& /*allocationDataCenter*/,
        const std::optional<std::string>& /*allocationInfinibandCluster*/,
        std::optional<EAbortReason>* /*maybeAbortReason*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    bool ProcessFinishedAllocation(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        TPoolTreeOperationElement* /*element*/,
        TAllocationId /*allocationId*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildSchedulingAttributesStringForNode(
        TNodeId /*nodeId*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildSchedulingAttributesForNode(TNodeId /*nodeId*/, TFluentMap /*fluent*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildSchedulingAttributesStringForOngoingAllocations(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const std::vector<TAllocationPtr>& /*allocations*/,
        TInstant /*now*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const TPoolTreeElement* /*element*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void PopulateOrchidService(const TCompositeMapServicePtr& /*orchidService*/) const override
    { }

    void ProfileOperation(
        const TPoolTreeOperationElement* /*element*/,
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        NProfiling::ISensorWriter* /*writer*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    TPostUpdateContext CreatePostUpdateContext(TPoolTreeRootElement* /*rootElement*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void PostUpdate(
        TFairSharePostUpdateContext* /*fairSharePostUpdateContext*/,
        TPostUpdateContext* /*postUpdateContext*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContext* /*postUpdateContext*/) override
    {
        YT_UNIMPLEMENTED();
    }

    virtual void OnResourceUsageSnapshotUpdate(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const TResourceUsageSnapshotPtr& /*resourceUsageSnapshot*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void UpdateConfig(TStrategyTreeConfigPtr config) override
    {
        if (EGpuSchedulingPolicyMode::Noop != config->GpuSchedulingPolicy->Mode) {
            YT_LOG_WARNING("GPU scheduling policy config update failed because mode has changed (OldMode: %v, NewMode: %v)",
                EGpuSchedulingPolicyMode::Noop,
                config->GpuSchedulingPolicy->Mode);
            return;
        }
    }

    void InitPersistentState(INodePtr /*persistentState*/) override
    {
        YT_UNIMPLEMENTED();
    }

    INodePtr BuildPersistentState() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TNoopSchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateSchedulingPolicy(
    TWeakPtr<ISchedulingPolicyHost> host,
    IStrategyHost* strategyHost,
    const std::string& treeId,
    const TStrategyTreeConfigPtr& config)
{
    const auto& Logger = GetLogger(treeId);

    switch (config->GpuSchedulingPolicy->Mode) {
        case EGpuSchedulingPolicyMode::Noop:
            return New<TNoopSchedulingPolicy>(treeId);
        case EGpuSchedulingPolicyMode::DryRun: {
            YT_LOG_WARNING_UNLESS(IsGpuPoolTree(config),
                "GPU scheduling policy configured for a non-GPU pool tree");

            return New<TSchedulingPolicy>(
                std::move(host),
                strategyHost,
                treeId,
                config->GpuSchedulingPolicy);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
