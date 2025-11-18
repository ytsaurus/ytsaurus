#include "scheduling_policy.h"

#include "private.h"
#include "assignment_plan_update.h"

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>

#include <yt/yt/server/scheduler/strategy/helpers.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

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
            // TODO(eshcherbin): Create a separate bucket for GPU scheduling policy.
            StrategyHost_->GetControlInvoker(EControlQueue::Strategy),
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

    void RegisterOperation(const TPoolTreeOperationElement* element) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = New<TOperation>(
            element->GetOperationId(),
            element->GetOperationType(),
            element->IsGang(),
            element->Spec()->SchedulingModules);

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

    void OnOperationMaterialized(const TPoolTreeOperationElement* element) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto operation = GetOrCrash(DisabledOperations_, element->GetOperationId());
        operation->Initialize(element->GetInitialGroupedNeededResources());

        YT_LOG_DEBUG("Operation materialized (OperationId: %v, InitialGroupedNeededResources: %v)",
            operation->GetId(),
            element->GetInitialGroupedNeededResources());
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

    void DisableOperation(const TPoolTreeOperationElement* element) override
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

    void UpdateConfig(TGpuSchedulingPolicyConfigPtr config) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (Config_->Mode != config->Mode) {
            YT_LOG_WARNING("Scheduling policy config update failed because mode has changed (OldMode: %v, NewMode: %v)",
                Config_->Mode,
                config->Mode);
            return;
        }

        Config_ = std::move(config);

        PlanUpdateExecutor_->SetPeriod(Config_->PlanUpdatePeriod);
    }

    void InitPersistentState(INodePtr /*persistentState*/)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_UNIMPLEMENTED();
    }

    INodePtr BuildPersistentState() const
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

        // TODO(eshcherbin): (!) Consider changes in needed resources for empty assignments.

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
                // TODO(eshcherbin): For map operations, the assignment on the boundary should be preemptible.
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

            auto assignmentCount = GetOrDefault(operation->AssignmentCountPerGroup(), allocationGroupName);
            auto allocationUsageShare = convertToShare(allocationGroupResources.MinNeededResources);
            while (true) {
                bool noMoreAssignmentsNeeded = assignmentCount >= allocationGroupResources.AllocationCount;
                bool fairShareExceeded = Dominates(assignedUsageShare + readyToAssignShare + TResourceVector::Epsilon(), fairShare);

                // XXX(eshcherbin): This is a dirty hack, which we use to factor in the real demand of the operation.
                // Remove it after honest grouped resource demand is added.
                bool demandExceeded = Dominates(
                    assignedUsageShare + readyToAssignShare + allocationUsageShare,
                    operationElement->Attributes().DemandShare + TResourceVector::Epsilon());

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

    void RegisterOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void UnregisterOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void OnOperationMaterialized(const TPoolTreeOperationElement* /*element*/) override
    { }

    void EnableOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void DisableOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void PopulateOrchidService(const TCompositeMapServicePtr& /*orchidService*/) const override
    { }

    void UpdateConfig(TGpuSchedulingPolicyConfigPtr config) override
    {
        if (EGpuSchedulingPolicyMode::Noop != config->Mode) {
            YT_LOG_WARNING("Scheduling policy config update failed because mode has changed (OldMode: %v, NewMode: %v)",
                EGpuSchedulingPolicyMode::Noop,
                config->Mode);
            return;
        }
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
    if (IsGpuPoolTree(config) && config->GpuSchedulingPolicy->Mode == EGpuSchedulingPolicyMode::DryRun) {
        return New<TSchedulingPolicy>(
            std::move(host),
            strategyHost,
            treeId,
            config->GpuSchedulingPolicy);
    }

    return New<TNoopSchedulingPolicy>(treeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
