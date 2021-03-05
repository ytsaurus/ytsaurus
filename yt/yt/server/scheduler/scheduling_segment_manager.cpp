#include "scheduling_segment_manager.h"
#include "private.h"
#include "persistent_scheduler_state.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <util/generic/algorithm.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

double GetNodeResourceLimit(const TExecNodeDescriptor& node, EJobResourceType resourceType)
{
    return node.Online
        ? GetResource(node.ResourceLimits, resourceType)
        : 0.0;
}

TLogger CreateTreeLogger(const TString& treeId)
{
    return Logger.WithTag("TreeId: %v", treeId);
}

////////////////////////////////////////////////////////////////////////////////

TNodeSchedulingSegmentManager::TNodeSchedulingSegmentManager()
    : BufferedProducer_(New<TBufferedProducer>())
{ }

EJobResourceType TNodeSchedulingSegmentManager::GetSegmentBalancingKeyResource(ESegmentedSchedulingMode mode)
{
    switch (mode) {
        case ESegmentedSchedulingMode::LargeGpu:
            return EJobResourceType::Gpu;
        default:
            YT_ABORT();
    }
}

void TNodeSchedulingSegmentManager::ManageNodeSegments(TManageNodeSchedulingSegmentsContext* context)
{
    TSensorBuffer sensorBuffer;
    for (const auto& [treeId, strategyTreeState] : context->StrategySegmentsState.TreeStates) {
        auto& treePersistentAttributes = TreeIdToPersistentAttributes_[treeId];

        if (strategyTreeState.Mode == ESegmentedSchedulingMode::Disabled) {
            if (treePersistentAttributes.PreviousMode != ESegmentedSchedulingMode::Disabled) {
                ResetTree(context, treeId);
            }

            LogAndProfileSegmentsInTree(context, treeId, /* currentResourceAmountPerSegment */ {}, &sensorBuffer);

            continue;
        }

        auto keyResource = GetSegmentBalancingKeyResource(strategyTreeState.Mode);
        YT_VERIFY(strategyTreeState.KeyResource == keyResource);

        TSegmentToResourceAmount currentResourceAmountPerSegment;
        for (auto nodeId : context->NodeIdsPerTree[treeId]) {
            const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
            auto resourceAmountOnNode = GetNodeResourceLimit(node, keyResource);
            auto& currentResourceAmount = IsDataCenterAwareSchedulingSegment(node.SchedulingSegment)
                ? currentResourceAmountPerSegment.At(node.SchedulingSegment).MutableAt(node.DataCenter)
                : currentResourceAmountPerSegment.At(node.SchedulingSegment).Mutable();
            currentResourceAmount += resourceAmountOnNode;
        }

        LogAndProfileSegmentsInTree(context, treeId, currentResourceAmountPerSegment, &sensorBuffer);

        auto [isSegmentUnsatisfied, hasUnsatisfiedSegment] = FindUnsatisfiedSegmentsInTree(context, treeId, currentResourceAmountPerSegment);

        auto& unsatisfiedSince = treePersistentAttributes.UnsatisfiedSince;
        if (!hasUnsatisfiedSegment) {
            unsatisfiedSince = std::nullopt;
            continue;
        }

        if (!unsatisfiedSince) {
            unsatisfiedSince = context->Now;
        }

        YT_LOG_DEBUG(
            "Found unsatisfied scheduling segments in tree "
            "(TreeId: %v, IsSegmentUnsatisfied: %v, UnsatisfiedFor: %v, Timeout: %v)",
            treeId,
            isSegmentUnsatisfied,
            context->Now - *unsatisfiedSince,
            strategyTreeState.UnsatisfiedSegmentsRebalancingTimeout);

        auto deadline = std::max(
            *unsatisfiedSince + strategyTreeState.UnsatisfiedSegmentsRebalancingTimeout,
            NodeSegmentsInitializationDeadline_);
        if (context->Now > deadline) {
            RebalanceSegmentsInTree(
                context,
                treeId,
                std::move(currentResourceAmountPerSegment));
            unsatisfiedSince = std::nullopt;
        }

        treePersistentAttributes.PreviousMode = strategyTreeState.Mode;
    }

    BufferedProducer_->Update(std::move(sensorBuffer));
}

TPersistentNodeSchedulingSegmentStateMap TNodeSchedulingSegmentManager::BuildPersistentNodeSegmentsState(
    TManageNodeSchedulingSegmentsContext* context) const
{
    TPersistentNodeSchedulingSegmentStateMap nodeStates;
    for (const auto& [treeId, treeNodeIds] : context->NodeIdsPerTree) {
        for (auto nodeId : treeNodeIds) {
            auto it = context->ExecNodeDescriptors->find(nodeId);
            if (it == context->ExecNodeDescriptors->end()) {
                // NB(eshcherbin): This should not happen usually but the exec node descriptors map here might differ
                // from the one used for rebalancing, because we need to update the segments at the moved nodes.
                // So I don't want to put any hard constraints here.
                continue;
            }
            const auto& node = it->second;

            if (node.SchedulingSegment != ESchedulingSegment::Default) {
                YT_VERIFY(nodeStates.emplace(
                    nodeId,
                    TPersistentNodeSchedulingSegmentState{
                        .Segment = node.SchedulingSegment,
                        .Address = node.Address,
                        .Tree = treeId,
                    }).second);
            }
        }
    }

    return nodeStates;
}

void TNodeSchedulingSegmentManager::SetProfilingEnabled(bool enabled)
{
    if (enabled) {
        BufferedProducer_ = New<TBufferedProducer>();
        SchedulerProfiler.AddProducer("/segments", BufferedProducer_);
    } else {
        BufferedProducer_.Reset();
    }
}

void TNodeSchedulingSegmentManager::ResetTree(TManageNodeSchedulingSegmentsContext* context, const TString& treeId)
{
    auto& treePersistentAttributes = TreeIdToPersistentAttributes_[treeId];
    treePersistentAttributes.PreviousMode = ESegmentedSchedulingMode::Disabled;
    treePersistentAttributes.UnsatisfiedSince = std::nullopt;

    for (auto nodeId : context->NodeIdsPerTree[treeId]) {
        const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
        if (node.SchedulingSegment != ESchedulingSegment::Default) {
            // NB(eshcherbin): Nodes with frozen segments won't be moved to the default segment by this.
            auto nodeShardId = context->NodeShardHost->GetNodeShardId(nodeId);
            context->MovedNodesPerNodeShard[nodeShardId].push_back(TSetNodeSchedulingSegmentOptions{
                .NodeId = nodeId,
                .Segment = ESchedulingSegment::Default
            });
        }
    }
}

void TNodeSchedulingSegmentManager::LogAndProfileSegmentsInTree(
    TManageNodeSchedulingSegmentsContext* context,
    const TString& treeId,
    const TSegmentToResourceAmount& currentResourceAmountPerSegment,
    ISensorWriter* sensorWriter) const
{
    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];
    bool segmentedSchedulingEnabled = strategyTreeState.Mode != ESegmentedSchedulingMode::Disabled;

    if (segmentedSchedulingEnabled) {
        YT_LOG_DEBUG(
            "Scheduling segments state in tree "
            "(TreeId: %v, Mode: %v, DataCenters: %v, KeyResource: %v, FairSharePerSegment: %v, TotalKeyResourceAmount: %v, "
            "FairResourceAmountPerSegment: %v, CurrentResourceAmountPerSegment: %v)",
            treeId,
            strategyTreeState.Mode,
            strategyTreeState.DataCenters,
            GetSegmentBalancingKeyResource(strategyTreeState.Mode),
            strategyTreeState.FairSharePerSegment,
            strategyTreeState.TotalKeyResourceAmount,
            strategyTreeState.FairResourceAmountPerSegment,
            currentResourceAmountPerSegment);
    } else {
        YT_LOG_DEBUG("Segmented scheduling is disabled in tree, skipping (TreeId: %v)",
            treeId);
    }

    TWithTagGuard treeTagGuard(sensorWriter, TTag{ProfilingPoolTreeKey, treeId});
    if (segmentedSchedulingEnabled) {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            auto profileResourceAmountPerSegment = [&] (const TString& sensorName, const TSegmentToResourceAmount& resourceAmountMap) {
                const auto& valueAtSegment = resourceAmountMap.At(segment);
                if (IsDataCenterAwareSchedulingSegment(segment)) {
                    for (const auto& dataCenter : strategyTreeState.DataCenters) {
                        TWithTagGuard guard(sensorWriter, TTag{"data_center", ToString(dataCenter)});
                        sensorWriter->AddGauge(sensorName, valueAtSegment.GetOrDefaultAt(dataCenter));
                    }
                } else {
                    sensorWriter->AddGauge(sensorName, valueAtSegment.GetOrDefault());
                }
            };

            TWithTagGuard guard(sensorWriter, TTag{"segment", FormatEnum(segment)});
            profileResourceAmountPerSegment("/fair_resource_amount", strategyTreeState.FairResourceAmountPerSegment);
            profileResourceAmountPerSegment("/current_resource_amount", currentResourceAmountPerSegment);
        }
    } else {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            TWithTagGuard guard(sensorWriter, TTag{"segment", FormatEnum(segment)});
            if (IsDataCenterAwareSchedulingSegment(segment)) {
                guard.AddTag(TTag{"data_center", ToString(NullDataCenter)});
            }

            sensorWriter->AddGauge("/fair_resource_amount", 0.0);
            sensorWriter->AddGauge("/current_resource_amount", 0.0);
        }
    }
}

void TNodeSchedulingSegmentManager::RebalanceSegmentsInTree(
    TManageNodeSchedulingSegmentsContext* context,
    const TString& treeId,
    TSegmentToResourceAmount currentResourceAmountPerSegment)
{
    auto Logger = CreateTreeLogger(treeId);

    YT_LOG_DEBUG("Rebalancing node scheduling segments in tree");

    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(strategyTreeState.Mode);

    auto getPenalty = CreatePenaltyFunction(context, treeId);
    TSchedulingSegmentMap<int> addedNodeCountPerSegment;
    TSchedulingSegmentMap<int> removedNodeCountPerSegment;
    double totalPenalty = 0.0;
    int totalMovedNodeCount = 0;

    auto trySatisfySegment = [&] (
        ESchedulingSegment segment,
        double& currentResourceAmount,
        double fairResourceAmount,
        std::vector<TExecNodeDescriptor>& availableNodes)
    {
        while (currentResourceAmount < fairResourceAmount) {
            if (availableNodes.empty()) {
                break;
            }

            // NB(eshcherbin): |availableNodes| is sorted in order of decreasing penalty.
            auto nextAvailableNode = availableNodes.back();
            availableNodes.pop_back();

            auto resourceAmountOnNode = GetNodeResourceLimit(nextAvailableNode, keyResource);
            auto oldSegment = nextAvailableNode.SchedulingSegment;

            auto nodeShardId = context->NodeShardHost->GetNodeShardId(nextAvailableNode.Id);
            context->MovedNodesPerNodeShard[nodeShardId].push_back(TSetNodeSchedulingSegmentOptions{
                .NodeId = nextAvailableNode.Id,
                .Segment = segment});
            ++totalMovedNodeCount;
            totalPenalty += getPenalty(nextAvailableNode);

            const auto& dataCenter = nextAvailableNode.DataCenter;
            currentResourceAmount += resourceAmountOnNode;
            if (IsDataCenterAwareSchedulingSegment(segment)) {
                ++addedNodeCountPerSegment.At(segment).MutableAt(dataCenter);
            } else {
                ++addedNodeCountPerSegment.At(segment).Mutable();
            }

            if (IsDataCenterAwareSchedulingSegment(oldSegment)) {
                currentResourceAmountPerSegment.At(oldSegment).MutableAt(dataCenter) -= resourceAmountOnNode;
                ++removedNodeCountPerSegment.At(oldSegment).MutableAt(dataCenter);
            } else {
                currentResourceAmountPerSegment.At(oldSegment).Mutable() -= resourceAmountOnNode;
                ++removedNodeCountPerSegment.At(oldSegment).Mutable();
            }
        }
    };

    // Every node has a penalty for moving it to another segment. We collect a set of movable nodes
    // iteratively by taking the node with the lowest penalty until the remaining nodes can no longer
    // satisfy the fair resource amount determined by the strategy.
    // In addition, the rest of the nodes are called aggressively movable if the current segment is not cross-DC.
    // The intuition is that we should be able to compensate for a loss of such a node from one DC by moving
    // a node from another DC to the segment.
    THashMap<TDataCenter, std::vector<TExecNodeDescriptor>> movableNodesPerDataCenter;
    THashMap<TDataCenter, std::vector<TExecNodeDescriptor>> aggressivelyMovableNodesPerDataCenter;
    GetMovableNodesInTree(
        context,
        treeId,
        currentResourceAmountPerSegment,
        &movableNodesPerDataCenter,
        &aggressivelyMovableNodesPerDataCenter);

    // First, we try to satisfy all cross-DC segments, one DC at a time.
    // During this phase we are allowed to use the nodes from |aggressivelyMovableNodesPerDataCenter|
    // if |movableNodesPerDataCenter| is exhausted.
    for (const auto& dataCenter : strategyTreeState.DataCenters) {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            if (!IsDataCenterAwareSchedulingSegment(segment)) {
                continue;
            }

            auto& currentResourceAmount = currentResourceAmountPerSegment.At(segment).MutableAt(dataCenter);
            auto fairResourceAmount = strategyTreeState.FairResourceAmountPerSegment.At(segment).GetOrDefaultAt(dataCenter);
            trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, movableNodesPerDataCenter[dataCenter]);
            trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, aggressivelyMovableNodesPerDataCenter[dataCenter]);
        }
    }

    std::vector<TExecNodeDescriptor> movableNodes;
    for (const auto& [_, movableNodesAtDataCenter] : movableNodesPerDataCenter) {
        std::move(movableNodesAtDataCenter.begin(), movableNodesAtDataCenter.end(), std::back_inserter(movableNodes));
    }
    SortBy(movableNodes, getPenalty);

    // Second, we try to satisfy all other segments using the remaining movable nodes.
    // Note that some segments might have become unsatisfied during the first phase
    // if we used any nodes from |aggressivelyMovableNodesPerDataCenter|.
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsDataCenterAwareSchedulingSegment(segment)) {
            continue;
        }

        auto& currentResourceAmount = currentResourceAmountPerSegment.At(segment).Mutable();
        auto fairResourceAmount = strategyTreeState.FairResourceAmountPerSegment.At(segment).GetOrDefault();
        trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, movableNodes);
    }

    auto [isSegmentUnsatisfied, hasUnsatisfiedSegment] = FindUnsatisfiedSegmentsInTree(context, treeId, currentResourceAmountPerSegment);
    YT_LOG_WARNING_IF(hasUnsatisfiedSegment,
        "Failed to satisfy all scheduling segments during rebalancing (IsSegmentUnsatisfied: %v)",
        isSegmentUnsatisfied);

    YT_LOG_DEBUG(
        "Finished node scheduling segments rebalancing "
        "(TotalMovedNodeCount: %v, AddedNodeCountPerSegment: %v, RemovedNodeCountPerSegment: %v, "
        "NewResourceAmountPerSegment: %v, TotalPenalty: %v)",
        totalMovedNodeCount,
        addedNodeCountPerSegment,
        removedNodeCountPerSegment,
        currentResourceAmountPerSegment,
        totalPenalty);
}

void TNodeSchedulingSegmentManager::GetMovableNodesInTree(
    TManageNodeSchedulingSegmentsContext* context,
    const TString& treeId,
    const TSegmentToResourceAmount& currentResourceAmountPerSegment,
    THashMap<TDataCenter, std::vector<TExecNodeDescriptor>>* movableNodesPerDataCenter,
    THashMap<TDataCenter, std::vector<TExecNodeDescriptor>>* aggressivelyMovableNodesPerDataCenter)
{
    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(strategyTreeState.Mode);
    auto getPenalty = CreatePenaltyFunction(context, treeId);

    TSchedulingSegmentMap<std::vector<TNodeId>> nodeIdsPerSegment;
    for (auto nodeId : context->NodeIdsPerTree[treeId]) {
        const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
        auto& nodeIds = nodeIdsPerSegment.At(node.SchedulingSegment);
        if (IsDataCenterAwareSchedulingSegment(node.SchedulingSegment)) {
            nodeIds.MutableAt(node.DataCenter).push_back(nodeId);
        } else {
            nodeIds.Mutable().push_back(nodeId);
        }
    }

    auto collectMovableNodes = [&] (double currentResourceAmount, double fairResourceAmount, const std::vector<TNodeId>& nodeIds) {
        std::vector<TExecNodeDescriptor> segmentNodes;
        segmentNodes.reserve(nodeIds.size());
        for (auto nodeId : nodeIds) {
            segmentNodes.push_back(GetOrCrash(*context->ExecNodeDescriptors, nodeId));
        }

        SortBy(segmentNodes, getPenalty);

        for (const auto& node : segmentNodes) {
            if (node.SchedulingSegmentFrozen) {
                continue;
            }

            auto resourceAmountOnNode = GetNodeResourceLimit(node, keyResource);
            currentResourceAmount -= resourceAmountOnNode;
            if (currentResourceAmount >= fairResourceAmount) {
                (*movableNodesPerDataCenter)[node.DataCenter].push_back(node);
            } else if (!IsDataCenterAwareSchedulingSegment(node.SchedulingSegment)) {
                (*aggressivelyMovableNodesPerDataCenter)[node.DataCenter].push_back(node);
            }
        }
    };

    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        const auto& currentResourceAmount = currentResourceAmountPerSegment.At(segment);
        const auto& fairResourceAmount = strategyTreeState.FairResourceAmountPerSegment.At(segment);
        const auto& nodeIds = nodeIdsPerSegment.At(segment);
        if (IsDataCenterAwareSchedulingSegment(segment)) {
            for (const auto& dataCenter : strategyTreeState.DataCenters) {
                collectMovableNodes(
                    currentResourceAmount.GetOrDefaultAt(dataCenter),
                    fairResourceAmount.GetOrDefaultAt(dataCenter),
                    nodeIds.GetOrDefaultAt(dataCenter));
            }
        } else {
            collectMovableNodes(
                currentResourceAmount.GetOrDefault(),
                fairResourceAmount.GetOrDefault(),
                nodeIds.GetOrDefault());
        }
    }

    auto sortAndReverseMovableNodes = [&] (auto& movableNodes) {
        for (const auto& dataCenter : strategyTreeState.DataCenters) {
            SortBy(movableNodes[dataCenter], getPenalty);
            std::reverse(movableNodes[dataCenter].begin(), movableNodes[dataCenter].end());
        }
    };
    sortAndReverseMovableNodes(*movableNodesPerDataCenter);
    sortAndReverseMovableNodes(*aggressivelyMovableNodesPerDataCenter);
}

TChangeNodeSegmentPenaltyFunction TNodeSchedulingSegmentManager::CreatePenaltyFunction(
    TManageNodeSchedulingSegmentsContext* context,
    const TString& treeId) const
{
    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(strategyTreeState.Mode);
    switch (keyResource) {
        case EJobResourceType::Gpu:
            return [] (const TExecNodeDescriptor& node) { return node.RunningJobStatistics.TotalGpuTime; };
        default:
            return [] (const TExecNodeDescriptor& node) { return node.RunningJobStatistics.TotalCpuTime; };
    }
}

std::pair<TSchedulingSegmentMap<bool>, bool> TNodeSchedulingSegmentManager::FindUnsatisfiedSegmentsInTree(
    TManageNodeSchedulingSegmentsContext *context,
    const TString& treeId,
    const TSegmentToResourceAmount& currentResourceAmountPerSegment) const
{
    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];

    TSchedulingSegmentMap<bool> isSegmentUnsatisfied;
    bool hasUnsatisfiedSegment = false;
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        const auto& fairResourceAmount = strategyTreeState.FairResourceAmountPerSegment.At(segment);
        const auto& currentResourceAmount = currentResourceAmountPerSegment.At(segment);
        if (IsDataCenterAwareSchedulingSegment(segment)) {
            for (const auto& dataCenter : strategyTreeState.DataCenters) {
                if (currentResourceAmount.GetOrDefaultAt(dataCenter) < fairResourceAmount.GetOrDefaultAt(dataCenter)) {
                    hasUnsatisfiedSegment = true;
                    isSegmentUnsatisfied.At(segment).SetAt(dataCenter, true);
                }
            }
        } else if (currentResourceAmount.GetOrDefault() < fairResourceAmount.GetOrDefault()) {
            hasUnsatisfiedSegment = true;
            isSegmentUnsatisfied.At(segment).Set(true);
        }
    }

    return {std::move(isSegmentUnsatisfied), hasUnsatisfiedSegment};
}

////////////////////////////////////////////////////////////////////////////////

static constexpr int LargeGpuSegmentJobGpuDemand = 8;

////////////////////////////////////////////////////////////////////////////////

ESchedulingSegment TStrategySchedulingSegmentManager::GetSegmentForOperation(
    ESegmentedSchedulingMode mode,
    const TJobResources& operationMinNeededResources)
{
    switch (mode) {
        case ESegmentedSchedulingMode::LargeGpu:
            return operationMinNeededResources.GetGpu() == LargeGpuSegmentJobGpuDemand
                ? ESchedulingSegment::LargeGpu
                : ESchedulingSegment::Default;
        default:
            return ESchedulingSegment::Default;
    }
}

void TStrategySchedulingSegmentManager::ManageSegmentsInTree(TManageTreeSchedulingSegmentsContext* context, const TString& treeId)
{
    auto Logger = CreateTreeLogger(treeId);

    auto& state = context->SchedulingSegmentsState;
    auto treeConfig = context->TreeConfig;
    state.Mode = treeConfig->SchedulingSegments->Mode;
    state.UnsatisfiedSegmentsRebalancingTimeout = treeConfig->SchedulingSegments->UnsatisfiedSegmentsRebalancingTimeout;

    if (state.Mode == ESegmentedSchedulingMode::Disabled) {
        return;
    }

    auto keyResource = TNodeSchedulingSegmentManager::GetSegmentBalancingKeyResource(state.Mode);
    state.KeyResource = keyResource;
    state.TotalKeyResourceAmount = GetResource(context->TotalResourceLimits, keyResource);
    for (const auto& dataCenter : treeConfig->SchedulingSegments->DataCenters) {
        state.DataCenters.push_back(dataCenter);
    }

    double expectedTotalKeyResourceAmount = 0.0;
    for (const auto& [_, resourceLimitsAtDataCenter] : context->ResourceLimitsPerDataCenter) {
        expectedTotalKeyResourceAmount += GetResource(resourceLimitsAtDataCenter, keyResource);
    }
    YT_LOG_WARNING_IF(expectedTotalKeyResourceAmount != state.TotalKeyResourceAmount,
        "Total key resource amount differs from the sum of provided per-DC limits, "
        "operation scheduling segments distribution might be unfair "
        "(TotalKeyResourceAmount: %v, ExpectedTotalKeyResourceAmount: %v, KeyResource: %v)",
        state.TotalKeyResourceAmount,
        expectedTotalKeyResourceAmount,
        keyResource);

    ResetOperationDataCenterAssignmentsInTree(context, treeId);

    CollectFairSharePerSegmentInTree(context);

    THashMap<TDataCenter, double> totalCapacityPerDataCenter;
    THashMap<TDataCenter, double> remainingCapacityPerDataCenter;
    for (const auto& dataCenter : state.DataCenters) {
        YT_VERIFY(dataCenter);

        auto capacity = GetResource(context->ResourceLimitsPerDataCenter[*dataCenter], keyResource);
        totalCapacityPerDataCenter.emplace(*dataCenter, capacity);
        remainingCapacityPerDataCenter.emplace(*dataCenter, capacity);
    }

    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsDataCenterAwareSchedulingSegment(segment)) {
            for (const auto& dataCenter : state.DataCenters) {
                auto fairResourceAmount = state.FairSharePerSegment.At(segment).GetOrDefaultAt(dataCenter) * state.TotalKeyResourceAmount;
                state.FairResourceAmountPerSegment.At(segment).SetAt(dataCenter, fairResourceAmount);
                remainingCapacityPerDataCenter[dataCenter] -= fairResourceAmount;
            }
        } else {
            auto fairResourceAmount = state.FairSharePerSegment.At(segment).GetOrDefault() * state.TotalKeyResourceAmount;
            state.FairResourceAmountPerSegment.At(segment).Set(fairResourceAmount);
        }
    }

    AssignOperationsToDataCentersInTree(context, treeId, totalCapacityPerDataCenter, remainingCapacityPerDataCenter);

    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsDataCenterAwareSchedulingSegment(segment)) {
            for (const auto& dataCenter : state.DataCenters) {
                auto satisfactionMargin = treeConfig->SchedulingSegments->SatisfactionMargins.At(segment).GetOrDefaultAt(dataCenter);
                auto& value = state.FairResourceAmountPerSegment.At(segment).MutableAt(dataCenter);
                value = std::max(value + satisfactionMargin, 0.0);
            }
        } else {
            auto satisfactionMargin = treeConfig->SchedulingSegments->SatisfactionMargins.At(segment).GetOrDefault();
            auto& value = state.FairResourceAmountPerSegment.At(segment).Mutable();
            value = std::max(value + satisfactionMargin, 0.0);
        }
    }
}

void TStrategySchedulingSegmentManager::ResetOperationDataCenterAssignmentsInTree(TManageTreeSchedulingSegmentsContext* context, const TString& treeId)
{
    auto Logger = CreateTreeLogger(treeId);

    auto treeConfig = context->TreeConfig;

    auto now = TInstant::Now();
    for (auto& [operationId, operation] : context->Operations) {
        const auto& segment = operation.Segment;
        if (!segment || !IsDataCenterAwareSchedulingSegment(*segment)) {
            // Segment may be unset due to a race, and in this case we silently ignore the operation.
            continue;
        }

        auto& dataCenter = operation.DataCenter;
        if (!dataCenter) {
            continue;
        }

        if (operation.ResourceUsage != operation.ResourceDemand) {
            if (!operation.FailingToScheduleAtDataCenterSince) {
                operation.FailingToScheduleAtDataCenterSince = now;
            }

            if (*operation.FailingToScheduleAtDataCenterSince + treeConfig->SchedulingSegments->DataCenterReconsiderationTimeout < now) {
                YT_LOG_DEBUG(
                    "Operation has failed to schedule all jobs for too long, revoking its data center assignment "
                    "(OperationId: %v, SchedulingSegment: %v, PreviousDataCenter: %v, ResourceUsage: %v, ResourceDemand: %v, Timeout: %v)",
                    operationId,
                    segment,
                    dataCenter,
                    operation.ResourceUsage,
                    operation.ResourceDemand,
                    treeConfig->SchedulingSegments->DataCenterReconsiderationTimeout);

                // NB: We will abort all jobs that are running in the wrong data center.
                dataCenter.reset();
                operation.FailingToScheduleAtDataCenterSince.reset();
            }
        } else {
            operation.FailingToScheduleAtDataCenterSince.reset();
        }
    }
}

void TStrategySchedulingSegmentManager::CollectFairSharePerSegmentInTree(TManageTreeSchedulingSegmentsContext* context)
{
    auto& state = context->SchedulingSegmentsState;
    auto treeConfig = context->TreeConfig;
    auto keyResource = TNodeSchedulingSegmentManager::GetSegmentBalancingKeyResource(state.Mode);

    for (auto& [operationId, operation] : context->Operations) {
        const auto& segment = operation.Segment;
        if (!segment) {
            // Segment may be unset due to a race, and in this case we silently ignore the operation.
            continue;
        }

        auto& fairShareAtSegment = state.FairSharePerSegment.At(*segment);
        if (IsDataCenterAwareSchedulingSegment(*segment)) {
            auto& dataCenter = operation.DataCenter;
            if (!dataCenter) {
                continue;
            }

            fairShareAtSegment.MutableAt(dataCenter) += operation.FairShare[keyResource];
        } else {
            fairShareAtSegment.Mutable() += operation.FairShare[keyResource];
        }
    }
}

void TStrategySchedulingSegmentManager::AssignOperationsToDataCentersInTree(
    TManageTreeSchedulingSegmentsContext* context,
    const TString& treeId,
    THashMap<TDataCenter, double> totalCapacityPerDataCenter,
    THashMap<TDataCenter, double> remainingCapacityPerDataCenter)
{
    auto Logger = CreateTreeLogger(treeId);

    auto& state = context->SchedulingSegmentsState;
    auto treeConfig = context->TreeConfig;
    auto keyResource = TNodeSchedulingSegmentManager::GetSegmentBalancingKeyResource(state.Mode);

    std::vector<std::pair<TOperationId, TOperationSchedulingSegmentContext*>> operationsToAssignToDataCenter;
    operationsToAssignToDataCenter.reserve(context->Operations.size());
    for (auto& [operationId, operation] : context->Operations) {
        const auto& segment = operation.Segment;
        if (!segment || !IsDataCenterAwareSchedulingSegment(*segment)) {
            continue;
        }

        // NB(eshcherbin): Demand could be zero, because needed resources update is asynchronous.
        if (operation.ResourceDemand == TJobResources{}) {
            continue;
        }

        bool demandFullySatisfied = operation.DemandShare == operation.FairShare;
        if (operation.DataCenter || !demandFullySatisfied) {
            continue;
        }

        operationsToAssignToDataCenter.push_back({operationId, &operation});
    }

    std::sort(
        operationsToAssignToDataCenter.begin(),
        operationsToAssignToDataCenter.end(),
        [keyResource] (const auto& lhs, const auto& rhs) {
            const auto& lhsOperation = *lhs.second;
            const auto& rhsOperation = *rhs.second;
            auto lhsSpecifiedDataCenterCount = lhsOperation.SpecifiedDataCenters
                ? lhsOperation.SpecifiedDataCenters->size()
                : 0;
            auto rhsSpecifiedDataCenterCount = rhsOperation.SpecifiedDataCenters
                ? rhsOperation.SpecifiedDataCenters->size()
                : 0;
            if (lhsSpecifiedDataCenterCount != rhsSpecifiedDataCenterCount) {
                return lhsSpecifiedDataCenterCount < rhsSpecifiedDataCenterCount;
            }

            return GetResource(rhsOperation.ResourceDemand, keyResource) < GetResource(lhsOperation.ResourceDemand, keyResource);
        });

    for (const auto& [operationId, operation] : operationsToAssignToDataCenter) {
        const auto& segment = operation->Segment;

        TDataCenter bestDataCenter;
        auto bestDataCenterRemainingCapacity = std::numeric_limits<double>::lowest();
        for (const auto& [dataCenter, remainingCapacity] : remainingCapacityPerDataCenter) {
            YT_VERIFY(dataCenter);

            if (const auto& specifiedDataCenters = operation->SpecifiedDataCenters) {
                if (specifiedDataCenters->find(*dataCenter) == specifiedDataCenters->end()) {
                    continue;
                }
            }

            if (bestDataCenterRemainingCapacity < remainingCapacity) {
                bestDataCenter = dataCenter;
                bestDataCenterRemainingCapacity = remainingCapacity;
            }
        }

        if (!bestDataCenter) {
            YT_LOG_INFO(
                "No available data center matches operation's specified scheduling segment data centers "
                "(AvailableDataCenters: %v, SpecifiedDataCenters: %v, OperationId: %v)",
                state.DataCenters,
                operation->SpecifiedDataCenters,
                operationId);
            continue;
        }

        auto operationFairShare = operation->FairShare[keyResource];
        state.FairSharePerSegment.At(*segment).MutableAt(operation->DataCenter) -= operationFairShare;
        operation->DataCenter = bestDataCenter;
        state.FairSharePerSegment.At(*segment).MutableAt(operation->DataCenter) += operationFairShare;

        auto operationDemand = GetResource(operation->ResourceDemand, keyResource);
        state.FairResourceAmountPerSegment.At(*segment).MutableAt(operation->DataCenter) += operationDemand;
        remainingCapacityPerDataCenter[operation->DataCenter] -= operationDemand;

        YT_LOG_DEBUG(
            "Assigned operation to a new scheduling segment data center "
            "(SchedulingSegment: %v, DataCenter: %v, SpecifiedDataCenters: %v, "
            "OperationDemand: %v, RemainingCapacityPerDataCenter: %v, TotalCapacityPerDataCenter: %v, "
            "OperationId: %v)",
            segment,
            operation->DataCenter,
            operation->SpecifiedDataCenters,
            operationDemand,
            remainingCapacityPerDataCenter,
            totalCapacityPerDataCenter,
            operationId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
