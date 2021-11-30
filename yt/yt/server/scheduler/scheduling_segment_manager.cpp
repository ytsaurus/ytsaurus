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

namespace {

double GetNodeResourceLimit(const TExecNodeDescriptor& node, EJobResourceType resourceType)
{
    return node.Online
        ? GetResource(node.ResourceLimits, resourceType)
        : 0.0;
}

void SortByPenalty(TNodeWithMovePenaltyList& nodeWithPenaltyList)
{
    SortBy(nodeWithPenaltyList, [] (const TNodeWithMovePenalty& node) { return node.MovePenalty; });
}

TLogger CreateTreeLogger(const TString& treeId)
{
    return Logger.WithTag("TreeId: %v", treeId);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool operator <(const TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs)
{
    if (lhs.PriorityPenalty != rhs.PriorityPenalty) {
        return lhs.PriorityPenalty < rhs.PriorityPenalty;
    }
    return lhs.RegularPenalty < rhs.RegularPenalty;
}

TNodeMovePenalty& operator +=(TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs)
{
    lhs.PriorityPenalty += rhs.PriorityPenalty;
    lhs.RegularPenalty += rhs.RegularPenalty;
    return lhs;
}

void FormatValue(TStringBuilderBase* builder, const TNodeMovePenalty& penalty, TStringBuf /*format*/)
{
    builder->AppendFormat("{PriorityPenalty: %.3f, RegularPenalty: %.3f}",
        penalty.PriorityPenalty,
        penalty.RegularPenalty);
}

TString ToString(const TNodeMovePenalty& penalty)
{
    return ToStringViaBuilder(penalty);
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

const TSchedulingSegmentModule& TNodeSchedulingSegmentManager::GetNodeModule(
    const std::optional<TString>& nodeDataCenter,
    const std::optional<TString>& nodeInfinibandCluster,
    ESchedulingSegmentModuleType moduleType)
{
    switch (moduleType) {
        case ESchedulingSegmentModuleType::DataCenter:
            return nodeDataCenter;
        case ESchedulingSegmentModuleType::InfinibandCluster:
            return nodeInfinibandCluster;
        default:
            YT_ABORT();
    }
}

const TSchedulingSegmentModule& TNodeSchedulingSegmentManager::GetNodeModule(
    const TExecNodeDescriptor& nodeDescriptor,
    ESchedulingSegmentModuleType moduleType)
{
    return GetNodeModule(nodeDescriptor.DataCenter, nodeDescriptor.InfinibandCluster, moduleType);
}

TString TNodeSchedulingSegmentManager::GetNodeTagFromModuleName(const TString& moduleName, ESchedulingSegmentModuleType moduleType)
{
    switch (moduleType) {
        case ESchedulingSegmentModuleType::DataCenter:
            return moduleName;
        case ESchedulingSegmentModuleType::InfinibandCluster:
            return Format("%v:%v", InfinibandClusterNameKey, moduleName);
        default:
            YT_ABORT();
    }
}

void TNodeSchedulingSegmentManager::ValidateNodeTags(const TBooleanFormulaTags& tags)
{
    static const TString InfinibandClusterTagPrefix = InfinibandClusterNameKey + ":";

    std::vector<TString> infinibandClusterTags;
    for (const auto& tag : tags.GetSourceTags()) {
        if (tag.StartsWith(InfinibandClusterTagPrefix)) {
            infinibandClusterTags.push_back(tag);
        }
    }

    if (std::ssize(infinibandClusterTags) > 1) {
        THROW_ERROR_EXCEPTION("Node has more than one infiniband cluster tags")
            << TErrorAttribute("infiniband_cluster_tags", infinibandClusterTags);
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

            LogAndProfileSegmentsInTree(
                context,
                treeId,
                /*currentResourceAmountPerSegment*/ {},
                /*totalResourceAmountPerModule*/ {},
                &sensorBuffer);

            continue;
        }

        auto keyResource = GetSegmentBalancingKeyResource(strategyTreeState.Mode);
        YT_VERIFY(strategyTreeState.KeyResource == keyResource);

        TSegmentToResourceAmount currentResourceAmountPerSegment;
        THashMap<TSchedulingSegmentModule, double> totalResourceAmountPerModule;
        for (auto nodeId : context->NodeIdsPerTree[treeId]) {
            const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
            const auto& nodeModule = GetNodeModule(node, strategyTreeState.ModuleType);
            auto resourceAmountOnNode = GetNodeResourceLimit(node, keyResource);
            auto& currentResourceAmount = IsModuleAwareSchedulingSegment(node.SchedulingSegment)
                ? currentResourceAmountPerSegment.At(node.SchedulingSegment).MutableAt(nodeModule)
                : currentResourceAmountPerSegment.At(node.SchedulingSegment).Mutable();
            currentResourceAmount += resourceAmountOnNode;
            totalResourceAmountPerModule[nodeModule] += resourceAmountOnNode;
        }

        LogAndProfileSegmentsInTree(
            context,
            treeId,
            currentResourceAmountPerSegment,
            totalResourceAmountPerModule,
            &sensorBuffer);

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
    const THashMap<TSchedulingSegmentModule, double> totalResourceAmountPerModule,
    ISensorWriter* sensorWriter) const
{
    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];
    bool segmentedSchedulingEnabled = strategyTreeState.Mode != ESegmentedSchedulingMode::Disabled;

    if (segmentedSchedulingEnabled) {
        YT_LOG_DEBUG(
            "Scheduling segments state in tree "
            "(TreeId: %v, Mode: %v, Modules: %v, KeyResource: %v, FairSharePerSegment: %v, TotalResourceAmount: %v, "
            "TotalResourceAmountPerModule: %v, FairResourceAmountPerSegment: %v, CurrentResourceAmountPerSegment: %v)",
            treeId,
            strategyTreeState.Mode,
            strategyTreeState.Modules,
            GetSegmentBalancingKeyResource(strategyTreeState.Mode),
            strategyTreeState.FairSharePerSegment,
            strategyTreeState.TotalKeyResourceAmount,
            totalResourceAmountPerModule,
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
                if (IsModuleAwareSchedulingSegment(segment)) {
                    for (const auto& schedulingSegmentModule : strategyTreeState.Modules) {
                        TWithTagGuard guard(sensorWriter, TTag{"module", ToString(schedulingSegmentModule)});
                        sensorWriter->AddGauge(sensorName, valueAtSegment.GetOrDefaultAt(schedulingSegmentModule));
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
            if (IsModuleAwareSchedulingSegment(segment)) {
                guard.AddTag(TTag{"module", ToString(NullModule)});
            }

            sensorWriter->AddGauge("/fair_resource_amount", 0.0);
            sensorWriter->AddGauge("/current_resource_amount", 0.0);
        }
    }

    for (const auto& schedulingSegmentModule : strategyTreeState.Modules) {
        auto it = totalResourceAmountPerModule.find(schedulingSegmentModule);
        auto moduleCapacity = it != totalResourceAmountPerModule.end() ? it->second : 0.0;

        TWithTagGuard guard(sensorWriter, TTag{"module", ToString(schedulingSegmentModule)});
        sensorWriter->AddGauge("/module_capacity", moduleCapacity);
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

    TSchedulingSegmentMap<int> addedNodeCountPerSegment;
    TSchedulingSegmentMap<int> removedNodeCountPerSegment;
    TNodeMovePenalty totalPenalty;
    std::vector<std::pair<TNodeWithMovePenalty, ESchedulingSegment>> movedNodes;

    auto trySatisfySegment = [&] (
        ESchedulingSegment segment,
        double& currentResourceAmount,
        double fairResourceAmount,
        TNodeWithMovePenaltyList& availableNodes)
    {
        while (currentResourceAmount < fairResourceAmount) {
            if (availableNodes.empty()) {
                break;
            }

            // NB(eshcherbin): |availableNodes| is sorted in order of decreasing penalty.
            auto [nextAvailableNode, nextAvailableNodeMovePenalty] = availableNodes.back();
            availableNodes.pop_back();

            auto resourceAmountOnNode = GetNodeResourceLimit(*nextAvailableNode, keyResource);
            auto oldSegment = nextAvailableNode->SchedulingSegment;

            auto nodeShardId = context->NodeShardHost->GetNodeShardId(nextAvailableNode->Id);
            context->MovedNodesPerNodeShard[nodeShardId].push_back(TSetNodeSchedulingSegmentOptions{
                .NodeId = nextAvailableNode->Id,
                .Segment = segment});
            movedNodes.emplace_back(TNodeWithMovePenalty{nextAvailableNode, nextAvailableNodeMovePenalty}, segment);
            totalPenalty += nextAvailableNodeMovePenalty;

            const auto& schedulingSegmentModule = GetNodeModule(*nextAvailableNode, strategyTreeState.ModuleType);
            currentResourceAmount += resourceAmountOnNode;
            if (IsModuleAwareSchedulingSegment(segment)) {
                ++addedNodeCountPerSegment.At(segment).MutableAt(schedulingSegmentModule);
            } else {
                ++addedNodeCountPerSegment.At(segment).Mutable();
            }

            if (IsModuleAwareSchedulingSegment(oldSegment)) {
                currentResourceAmountPerSegment.At(oldSegment).MutableAt(schedulingSegmentModule) -= resourceAmountOnNode;
                ++removedNodeCountPerSegment.At(oldSegment).MutableAt(schedulingSegmentModule);
            } else {
                currentResourceAmountPerSegment.At(oldSegment).Mutable() -= resourceAmountOnNode;
                ++removedNodeCountPerSegment.At(oldSegment).Mutable();
            }
        }
    };

    // Every node has a penalty for moving it to another segment. We collect a set of movable nodes
    // iteratively by taking the node with the lowest penalty until the remaining nodes can no longer
    // satisfy the fair resource amount determined by the strategy.
    // In addition, the rest of the nodes are called aggressively movable if the current segment is not module-aware.
    // The intuition is that we should be able to compensate for a loss of such a node from one module by moving
    // a node from another module to the segment.
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList> movableNodesPerModule;
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList> aggressivelyMovableNodesPerModule;
    GetMovableNodesInTree(
        context,
        treeId,
        currentResourceAmountPerSegment,
        &movableNodesPerModule,
        &aggressivelyMovableNodesPerModule);

    // First, we try to satisfy all module-aware segments, one module at a time.
    // During this phase we are allowed to use the nodes from |aggressivelyMovableNodesPerModule|
    // if |movableNodesPerModule| is exhausted.
    for (const auto& schedulingSegmentModule : strategyTreeState.Modules) {
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            if (!IsModuleAwareSchedulingSegment(segment)) {
                continue;
            }

            auto& currentResourceAmount = currentResourceAmountPerSegment.At(segment).MutableAt(schedulingSegmentModule);
            auto fairResourceAmount = strategyTreeState.FairResourceAmountPerSegment.At(segment).GetOrDefaultAt(schedulingSegmentModule);
            trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, movableNodesPerModule[schedulingSegmentModule]);
            trySatisfySegment(segment, currentResourceAmount, fairResourceAmount, aggressivelyMovableNodesPerModule[schedulingSegmentModule]);
        }
    }

    TNodeWithMovePenaltyList movableNodes;
    for (const auto& [_, movableNodesAtModule] : movableNodesPerModule) {
        std::move(movableNodesAtModule.begin(), movableNodesAtModule.end(), std::back_inserter(movableNodes));
    }
    SortByPenalty(movableNodes);

    // Second, we try to satisfy all other segments using the remaining movable nodes.
    // Note that some segments might have become unsatisfied during the first phase
    // if we used any nodes from |aggressivelyMovableNodesPerModule|.
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsModuleAwareSchedulingSegment(segment)) {
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
        movedNodes.size(),
        addedNodeCountPerSegment,
        removedNodeCountPerSegment,
        currentResourceAmountPerSegment,
        totalPenalty);

    for (const auto& [nodeWithPenalty, newSegment] : movedNodes) {
        YT_LOG_DEBUG("Moving node to a new scheduling segment (Address: %v, Segment: %v, Penalty: %v)",
            nodeWithPenalty.Descriptor->Address,
            nodeWithPenalty.MovePenalty,
            newSegment);
    }
}

void TNodeSchedulingSegmentManager::GetMovableNodesInTree(
    TManageNodeSchedulingSegmentsContext* context,
    const TString& treeId,
    const TSegmentToResourceAmount& currentResourceAmountPerSegment,
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* movableNodesPerModule,
    THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* aggressivelyMovableNodesPerModule)
{
    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(strategyTreeState.Mode);

    TSchedulingSegmentMap<std::vector<TNodeId>> nodeIdsPerSegment;
    for (auto nodeId : context->NodeIdsPerTree[treeId]) {
        const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
        auto& nodeIds = nodeIdsPerSegment.At(node.SchedulingSegment);
        if (IsModuleAwareSchedulingSegment(node.SchedulingSegment)) {
            nodeIds.MutableAt(GetNodeModule(node, strategyTreeState.ModuleType)).push_back(nodeId);
        } else {
            nodeIds.Mutable().push_back(nodeId);
        }
    }

    auto collectMovableNodes = [&] (double currentResourceAmount, double fairResourceAmount, const std::vector<TNodeId>& nodeIds) {
        TNodeWithMovePenaltyList segmentNodes;
        segmentNodes.reserve(nodeIds.size());
        for (auto nodeId : nodeIds) {
            const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
            segmentNodes.push_back(TNodeWithMovePenalty{
                .Descriptor = &node,
                .MovePenalty = GetMovePenaltyForNode(node, context, treeId)});
        }

        SortByPenalty(segmentNodes);

        for (const auto& nodeWithMovePenalty : segmentNodes) {
            auto* node = nodeWithMovePenalty.Descriptor;
            if (node->SchedulingSegmentFrozen) {
                continue;
            }

            const auto& schedulingSegmentModule = GetNodeModule(*node, strategyTreeState.ModuleType);
            auto resourceAmountOnNode = GetNodeResourceLimit(*node, keyResource);
            currentResourceAmount -= resourceAmountOnNode;
            if (currentResourceAmount >= fairResourceAmount) {
                (*movableNodesPerModule)[schedulingSegmentModule].push_back(nodeWithMovePenalty);
            } else if (!IsModuleAwareSchedulingSegment(node->SchedulingSegment)) {
                (*aggressivelyMovableNodesPerModule)[schedulingSegmentModule].push_back(nodeWithMovePenalty);
            }
        }
    };

    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        const auto& currentResourceAmount = currentResourceAmountPerSegment.At(segment);
        const auto& fairResourceAmount = strategyTreeState.FairResourceAmountPerSegment.At(segment);
        const auto& nodeIds = nodeIdsPerSegment.At(segment);
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : strategyTreeState.Modules) {
                collectMovableNodes(
                    currentResourceAmount.GetOrDefaultAt(schedulingSegmentModule),
                    fairResourceAmount.GetOrDefaultAt(schedulingSegmentModule),
                    nodeIds.GetOrDefaultAt(schedulingSegmentModule));
            }
        } else {
            collectMovableNodes(
                currentResourceAmount.GetOrDefault(),
                fairResourceAmount.GetOrDefault(),
                nodeIds.GetOrDefault());
        }
    }

    auto sortAndReverseMovableNodes = [&] (auto& movableNodes) {
        for (const auto& schedulingSegmentModule : strategyTreeState.Modules) {
            SortByPenalty(movableNodes[schedulingSegmentModule]);
            std::reverse(movableNodes[schedulingSegmentModule].begin(), movableNodes[schedulingSegmentModule].end());
        }
    };
    sortAndReverseMovableNodes(*movableNodesPerModule);
    sortAndReverseMovableNodes(*aggressivelyMovableNodesPerModule);
}

TNodeMovePenalty TNodeSchedulingSegmentManager::GetMovePenaltyForNode(
    const TExecNodeDescriptor& node,
    TManageNodeSchedulingSegmentsContext* context,
    const TString& treeId) const
{
    const auto& strategyTreeState = context->StrategySegmentsState.TreeStates[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(strategyTreeState.Mode);
    switch (keyResource) {
        case EJobResourceType::Gpu:
            return TNodeMovePenalty{
                .PriorityPenalty = node.RunningJobStatistics.TotalGpuTime - node.RunningJobStatistics.PreemptableGpuTime,
                .RegularPenalty = node.RunningJobStatistics.TotalGpuTime};
        default:
            return TNodeMovePenalty{
                .PriorityPenalty = node.RunningJobStatistics.TotalCpuTime - node.RunningJobStatistics.PreemptableCpuTime,
                .RegularPenalty = node.RunningJobStatistics.TotalCpuTime};
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
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : strategyTreeState.Modules) {
                if (currentResourceAmount.GetOrDefaultAt(schedulingSegmentModule) < fairResourceAmount.GetOrDefaultAt(schedulingSegmentModule)) {
                    hasUnsatisfiedSegment = true;
                    isSegmentUnsatisfied.At(segment).SetAt(schedulingSegmentModule, true);
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
    state.ModuleType = treeConfig->SchedulingSegments->ModuleType;
    state.UnsatisfiedSegmentsRebalancingTimeout = treeConfig->SchedulingSegments->UnsatisfiedSegmentsRebalancingTimeout;

    if (state.Mode == ESegmentedSchedulingMode::Disabled) {
        return;
    }

    auto keyResource = TNodeSchedulingSegmentManager::GetSegmentBalancingKeyResource(state.Mode);
    state.KeyResource = keyResource;
    state.TotalKeyResourceAmount = GetResource(context->TotalResourceLimits, keyResource);
    for (const auto& schedulingSegmentModule : treeConfig->SchedulingSegments->GetModules()) {
        state.Modules.push_back(schedulingSegmentModule);
    }

    double expectedTotalKeyResourceAmount = 0.0;
    for (const auto& [_, resourceLimitsAtModule] : context->ResourceLimitsPerModule) {
        expectedTotalKeyResourceAmount += GetResource(resourceLimitsAtModule, keyResource);
    }
    YT_LOG_WARNING_IF(expectedTotalKeyResourceAmount != state.TotalKeyResourceAmount,
        "Total key resource amount differs from the sum of provided per-module limits, "
        "operation scheduling segments distribution might be unfair "
        "(TotalKeyResourceAmount: %v, ExpectedTotalKeyResourceAmount: %v, KeyResource: %v)",
        state.TotalKeyResourceAmount,
        expectedTotalKeyResourceAmount,
        keyResource);

    ResetOperationModuleAssignmentsInTree(context, treeId);

    CollectFairSharePerSegmentInTree(context);

    THashMap<TSchedulingSegmentModule, double> totalCapacityPerModule;
    THashMap<TSchedulingSegmentModule, double> remainingCapacityPerModule;
    for (const auto& schedulingSegmentModule : state.Modules) {
        YT_VERIFY(schedulingSegmentModule);

        auto capacity = GetResource(context->ResourceLimitsPerModule[*schedulingSegmentModule], keyResource);
        totalCapacityPerModule.emplace(*schedulingSegmentModule, capacity);
        remainingCapacityPerModule.emplace(*schedulingSegmentModule, capacity);
    }

    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : state.Modules) {
                auto fairResourceAmount = state.FairSharePerSegment.At(segment).GetOrDefaultAt(schedulingSegmentModule) * state.TotalKeyResourceAmount;
                state.FairResourceAmountPerSegment.At(segment).SetAt(schedulingSegmentModule, fairResourceAmount);
                remainingCapacityPerModule[schedulingSegmentModule] -= fairResourceAmount;
            }
        } else {
            auto fairResourceAmount = state.FairSharePerSegment.At(segment).GetOrDefault() * state.TotalKeyResourceAmount;
            state.FairResourceAmountPerSegment.At(segment).Set(fairResourceAmount);
        }
    }

    AssignOperationsToModulesInTree(context, treeId, totalCapacityPerModule, remainingCapacityPerModule);

    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        if (IsModuleAwareSchedulingSegment(segment)) {
            for (const auto& schedulingSegmentModule : state.Modules) {
                auto satisfactionMargin = treeConfig->SchedulingSegments->SatisfactionMargins.At(segment).GetOrDefaultAt(schedulingSegmentModule);
                auto& value = state.FairResourceAmountPerSegment.At(segment).MutableAt(schedulingSegmentModule);
                value = std::max(value + satisfactionMargin, 0.0);
            }
        } else {
            auto satisfactionMargin = treeConfig->SchedulingSegments->SatisfactionMargins.At(segment).GetOrDefault();
            auto& value = state.FairResourceAmountPerSegment.At(segment).Mutable();
            value = std::max(value + satisfactionMargin, 0.0);
        }
    }
}

void TStrategySchedulingSegmentManager::ResetOperationModuleAssignmentsInTree(TManageTreeSchedulingSegmentsContext* context, const TString& treeId)
{
    auto Logger = CreateTreeLogger(treeId);

    auto treeConfig = context->TreeConfig;

    auto now = TInstant::Now();
    for (auto& [operationId, operation] : context->Operations) {
        const auto& segment = operation.Segment;
        if (!segment || !IsModuleAwareSchedulingSegment(*segment)) {
            // Segment may be unset due to a race, and in this case we silently ignore the operation.
            continue;
        }

        auto& schedulingSegmentModule = operation.Module;
        if (!schedulingSegmentModule) {
            continue;
        }

        if (operation.ResourceUsage != operation.ResourceDemand) {
            if (!operation.FailingToScheduleAtModuleSince) {
                operation.FailingToScheduleAtModuleSince = now;
            }

            if (*operation.FailingToScheduleAtModuleSince + treeConfig->SchedulingSegments->ModuleReconsiderationTimeout < now) {
                YT_LOG_DEBUG(
                    "Operation has failed to schedule all jobs for too long, revoking its module assignment "
                    "(OperationId: %v, SchedulingSegment: %v, PreviousModule: %v, ResourceUsage: %v, ResourceDemand: %v, Timeout: %v)",
                    operationId,
                    segment,
                    schedulingSegmentModule,
                    operation.ResourceUsage,
                    operation.ResourceDemand,
                    treeConfig->SchedulingSegments->ModuleReconsiderationTimeout);

                // NB: We will abort all jobs that are running in the wrong module.
                schedulingSegmentModule.reset();
                operation.FailingToScheduleAtModuleSince.reset();
            }
        } else {
            operation.FailingToScheduleAtModuleSince.reset();
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
        if (IsModuleAwareSchedulingSegment(*segment)) {
            auto& schedulingSegmentModule = operation.Module;
            if (!schedulingSegmentModule) {
                continue;
            }

            fairShareAtSegment.MutableAt(schedulingSegmentModule) += operation.FairShare[keyResource];
        } else {
            fairShareAtSegment.Mutable() += operation.FairShare[keyResource];
        }
    }
}

void TStrategySchedulingSegmentManager::AssignOperationsToModulesInTree(
    TManageTreeSchedulingSegmentsContext* context,
    const TString& treeId,
    THashMap<TSchedulingSegmentModule, double> totalCapacityPerModule,
    THashMap<TSchedulingSegmentModule, double> remainingCapacityPerModule)
{
    auto Logger = CreateTreeLogger(treeId);

    auto& state = context->SchedulingSegmentsState;
    auto treeConfig = context->TreeConfig;
    auto keyResource = TNodeSchedulingSegmentManager::GetSegmentBalancingKeyResource(state.Mode);

    // COMPAT(eshcherbin): Remove after all GPU trees with scheduling segments have fully migrated to new module type.
    // We don't reflect this change in runtime parameters immediately. Runtime parameters will be updated during
    // the next scheduling segments management iteration.
    for (auto& [operationId, operation] : context->Operations) {
        const auto& moduleMigrationMapping = treeConfig->SchedulingSegments->ModuleMigrationMapping;
        auto findNewModuleName = [&] (const TString& moduleName) -> std::optional<TString> {
            auto it = moduleMigrationMapping.find(moduleName);
            return it != moduleMigrationMapping.end() ? std::make_optional(it->second) : std::nullopt;
        };

        if (operation.Module) {
            if (auto newModuleName = findNewModuleName(*operation.Module)) {
                YT_LOG_INFO(
                    "Migrated operation to new scheduling segment module (OperationId: %v, OldModuleName: %v, NewModuleName: %v)",
                    operationId,
                    operation.Module,
                    newModuleName);

                operation.Module = std::move(newModuleName);
            }
        }

        // NB(eshcherbin): Specified modules should be quite rare.
        if (operation.SpecifiedModules) {
            THashSet<TString> newSpecifiedModules;
            bool hasChanged = false;
            for (const auto& specifiedModule : *operation.SpecifiedModules) {
                auto newModuleName = findNewModuleName(specifiedModule);
                hasChanged |= newModuleName.has_value();
                newSpecifiedModules.emplace(std::move(newModuleName).value_or(specifiedModule));
            }

            if (hasChanged) {
                YT_LOG_DEBUG(
                    "Migrated operation to new specified modules (OperationId: %v, OldSpecifiedModules: %v, NewSpecifiedModules: %v)",
                    operationId,
                    operation.SpecifiedModules,
                    newSpecifiedModules);

                operation.SpecifiedModules = std::move(newSpecifiedModules);
            }
        }
    }

    std::vector<std::pair<TOperationId, TOperationSchedulingSegmentContext*>> operationsToAssignToModule;
    operationsToAssignToModule.reserve(context->Operations.size());
    for (auto& [operationId, operation] : context->Operations) {
        const auto& segment = operation.Segment;
        if (!segment || !IsModuleAwareSchedulingSegment(*segment)) {
            continue;
        }

        // NB(eshcherbin): Demand could be zero, because needed resources update is asynchronous.
        if (operation.ResourceDemand == TJobResources{}) {
            continue;
        }

        bool demandFullySatisfied = operation.DemandShare == operation.FairShare;
        if (operation.Module || !demandFullySatisfied) {
            continue;
        }

        operationsToAssignToModule.push_back({operationId, &operation});
    }

    std::sort(
        operationsToAssignToModule.begin(),
        operationsToAssignToModule.end(),
        [keyResource] (const auto& lhs, const auto& rhs) {
            const auto& lhsOperation = *lhs.second;
            const auto& rhsOperation = *rhs.second;
            auto lhsSpecifiedModuleCount = lhsOperation.SpecifiedModules
                ? lhsOperation.SpecifiedModules->size()
                : 0;
            auto rhsSpecifiedModuleCount = rhsOperation.SpecifiedModules
                ? rhsOperation.SpecifiedModules->size()
                : 0;
            if (lhsSpecifiedModuleCount != rhsSpecifiedModuleCount) {
                return lhsSpecifiedModuleCount < rhsSpecifiedModuleCount;
            }

            return GetResource(rhsOperation.ResourceDemand, keyResource) < GetResource(lhsOperation.ResourceDemand, keyResource);
        });

    for (const auto& [operationId, operation] : operationsToAssignToModule) {
        const auto& segment = operation->Segment;
        auto operationDemand = GetResource(operation->ResourceDemand, keyResource);

        std::function<bool(double, double)> isModuleBetter;
        double initialBestRemainingCapacity;
        switch (treeConfig->SchedulingSegments->ModuleAssignmentHeuristic) {
            case ESchedulingSegmentModuleAssignmentHeuristic::MaxRemainingCapacity:
                isModuleBetter = [] (double remainingCapacity, double bestRemainingCapacity) {
                    return bestRemainingCapacity < remainingCapacity;
                };
                initialBestRemainingCapacity = std::numeric_limits<double>::lowest();
                break;

            case ESchedulingSegmentModuleAssignmentHeuristic::MinRemainingFeasibleCapacity:
                isModuleBetter = [operationDemand] (double remainingCapacity, double bestRemainingCapacity) {
                    return remainingCapacity >= operationDemand && bestRemainingCapacity > remainingCapacity;
                };
                initialBestRemainingCapacity = std::numeric_limits<double>::max();
                break;

            default:
                YT_ABORT();
        }

        TSchedulingSegmentModule bestModule;
        auto bestRemainingCapacity = initialBestRemainingCapacity;
        for (const auto& [schedulingSegmentModule, remainingCapacity] : remainingCapacityPerModule) {
            YT_VERIFY(schedulingSegmentModule);

            if (const auto& specifiedModules = operation->SpecifiedModules) {
                if (specifiedModules->find(*schedulingSegmentModule) == specifiedModules->end()) {
                    continue;
                }
            }

            if (isModuleBetter(remainingCapacity, bestRemainingCapacity)) {
                bestModule = schedulingSegmentModule;
                bestRemainingCapacity = remainingCapacity;
            }
        }

        if (!bestModule) {
            YT_LOG_INFO(
                "Failed to find a suitable module for operation "
                "(AvailableModules: %v, SpecifiedModules: %v, OperationDemand: %v, "
                "RemainingCapacityPerModule: %v, TotalCapacityPerModule: %v, OperationId: %v)",
                state.Modules,
                operation->SpecifiedModules,
                operationDemand,
                remainingCapacityPerModule,
                totalCapacityPerModule,
                operationId);

            continue;
        }

        auto operationFairShare = operation->FairShare[keyResource];
        state.FairSharePerSegment.At(*segment).MutableAt(operation->Module) -= operationFairShare;
        operation->Module = bestModule;
        state.FairSharePerSegment.At(*segment).MutableAt(operation->Module) += operationFairShare;

        state.FairResourceAmountPerSegment.At(*segment).MutableAt(operation->Module) += operationDemand;
        remainingCapacityPerModule[operation->Module] -= operationDemand;

        YT_LOG_DEBUG(
            "Assigned operation to a new scheduling segment module "
            "(SchedulingSegment: %v, Module: %v, SpecifiedModules: %v, "
            "OperationDemand: %v, RemainingCapacityPerModule: %v, TotalCapacityPerModule: %v, "
            "OperationId: %v)",
            segment,
            operation->Module,
            operation->SpecifiedModules,
            operationDemand,
            remainingCapacityPerModule,
            totalCapacityPerModule,
            operationId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
