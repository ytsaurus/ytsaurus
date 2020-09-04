#include "scheduling_segment_manager.h"
#include "private.h"

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

static constexpr int LargeGpuSegmentJobGpuDemand = 8;

////////////////////////////////////////////////////////////////////////////////

ESchedulingSegment TSchedulingSegmentManager::GetSegmentForOperation(
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

EJobResourceType TSchedulingSegmentManager::GetSegmentBalancingKeyResource(ESegmentedSchedulingMode mode)
{
    switch (mode) {
        case ESegmentedSchedulingMode::LargeGpu:
            return EJobResourceType::Gpu;
        default:
            YT_ABORT();
    }
}

void TSchedulingSegmentManager::ManageSegments(TManageSchedulingSegmentsContext* context)
{
    for (const auto& [treeId, segmentsInfo] : context->SegmentsInfoPerTree) {
        if (segmentsInfo.Mode == ESegmentedSchedulingMode::Disabled) {
            TreeSchedulingSegmentsUnsatisfiedSince_.erase(treeId);

            YT_LOG_DEBUG("Segmented scheduling is disabled in tree, skipping (TreeId: %v)",
                treeId);

            continue;
        }

        auto keyResource = GetSegmentBalancingKeyResource(segmentsInfo.Mode);
        YT_VERIFY(segmentsInfo.KeyResource == keyResource);

        TSegmentToResourceAmount currentResourceAmountPerSegment;
        for (auto nodeId : context->NodeIdsPerTree[treeId]) {
            const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
            auto resourceAmountOnNode = GetResource(node.ResourceLimits, keyResource);
            currentResourceAmountPerSegment[node.SchedulingSegment] += resourceAmountOnNode;
        }

        LogAndProfileSegmentsInTree(context, treeId, currentResourceAmountPerSegment);

        SmallVector<ESchedulingSegment, TEnumTraits<ESchedulingSegment>::DomainSize> unsatisfiedSegments;
        for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
            if (currentResourceAmountPerSegment[segment] < segmentsInfo.FairResourceAmountPerSegment[segment]) {
                unsatisfiedSegments.push_back(segment);
            }
        }

        auto& unsatisfiedSince = TreeSchedulingSegmentsUnsatisfiedSince_[treeId];
        if (unsatisfiedSegments.empty()) {
            unsatisfiedSince = std::nullopt;
            continue;
        }

        auto now = TInstant::Now();
        if (!unsatisfiedSince) {
            unsatisfiedSince = now;
        }

        YT_LOG_DEBUG("Found unsatisfied scheduling segments in tree ("
            "TreeId: %v, UnsatisfiedSegments: %v, UnsatisfiedFor: %v, Timeout: %v)",
            treeId,
            unsatisfiedSegments,
            now - *unsatisfiedSince,
            segmentsInfo.UnsatisfiedSegmentsRebalancingTimeout);

        if (now > *unsatisfiedSince + segmentsInfo.UnsatisfiedSegmentsRebalancingTimeout) {
            RebalanceSegmentsInTree(
                context,
                treeId,
                std::move(currentResourceAmountPerSegment));
            unsatisfiedSince = std::nullopt;
        }
    }
}

void TSchedulingSegmentManager::LogAndProfileSegmentsInTree(
    TManageSchedulingSegmentsContext* context,
    const TString& treeId,
    const TSegmentToResourceAmount& currentResourceAmountPerSegment) const
{
    static const TEnumMemberTagCache<ESchedulingSegment> SchedulingSegmentTagCache("segment");

    const auto& segmentsInfo = context->SegmentsInfoPerTree[treeId];

    YT_LOG_DEBUG("Scheduling segments state in tree ("
        "TreeId: %v, Mode: %v, KeyResource: %v, FairResourceAmountPerSegment: %v, CurrentResourceAmountPerSegment: %v)",
        treeId,
        segmentsInfo.Mode,
        GetSegmentBalancingKeyResource(segmentsInfo.Mode),
        segmentsInfo.FairResourceAmountPerSegment,
        currentResourceAmountPerSegment);

    auto profiler = Profiler.AppendPath("/segments");
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        TTagIdList tags{SchedulingSegmentTagCache.GetTag(segment), segmentsInfo.TreeIdProfilingTag};
        profiler.Enqueue("/fair_resource_amount", std::round(segmentsInfo.FairResourceAmountPerSegment[segment]), EMetricType::Gauge, tags);
        profiler.Enqueue("/current_resource_amount", std::round(currentResourceAmountPerSegment[segment]), EMetricType::Gauge, tags);
    }
}

void TSchedulingSegmentManager::RebalanceSegmentsInTree(
    TManageSchedulingSegmentsContext *context,
    const TString &treeId,
    TSegmentToResourceAmount currentResourceAmountPerSegment)
{
    auto Logger = CreateTreeLogger(treeId);

    YT_LOG_DEBUG("Rebalancing scheduling segments in tree");

    const auto& segmentsInfo = context->SegmentsInfoPerTree[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(segmentsInfo.Mode);

    auto movableNodes = GetMovableNodesInTree(context, treeId, currentResourceAmountPerSegment);
    auto getPenalty = CreatePenaltyFunction(context, treeId);
    SortBy(movableNodes, getPenalty);

    TEnumIndexedVector<ESchedulingSegment, int> deltaNodeCountPerSegment;
    double totalPenalty = 0.0;
    int movedNodeCount = 0;
    auto nextMovableNodeIterator = movableNodes.begin();
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        auto& resourceAmount = currentResourceAmountPerSegment[segment];
        auto fairResourceAmount = segmentsInfo.FairResourceAmountPerSegment[segment];
        while (resourceAmount < fairResourceAmount) {
            if (nextMovableNodeIterator == movableNodes.end()) {
                break;
            }

            const auto& nextMovableNode = *nextMovableNodeIterator;
            ++nextMovableNodeIterator;

            auto resourceAmountOnNode = GetResource(nextMovableNode.ResourceLimits, keyResource);
            auto oldSegment = nextMovableNode.SchedulingSegment;

            auto nodeShardId = context->NodeShardHost->GetNodeShardId(nextMovableNode.Id);
            context->MovedNodesPerNodeShard[nodeShardId].emplace_back(nextMovableNode.Id, segment);
            ++movedNodeCount;
            totalPenalty += getPenalty(nextMovableNode);

            resourceAmount += resourceAmountOnNode;
            currentResourceAmountPerSegment[oldSegment] -= resourceAmountOnNode;
            ++deltaNodeCountPerSegment[segment];
            --deltaNodeCountPerSegment[oldSegment];
        }

        // If we failed to satisfy the segment we should stop and report that there is a misconfiguration or some other fault.
        if (resourceAmount < fairResourceAmount) {
            YT_LOG_WARNING("Failed to satisfy all scheduling segments during rebalancing, stopping");
            break;
        }
    }

    YT_LOG_DEBUG("Scheduling segments rebalancing finished ("
        "MovedNodeCount: %v, DeltaNodeCountPerSegment: %v, NewResourceAmountPerSegment: %v, TotalPenalty: %v)",
        movedNodeCount,
        deltaNodeCountPerSegment,
        currentResourceAmountPerSegment,
        totalPenalty);
}

std::vector<TExecNodeDescriptor> TSchedulingSegmentManager::GetMovableNodesInTree(
    TManageSchedulingSegmentsContext *context,
    const TString& treeId,
    const TSegmentToResourceAmount& currentResourceAmountPerSegment)
{
    const auto& segmentsInfo = context->SegmentsInfoPerTree[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(segmentsInfo.Mode);
    auto getPenalty = CreatePenaltyFunction(context, treeId);

    TEnumIndexedVector<ESchedulingSegment, std::vector<TNodeId>> nodeIdsPerSegment;
    for (auto nodeId : context->NodeIdsPerTree[treeId]) {
        const auto& node = GetOrCrash(*context->ExecNodeDescriptors, nodeId);
        nodeIdsPerSegment[node.SchedulingSegment].push_back(nodeId);
    }

    std::vector<TExecNodeDescriptor> movableNodes;
    for (auto segment : TEnumTraits<ESchedulingSegment>::GetDomainValues()) {
        auto resourceAmount = currentResourceAmountPerSegment[segment];
        auto fairResourceAmount = segmentsInfo.FairResourceAmountPerSegment[segment];

        if (resourceAmount > fairResourceAmount) {
            std::vector<TExecNodeDescriptor> segmentNodes;
            segmentNodes.reserve(nodeIdsPerSegment[segment].size());
            for (auto nodeId : nodeIdsPerSegment[segment]) {
                segmentNodes.push_back(GetOrCrash(*context->ExecNodeDescriptors, nodeId));
            }

            SortBy(segmentNodes, getPenalty);

            for (const auto& node : segmentNodes) {
                auto resourceAmountOnNode = GetResource(node.ResourceLimits, keyResource);

                if (resourceAmount - resourceAmountOnNode < fairResourceAmount) {
                    break;
                }

                resourceAmount -= resourceAmountOnNode;
                movableNodes.push_back(node);
            }
        }
    }

    return movableNodes;
}

TChangeNodeSegmentPenaltyFunction TSchedulingSegmentManager::CreatePenaltyFunction(
    TManageSchedulingSegmentsContext* context,
    const TString& treeId) const
{
    const auto& segmentsInfo = context->SegmentsInfoPerTree[treeId];
    auto keyResource = GetSegmentBalancingKeyResource(segmentsInfo.Mode);
    switch (keyResource) {
        case EJobResourceType::Gpu:
            return [] (const TExecNodeDescriptor& node) { return node.RunningJobStatistics.TotalGpuTime; };
        default:
            return [] (const TExecNodeDescriptor& node) { return node.RunningJobStatistics.TotalCpuTime; };
    }
}

TLogger TSchedulingSegmentManager::CreateTreeLogger(const TString &treeId)
{
    auto logger = Logger;
    logger.AddTag("TreeId: %v", treeId);
    return logger;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
