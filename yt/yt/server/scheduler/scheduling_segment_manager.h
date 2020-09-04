#pragma once

#include "public.h"
#include "node_shard.h"

#include <yt/server/lib/scheduler/structs.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TNodeShardIdToMovedNodes = std::array<TNodeIdWithSchedulingSegmentList, MaxNodeShardCount>;

struct TManageSchedulingSegmentsContext
{
    INodeShardHost* NodeShardHost;
    TTreeIdToSchedulingSegmentsInfo SegmentsInfoPerTree;
    TRefCountedExecNodeDescriptorMapPtr ExecNodeDescriptors;
    THashMap<TString, std::vector<NNodeTrackerClient::TNodeId>> NodeIdsPerTree;

    TNodeShardIdToMovedNodes MovedNodesPerNodeShard;
};

////////////////////////////////////////////////////////////////////////////////

using TChangeNodeSegmentPenaltyFunction = std::function<double(const TExecNodeDescriptor&)>;

class TSchedulingSegmentManager
{
public:
    static ESchedulingSegment GetSegmentForOperation(
        ESegmentedSchedulingMode mode,
        const TJobResources& operationMinNeededResources);

    static EJobResourceType GetSegmentBalancingKeyResource(ESegmentedSchedulingMode mode);

    void ManageSegments(TManageSchedulingSegmentsContext* context);

private:
    THashMap<TString, std::optional<TInstant>> TreeSchedulingSegmentsUnsatisfiedSince_;

    void LogAndProfileSegmentsInTree(
        TManageSchedulingSegmentsContext* context,
        const TString& treeId,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment) const;

    void RebalanceSegmentsInTree(
        TManageSchedulingSegmentsContext* context,
        const TString& treeId,
        TSegmentToResourceAmount currentResourceAmountPerSegment);

    TChangeNodeSegmentPenaltyFunction CreatePenaltyFunction(
        TManageSchedulingSegmentsContext* context,
        const TString& treeId) const;

    std::vector<TExecNodeDescriptor> GetMovableNodesInTree(
        TManageSchedulingSegmentsContext *context,
        const TString& treeId,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment);

    static NLogging::TLogger CreateTreeLogger(const TString& treeId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
