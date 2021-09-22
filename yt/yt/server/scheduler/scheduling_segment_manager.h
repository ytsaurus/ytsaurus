#pragma once

#include "public.h"
#include "node_shard.h"
#include "persistent_scheduler_state.h"
#include "scheduler_strategy.h"

#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TNodeShardIdToMovedNodes = std::array<TSetNodeSchedulingSegmentOptionsList, MaxNodeShardCount>;

struct TManageNodeSchedulingSegmentsContext
{
    TInstant Now;
    INodeShardHost* NodeShardHost;
    TStrategySchedulingSegmentsState StrategySegmentsState;
    TRefCountedExecNodeDescriptorMapPtr ExecNodeDescriptors;
    THashMap<TString, std::vector<NNodeTrackerClient::TNodeId>> NodeIdsPerTree;

    TNodeShardIdToMovedNodes MovedNodesPerNodeShard;
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeMovePenalty
{
    double PriorityPenalty = 0.0;
    double RegularPenalty = 0.0;
};

bool operator <(const TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs);
TNodeMovePenalty& operator +=(TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs);

void FormatValue(TStringBuilderBase* builder, const TNodeMovePenalty& penalty, TStringBuf /*format*/);
TString ToString(const TNodeMovePenalty& penalty);

////////////////////////////////////////////////////////////////////////////////

struct TNodeWithMovePenalty
{
    const TExecNodeDescriptor* Descriptor = nullptr;
    TNodeMovePenalty MovePenalty;
};

using TNodeWithMovePenaltyList = std::vector<TNodeWithMovePenalty>;

////////////////////////////////////////////////////////////////////////////////

class TNodeSchedulingSegmentManager
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TInstant, NodeSegmentsInitializationDeadline);

    static EJobResourceType GetSegmentBalancingKeyResource(ESegmentedSchedulingMode mode);

    TNodeSchedulingSegmentManager();

    void ManageNodeSegments(TManageNodeSchedulingSegmentsContext* context);

    TPersistentNodeSchedulingSegmentStateMap BuildPersistentNodeSegmentsState(TManageNodeSchedulingSegmentsContext* context) const;

    void SetProfilingEnabled(bool enabled);

private:
    struct TPersistentTreeAttributes
    {
        std::optional<TInstant> UnsatisfiedSince;
        ESegmentedSchedulingMode PreviousMode = ESegmentedSchedulingMode::Disabled;
    };
    THashMap<TString, TPersistentTreeAttributes> TreeIdToPersistentAttributes_;

    NProfiling::TBufferedProducerPtr BufferedProducer_;

    void ResetTree(TManageNodeSchedulingSegmentsContext *context, const TString& treeId);

    void LogAndProfileSegmentsInTree(
        TManageNodeSchedulingSegmentsContext* context,
        const TString& treeId,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment,
        const THashMap<TDataCenter, double> totalResourceAmountPerDataCenter,
        NProfiling::ISensorWriter* sensorWriter) const;

    void RebalanceSegmentsInTree(
        TManageNodeSchedulingSegmentsContext* context,
        const TString& treeId,
        TSegmentToResourceAmount currentResourceAmountPerSegment);

    TNodeMovePenalty GetMovePenaltyForNode(
        const TExecNodeDescriptor& nodeDescriptor,
        TManageNodeSchedulingSegmentsContext* context,
        const TString& treeId) const;

    void GetMovableNodesInTree(
        TManageNodeSchedulingSegmentsContext *context,
        const TString& treeId,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment,
        THashMap<TDataCenter, TNodeWithMovePenaltyList>* movableNodesPerDataCenter,
        THashMap<TDataCenter, TNodeWithMovePenaltyList>* aggressivelyMovableNodesPerDataCenter);

    std::pair<TSchedulingSegmentMap<bool>, bool> FindUnsatisfiedSegmentsInTree(
        TManageNodeSchedulingSegmentsContext *context,
        const TString& treeId,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSchedulingSegmentContext
{
    const TJobResources& ResourceDemand;
    const TJobResources& ResourceUsage;
    const TResourceVector& DemandShare;
    const TResourceVector& FairShare;
    const std::optional<THashSet<TString>>& SpecifiedDataCenters;
    const std::optional<ESchedulingSegment>& Segment;

    TDataCenter DataCenter;
    std::optional<TInstant> FailingToScheduleAtDataCenterSince;
};

struct TManageTreeSchedulingSegmentsContext
{
    const TFairShareStrategyTreeConfigPtr& TreeConfig;
    const TJobResources& TotalResourceLimits;
    THashMap<TDataCenter, TJobResources> ResourceLimitsPerDataCenter;
    THashMap<TOperationId, TOperationSchedulingSegmentContext> Operations;

    TTreeSchedulingSegmentsState SchedulingSegmentsState;
};

////////////////////////////////////////////////////////////////////////////////

class TStrategySchedulingSegmentManager
{
public:
    static ESchedulingSegment GetSegmentForOperation(
        ESegmentedSchedulingMode mode,
        const TJobResources& operationMinNeededResources);

    static void ManageSegmentsInTree(
        TManageTreeSchedulingSegmentsContext* context,
        const TString& treeId);

private:
    static void ResetOperationDataCenterAssignmentsInTree(
        TManageTreeSchedulingSegmentsContext* context,
        const TString& treeId);

    static void CollectFairSharePerSegmentInTree(TManageTreeSchedulingSegmentsContext* context);

    static void AssignOperationsToDataCentersInTree(
        TManageTreeSchedulingSegmentsContext* context,
        const TString& treeId,
        THashMap<TDataCenter, double> totalCapacityPerDataCenter,
        THashMap<TDataCenter, double> remainingCapacityPerDataCenter);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
