#pragma once

#include "public.h"
#include "node_shard.h"
#include "persistent_scheduler_state.h"
#include "scheduler_strategy.h"
#include "scheduler_tree_structs.h"

#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>

namespace NYT::NScheduler {

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

struct TManageNodeSchedulingSegmentsContext
{
    TInstant Now;
    TTreeSchedulingSegmentsState TreeSegmentsState;
    TRefCountedExecNodeDescriptorMapPtr ExecNodeDescriptors;
    // NB(eshcherbin): |TreeNodeIds| may only contain nodes which are present in |ExecNodeDescriptors|.
    std::vector<NNodeTrackerClient::TNodeId> TreeNodeIds;

    TError Error;
    TSetNodeSchedulingSegmentOptionsList MovedNodes;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeSchedulingSegmentManager
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TInstant, NodeSegmentsInitializationDeadline);

    static EJobResourceType GetSegmentBalancingKeyResource(ESegmentedSchedulingMode mode);

    static const TSchedulingSegmentModule& GetNodeModule(
        const std::optional<TString>& nodeDataCenter,
        const std::optional<TString>& nodeInfinibandCluster,
        ESchedulingSegmentModuleType moduleType);
    static const TSchedulingSegmentModule& GetNodeModule(
        const TExecNodeDescriptor& nodeDescriptor,
        ESchedulingSegmentModuleType moduleType);

    static TString GetNodeTagFromModuleName(const TString& moduleName, ESchedulingSegmentModuleType moduleType);

    static TNodeMovePenalty GetMovePenaltyForNode(
        const TExecNodeDescriptor& node,
        const TTreeSchedulingSegmentsState& strategyTreeState);

    TNodeSchedulingSegmentManager(TString treeId, NLogging::TLogger logger, const NProfiling::TProfiler& profiler);

    void ManageNodeSegments(TManageNodeSchedulingSegmentsContext* context);

private:
    const TString TreeId_;
    const NLogging::TLogger Logger;

    std::optional<TInstant> UnsatisfiedSince_;
    ESegmentedSchedulingMode PreviousMode_ = ESegmentedSchedulingMode::Disabled;

    NProfiling::TBufferedProducerPtr BufferedProducer_;

    void Reset(TManageNodeSchedulingSegmentsContext* context);

    void ValidateInfinibandClusterTags(TManageNodeSchedulingSegmentsContext* context) const;

    void LogAndProfileSegments(
        TManageNodeSchedulingSegmentsContext* context,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment,
        const THashMap<TSchedulingSegmentModule, double> totalResourceAmountPerModule) const;

    void RebalanceSegments(
        TManageNodeSchedulingSegmentsContext* context,
        TSegmentToResourceAmount currentResourceAmountPerSegment);

    void GetMovableNodes(
        TManageNodeSchedulingSegmentsContext* context,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment,
        THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* movableNodesPerModule,
        THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* aggressivelyMovableNodesPerModule);

    std::pair<TSchedulingSegmentMap<bool>, bool> FindUnsatisfiedSegments(
        TManageNodeSchedulingSegmentsContext* context,
        const TSegmentToResourceAmount& currentResourceAmountPerSegment) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSchedulingSegmentContext
{
    TJobResources ResourceDemand;
    TJobResources ResourceUsage;
    TResourceVector DemandShare;
    TResourceVector FairShare;
    std::optional<ESchedulingSegment> Segment;

    TSchedulingSegmentModule Module;
    std::optional<THashSet<TString>> SpecifiedModules;
    std::optional<TInstant> FailingToScheduleAtModuleSince;
};

struct TManageTreeSchedulingSegmentsContext
{
    TFairShareStrategyTreeConfigPtr TreeConfig;
    TJobResources TotalResourceLimits;
    THashMap<TSchedulingSegmentModule, TJobResources> ResourceLimitsPerModule;
    THashMap<TOperationId, TOperationSchedulingSegmentContext> Operations;

    TTreeSchedulingSegmentsState SchedulingSegmentsState;
};

////////////////////////////////////////////////////////////////////////////////

class TStrategySchedulingSegmentManager
{
public:
    static ESchedulingSegment GetSegmentForOperation(
        const TFairShareStrategySchedulingSegmentsConfigPtr& config,
        const TJobResources& operationMinNeededResources,
        bool isGang);

    static void ManageSegmentsInTree(
        TManageTreeSchedulingSegmentsContext* context,
        const TString& treeId);

private:
    static void ResetOperationModuleAssignmentsInTree(
        TManageTreeSchedulingSegmentsContext* context,
        const TString& treeId);

    static void CollectFairSharePerSegmentInTree(TManageTreeSchedulingSegmentsContext* context);

    static void AssignOperationsToModulesInTree(
        TManageTreeSchedulingSegmentsContext* context,
        const TString& treeId,
        THashMap<TSchedulingSegmentModule, double> totalCapacityPerModule,
        THashMap<TSchedulingSegmentModule, double> remainingCapacityPerModule);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
