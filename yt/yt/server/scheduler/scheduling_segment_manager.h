#pragma once

#include "public.h"
#include "persistent_scheduler_state.h"
#include "scheduler_strategy.h"
#include "fair_share_tree_job_scheduler_structs.h"

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
    TFairShareTreeJobSchedulerNodeState* Node = nullptr;
    TNodeMovePenalty MovePenalty;
};

using TNodeWithMovePenaltyList = std::vector<TNodeWithMovePenalty>;

////////////////////////////////////////////////////////////////////////////////

struct TSetNodeSchedulingSegmentOptions
{
    NNodeTrackerClient::TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
    ESchedulingSegment Segment = ESchedulingSegment::Default;
};

using TSetNodeSchedulingSegmentOptionsList = std::vector<TSetNodeSchedulingSegmentOptions>;

////////////////////////////////////////////////////////////////////////////////

struct TUpdateSchedulingSegmentsContext
{
    const TInstant Now;
    const TFairShareTreeSnapshotPtr TreeSnapshot;

    // These are copies, it's safe to modify them.
    TFairShareTreeJobSchedulerOperationStateMap OperationStates;
    TFairShareTreeJobSchedulerNodeStateMap NodeStates;

    double NodesTotalKeyResourceLimit = 0.0;
    THashMap<TSchedulingSegmentModule, double> TotalCapacityPerModule;
    THashMap<TSchedulingSegmentModule, double> RemainingCapacityPerModule;
    TSegmentToResourceAmount CurrentResourceAmountPerSegment;
    TSegmentToResourceAmount FairResourceAmountPerSegment;
    TSegmentToFairShare FairSharePerSegment;

    TSetNodeSchedulingSegmentOptionsList MovedNodes;
    TError Error;
    TPersistentSchedulingSegmentsStatePtr PersistentState;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulingSegmentManager
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TInstant, InitializationDeadline);

    static const TSchedulingSegmentModule& GetNodeModule(
        const std::optional<TString>& nodeDataCenter,
        const std::optional<TString>& nodeInfinibandCluster,
        ESchedulingSegmentModuleType moduleType);
    static const TSchedulingSegmentModule& GetNodeModule(
        const TExecNodeDescriptor& nodeDescriptor,
        ESchedulingSegmentModuleType moduleType);

    static TString GetNodeTagFromModuleName(const TString& moduleName, ESchedulingSegmentModuleType moduleType);

    TSchedulingSegmentManager(
        TString treeId,
        TFairShareStrategySchedulingSegmentsConfigPtr config,
        NLogging::TLogger logger,
        const NProfiling::TProfiler& profiler);

    void UpdateSchedulingSegments(TUpdateSchedulingSegmentsContext* context);

    void InitOrUpdateOperationSchedulingSegment(const TFairShareTreeJobSchedulerOperationStatePtr& operationState) const;

    void UpdateConfig(TFairShareStrategySchedulingSegmentsConfigPtr config);

private:
    const TString TreeId_;
    const NLogging::TLogger Logger;

    TFairShareStrategySchedulingSegmentsConfigPtr Config_;

    std::optional<TInstant> UnsatisfiedSince_;
    ESegmentedSchedulingMode PreviousMode_ = ESegmentedSchedulingMode::Disabled;

    NProfiling::TBufferedProducerPtr SensorProducer_;

    //! Thread affinity: Control.
    void DoUpdateSchedulingSegments(TUpdateSchedulingSegmentsContext* context);
    void Reset(TUpdateSchedulingSegmentsContext* context);

    void CollectCurrentResourceAmountPerSegment(TUpdateSchedulingSegmentsContext* context) const;
    void ResetOperationModuleAssignments(TUpdateSchedulingSegmentsContext* context) const;
    void CollectFairResourceAmountPerSegment(TUpdateSchedulingSegmentsContext* context) const;
    void AssignOperationsToModules(TUpdateSchedulingSegmentsContext* context) const;
    void AdjustFairResourceAmountBySatisfactionMargins(TUpdateSchedulingSegmentsContext* context) const;

    void ValidateInfinibandClusterTags(TUpdateSchedulingSegmentsContext* context) const;
    void ApplySpecifiedSegments(TUpdateSchedulingSegmentsContext* context) const;
    void CheckAndRebalanceSegments(TUpdateSchedulingSegmentsContext* context);

    std::pair<TSchedulingSegmentMap<bool>, bool> FindUnsatisfiedSegments(const TUpdateSchedulingSegmentsContext* context) const;
    void DoRebalanceSegments(TUpdateSchedulingSegmentsContext* context) const;
    void GetMovableNodes(
        TUpdateSchedulingSegmentsContext* context,
        THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* movableNodesPerModule,
        THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* aggressivelyMovableNodesPerModule) const;

    const TSchedulingSegmentModule& GetNodeModule(const TFairShareTreeJobSchedulerNodeState& node) const;
    void SetNodeSegment(TFairShareTreeJobSchedulerNodeState* node, ESchedulingSegment segment, TUpdateSchedulingSegmentsContext* context) const;

    void LogAndProfileSegments(const TUpdateSchedulingSegmentsContext* context) const;
    void BuildPersistentState(TUpdateSchedulingSegmentsContext* context) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
