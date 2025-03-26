#pragma once

#include "public.h"
#include "persistent_scheduler_state.h"
#include "scheduler_strategy.h"
#include "fair_share_tree_allocation_scheduler_structs.h"

#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <yt/yt/core/logging/fluent_log.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TNodeMovePenalty
{
    double PriorityPenalty = 0.0;
    double RegularPenalty = 0.0;
};

bool operator <(const TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs);
TNodeMovePenalty& operator +=(TNodeMovePenalty& lhs, const TNodeMovePenalty& rhs);

void FormatValue(TStringBuilderBase* builder, const TNodeMovePenalty& penalty, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct TNodeWithMovePenalty
{
    TFairShareTreeAllocationSchedulerNodeState* Node = nullptr;
    TNodeMovePenalty MovePenalty;
};

using TNodeWithMovePenaltyList = std::vector<TNodeWithMovePenalty>;

////////////////////////////////////////////////////////////////////////////////

struct TSetNodeSchedulingSegmentOptions
{
    NNodeTrackerClient::TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
    std::string NodeAddress;
    ESchedulingSegment OldSegment = ESchedulingSegment::Default;
    ESchedulingSegment NewSegment = ESchedulingSegment::Default;
};

using TSetNodeSchedulingSegmentOptionsList = std::vector<TSetNodeSchedulingSegmentOptions>;

////////////////////////////////////////////////////////////////////////////////

struct TUpdateSchedulingSegmentsContext
{
    const TInstant Now;
    const TFairShareTreeSnapshotPtr TreeSnapshot;

    // These are copies, it's safe to modify them.
    TFairShareTreeAllocationSchedulerOperationStateMap OperationStates;
    TFairShareTreeAllocationSchedulerNodeStateMap NodeStates;

    double NodesTotalKeyResourceLimit = 0.0;
    THashMap<TSchedulingSegmentModule, double> TotalCapacityPerModule;
    THashMap<TSchedulingSegmentModule, double> RemainingCapacityPerModule;
    TSegmentToResourceAmount CurrentResourceAmountPerSegment;
    TSegmentToResourceAmount FairResourceAmountPerSegment;
    TSegmentToFairShare FairSharePerSegment;

    TSetNodeSchedulingSegmentOptionsList MovedNodes;
    TError Error;
    TPersistentSchedulingSegmentsStatePtr PersistentState;
    NYson::TYsonString SerializedSchedulingSegmentsInfo;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulingSegmentManager
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TInstant, InitializationDeadline);

    static const TSchedulingSegmentModule& GetNodeModule(
        const std::optional<std::string>& nodeDataCenter,
        const std::optional<std::string>& nodeInfinibandCluster,
        ESchedulingSegmentModuleType moduleType);
    static const TSchedulingSegmentModule& GetNodeModule(
        const TExecNodeDescriptorPtr& nodeDescriptor,
        ESchedulingSegmentModuleType moduleType);

    static TString GetNodeTagFromModuleName(
        const std::string& moduleName,
        ESchedulingSegmentModuleType moduleType);

    TSchedulingSegmentManager(
        TString treeId,
        TFairShareStrategySchedulingSegmentsConfigPtr config,
        NLogging::TLogger logger,
        const NProfiling::TProfiler& profiler);

    void UpdateSchedulingSegments(TUpdateSchedulingSegmentsContext* context);

    void InitOrUpdateOperationSchedulingSegment(
        TOperationId operationId,
        const TFairShareTreeAllocationSchedulerOperationStatePtr& operationState) const;

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

    void ResetOperationModule(const TSchedulerOperationElement* operationElement, TUpdateSchedulingSegmentsContext* context) const;
    void PreemptNonPriorityOperationsFromModuleForOperation(
        TOperationId priorityOperationId,
        const TNonOwningOperationElementList& operations,
        TUpdateSchedulingSegmentsContext* context) const;

    bool IsOperationEligibleForPriorityModuleAssignment(const TSchedulerOperationElement* operationElement, TUpdateSchedulingSegmentsContext* context) const;

    double GetElementFairResourceAmount(const TSchedulerOperationElement* element, TUpdateSchedulingSegmentsContext* context) const;

    struct TOperationsToPreempt
    {
        double TotalPenalty = 0.0;
        TNonOwningOperationElementList Operations;
        TSchedulingSegmentModule Module;
    };

    THashMap<TSchedulingSegmentModule, TNonOwningOperationElementList> CollectNonPriorityAssignedOperationsPerModule(
        TUpdateSchedulingSegmentsContext* context) const;
    std::optional<TOperationsToPreempt> FindBestOperationsToPreempt(
        TOperationId operationId,
        TUpdateSchedulingSegmentsContext* context) const;
    std::optional<TOperationsToPreempt> FindBestOperationsToPreemptInModuleGreedy(
        const TSchedulingSegmentModule& module,
        double neededDemand,
        TNonOwningOperationElementList assignedOperationElements,
        TUpdateSchedulingSegmentsContext* context) const;

    void CollectCurrentResourceAmountPerSegment(TUpdateSchedulingSegmentsContext* context) const;
    void ResetOperationModuleAssignments(TUpdateSchedulingSegmentsContext* context) const;
    void CollectFairResourceAmountPerSegment(TUpdateSchedulingSegmentsContext* context) const;
    void AssignOperationsToModules(TUpdateSchedulingSegmentsContext* context) const;

    void ValidateInfinibandClusterTags(TUpdateSchedulingSegmentsContext* context) const;
    void ApplySpecifiedSegments(TUpdateSchedulingSegmentsContext* context) const;
    void CheckAndRebalanceSegments(TUpdateSchedulingSegmentsContext* context);

    std::pair<TSchedulingSegmentMap<bool>, bool> FindUnsatisfiedSegments(const TUpdateSchedulingSegmentsContext* context) const;
    void DoRebalanceSegments(TUpdateSchedulingSegmentsContext* context) const;
    void GetMovableNodes(
        TUpdateSchedulingSegmentsContext* context,
        THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* movableNodesPerModule,
        THashMap<TSchedulingSegmentModule, TNodeWithMovePenaltyList>* aggressivelyMovableNodesPerModule) const;

    const TSchedulingSegmentModule& GetNodeModule(const TFairShareTreeAllocationSchedulerNodeState& node) const;
    void SetNodeSegment(TFairShareTreeAllocationSchedulerNodeState* node, ESchedulingSegment segment, TUpdateSchedulingSegmentsContext* context) const;

    void LogAndProfileSegments(const TUpdateSchedulingSegmentsContext* context) const;
    NLogging::TOneShotFluentLogEvent LogStructuredGpuEventFluently(EGpuSchedulingLogEventType eventType) const;

    NYson::TYsonString GetSerializedSchedulingSegmentsInfo(const TUpdateSchedulingSegmentsContext* context) const;
    void BuildGpuOperationInfo(
        TOperationId operationId,
        const TFairShareTreeAllocationSchedulerOperationStatePtr& operationState,
        NYTree::TFluentMap fluent) const;
    void BuildGpuNodeInfo(
        const TFairShareTreeAllocationSchedulerNodeState& nodeState,
        NYTree::TFluentMap fluent) const;

    void BuildPersistentState(TUpdateSchedulingSegmentsContext* context) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
