#pragma once

#include "fair_share_tree_allocation_scheduler_structs.h"

#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/library/vector_hdrf/resource_vector.h>
#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/fair_share_update.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TSchedulingModule = TSchedulingSegmentModule;
using ESchedulingModuleType = ESchedulingSegmentModuleType;
using ESchedulingModuleAssignmentHeuristic = ESchedulingSegmentModuleAssignmentHeuristic;
using ESchedulingModulePreemptionHeuristic = ESchedulingSegmentModulePreemptionHeuristic;

////////////////////////////////////////////////////////////////////////////////

struct TGpuAllocationState final
{
    TOperationId OperationId;
    TJobResourcesWithQuota Resources;
    bool Scheduled = false;
    bool Preemptible = false;
};

DEFINE_REFCOUNTED_TYPE(TGpuAllocationState)

////////////////////////////////////////////////////////////////////////////////

struct TOperationRuntimeAttributes
{
    TJobResources Demand;
    TJobResources UsageAtUpdate;
    double FairResourceAmount = 0.0;
    THashSet<TGpuAllocationStatePtr> AllocationResources;
    bool EffectivePrioritySchedulingModuleAssignmentEnabled = false;
};

// TODO(eshcherbin): Should this be a class?
class TGpuSchedulerOperationState final
{
public:
    DEFINE_BYREF_RO_PROPERTY(TOperationId, OperationId);
    DEFINE_BYVAL_RO_PROPERTY(bool, IsGang);
    DEFINE_BYREF_RO_PROPERTY(std::optional<THashSet<TString>>, SpecifiedSchedulingModules);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TOperationRuntimeAttributes>, RuntimeAttributes);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TJobResources>, AggregatedInitialMinNeededResources);

    DEFINE_BYVAL_RW_PROPERTY(double, ResourceUsage, 0.0);
    DEFINE_BYVAL_RW_PROPERTY(double, TotalResourceUsage, 0.0);
    DEFINE_BYVAL_RW_PROPERTY(bool, OperationHasPriority, false);

    DEFINE_BYREF_RW_PROPERTY(TSchedulingModule, SchedulingModule);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TGpuAllocationStatePtr>, AllocationsToSchedule);

    using TScheduledAllocationsMap = THashMap<TGpuAllocationStatePtr, NNodeTrackerClient::TNodeId>;
    DEFINE_BYREF_RW_PROPERTY(TScheduledAllocationsMap, ScheduledAllocations);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TInstant>, FailingToRunAllocationsAtModuleSince);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TInstant>, FailingToScheduleAtModuleSince);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TInstant>, FailingToAssignToModuleSince);

public:
    TGpuSchedulerOperationState(
        TOperationId operationId,
        bool isGang,
        std::optional<THashSet<TString>> specifiedSchedulingModules = {},
        std::optional<TOperationRuntimeAttributes> runtimeAttributes = {});

    double GetNeededResources() const;

    void OnAllocationScheduled(const TGpuAllocationStatePtr& allocation, NNodeTrackerClient::TNodeId nodeId);
    void RemoveAllocation(const TGpuAllocationStatePtr& allocation);
    void ResetUsage();
};

using TGpuSchedulerOperationStateMap = THashMap<TOperationId, TGpuSchedulerOperationStatePtr>;

DEFINE_REFCOUNTED_TYPE(TGpuSchedulerOperationState)

////////////////////////////////////////////////////////////////////////////////

struct TGpuSchedulerNodeState final
{
    NNodeTrackerClient::TNodeId NodeId;

    // NB: Descriptor may be missing if the node has only just registered and we haven't processed any heartbeats from it.
    TExecNodeDescriptorPtr Descriptor;

    // Usage from the GPU scheduler's point of view.
    TJobResources ResourceUsage;

    THashMap<TGpuAllocationStatePtr, TDiskQuota> DiskRequests;

    TJobResources PreemptibleResourceUsage;
    THashSet<TGpuAllocationStatePtr> PreemptibleAllocations;

    THashSet<TGpuAllocationStatePtr> RunningAllocations;
};

using TGpuSchedulerNodeStateMap = THashMap<NNodeTrackerClient::TNodeId, TGpuSchedulerNodeStatePtr>;

DEFINE_REFCOUNTED_TYPE(TGpuSchedulerNodeState)

////////////////////////////////////////////////////////////////////////////////

using TOperationsPerNodeMap = THashMap<NNodeTrackerClient::TNodeId, THashMultiSet<TGpuSchedulerOperationStatePtr>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
