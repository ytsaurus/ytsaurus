#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRunningAllocationStatistics
{
    //! In CPU*seconds.
    double TotalCpuTime = 0.0;
    double PreemptibleCpuTime = 0.0;

    //! In GPU*seconds.
    double TotalGpuTime = 0.0;
    double PreemptibleGpuTime = 0.0;
};

void FormatValue(TStringBuilderBase* builder, const TRunningAllocationStatistics& statistics, TStringBuf /*spec*/);
TString FormatRunningAllocationStatisticsCompact(const TRunningAllocationStatistics& statistics);
void Serialize(const TRunningAllocationStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TFairShareTreeAllocationSchedulerAllocationState
    : public NYTree::TYsonStructLite
{
    TOperationId OperationId;
    TJobResources ResourceLimits;

    REGISTER_YSON_STRUCT_LITE(TFairShareTreeAllocationSchedulerAllocationState);

    static void Register(TRegistrar);
};

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Make this refcounted?
struct TFairShareTreeAllocationSchedulerNodeState
{
    // NB: Descriptor may be missing if the node has only just registered and we haven't processed any heartbeats from it.
    TExecNodeDescriptorPtr Descriptor;

    ESchedulingSegment SchedulingSegment = ESchedulingSegment::Default;
    std::optional<ESchedulingSegment> SpecifiedSchedulingSegment;

    TRunningAllocationStatistics RunningAllocationStatistics;
    std::optional<NProfiling::TCpuInstant> LastRunningAllocationStatisticsUpdateTime;
    bool ForceRunningAllocationStatisticsUpdate = false;

    THashMap<TAllocationId, TFairShareTreeAllocationSchedulerAllocationState> RunningAllocations;
};

using TFairShareTreeAllocationSchedulerNodeStateMap = THashMap<NNodeTrackerClient::TNodeId, TFairShareTreeAllocationSchedulerNodeState>;

////////////////////////////////////////////////////////////////////////////////

struct TFairShareTreeAllocationSchedulerOperationState final
{
    const TStrategyOperationSpecPtr Spec;
    const bool IsGang;

    // Initialized after operation's materialization, but should not be modified after that.
    std::optional<TJobResources> AggregatedInitialMinNeededResources;

    std::optional<ESchedulingSegment> SchedulingSegment;
    TSchedulingSegmentModule SchedulingSegmentModule;
    std::optional<THashSet<TString>> SpecifiedSchedulingSegmentModules;
    std::optional<TInstant> FailingToScheduleAtModuleSince;
    std::optional<TInstant> FailingToAssignToModuleSince;

    TFairShareTreeAllocationSchedulerOperationState(
        TStrategyOperationSpecPtr spec,
        bool isGang);
};

using TFairShareTreeAllocationSchedulerOperationStatePtr = TIntrusivePtr<TFairShareTreeAllocationSchedulerOperationState>;
using TFairShareTreeAllocationSchedulerOperationStateMap = THashMap<TOperationId, TFairShareTreeAllocationSchedulerOperationStatePtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
