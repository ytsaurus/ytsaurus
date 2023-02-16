#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRunningJobStatistics
{
    //! In CPU*seconds.
    double TotalCpuTime = 0.0;
    double PreemptibleCpuTime = 0.0;

    //! In GPU*seconds.
    double TotalGpuTime = 0.0;
    double PreemptibleGpuTime = 0.0;
};

void FormatValue(TStringBuilderBase* builder, const TRunningJobStatistics& statistics, TStringBuf /*format*/);
TString ToString(const TRunningJobStatistics& statistics);
TString FormatRunningJobStatisticsCompact(const TRunningJobStatistics& statistics);
void Serialize(const TRunningJobStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Make this refcounted?
struct TFairShareTreeJobSchedulerNodeState
{
    std::optional<TExecNodeDescriptor> Descriptor;

    ESchedulingSegment SchedulingSegment = ESchedulingSegment::Default;
    std::optional<ESchedulingSegment> SpecifiedSchedulingSegment;

    TRunningJobStatistics RunningJobStatistics;
    std::optional<NProfiling::TCpuInstant> LastRunningJobStatisticsUpdateTime;
};

using TFairShareTreeJobSchedulerNodeStateMap = THashMap<NNodeTrackerClient::TNodeId, TFairShareTreeJobSchedulerNodeState>;

////////////////////////////////////////////////////////////////////////////////

struct TFairShareTreeJobSchedulerOperationState final
{
    const TStrategyOperationSpecPtr Spec;
    const bool IsGang;

    // Initialized after operation's materialization, but should not be modified after that.
    std::optional<TJobResources> InitialAggregatedMinNeededResources;

    std::optional<ESchedulingSegment> SchedulingSegment;
    TSchedulingSegmentModule SchedulingSegmentModule;
    std::optional<THashSet<TString>> SpecifiedSchedulingSegmentModules;
    std::optional<TInstant> FailingToScheduleAtModuleSince;

    TFairShareTreeJobSchedulerOperationState(
        TStrategyOperationSpecPtr spec,
        bool isGang);
};

using TFairShareTreeJobSchedulerOperationStatePtr = TIntrusivePtr<TFairShareTreeJobSchedulerOperationState>;
using TFairShareTreeJobSchedulerOperationStateMap = THashMap<TOperationId, TFairShareTreeJobSchedulerOperationStatePtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
