#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

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

} // namespace NYT::NScheduler
