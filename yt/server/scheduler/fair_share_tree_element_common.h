#pragma once

#include "private.h"

#include <yt/ytlib/controller_agent/public.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobsProfilingCounters
{
    TScheduleJobsProfilingCounters(const TString& prefix, const NProfiling::TTagIdList& treeIdProfilingTags);

    NProfiling::TAggregateGauge PrescheduleJobTime;
    NProfiling::TAggregateGauge TotalControllerScheduleJobTime;
    NProfiling::TAggregateGauge ExecControllerScheduleJobTime;
    NProfiling::TAggregateGauge StrategyScheduleJobTime;
    NProfiling::TAggregateGauge PackingRecordHeartbeatTime;
    NProfiling::TAggregateGauge PackingCheckTime;
    NProfiling::TMonotonicCounter ScheduleJobAttemptCount;
    NProfiling::TMonotonicCounter ScheduleJobFailureCount;
    TEnumIndexedVector<NControllerAgent::EScheduleJobFailReason, NProfiling::TMonotonicCounter> ControllerScheduleJobFail;
};

////////////////////////////////////////////////////////////////////////////////

constexpr int UnassignedTreeIndex = -1;
constexpr int EmptySchedulingTagFilterIndex = -1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
