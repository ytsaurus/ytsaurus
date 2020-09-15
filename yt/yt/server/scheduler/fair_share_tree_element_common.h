#pragma once

#include "private.h"

#include <yt/ytlib/controller_agent/public.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobsProfilingCounters
{
    TScheduleJobsProfilingCounters(const TString& prefix, const NProfiling::TTagIdList& treeIdProfilingTags);

    NProfiling::TShardedAggregateGauge PrescheduleJobTime;
    NProfiling::TShardedAggregateGauge TotalControllerScheduleJobTime;
    NProfiling::TShardedAggregateGauge ExecControllerScheduleJobTime;
    NProfiling::TShardedAggregateGauge StrategyScheduleJobTime;
    NProfiling::TShardedAggregateGauge PackingRecordHeartbeatTime;
    NProfiling::TShardedAggregateGauge PackingCheckTime;
    NProfiling::TShardedMonotonicCounter ScheduleJobAttemptCount;
    NProfiling::TShardedMonotonicCounter ScheduleJobFailureCount;
    TEnumIndexedVector<NControllerAgent::EScheduleJobFailReason, NProfiling::TShardedMonotonicCounter> ControllerScheduleJobFail;
};

////////////////////////////////////////////////////////////////////////////////

constexpr int UnassignedTreeIndex = -1;
constexpr int EmptySchedulingTagFilterIndex = -1;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrescheduleJobOperationCriterion,
    (All)
    (AggressivelyStarvingOnly)
    (StarvingOnly)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
