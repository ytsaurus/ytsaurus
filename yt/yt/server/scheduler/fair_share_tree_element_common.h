#pragma once

#include "private.h"

#include <yt/ytlib/controller_agent/public.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TDetailedFairShare
{
    double MinShareGuaranteeRatio = 0.0;
    double IntegralGuaranteeRatio = 0.0;
    double WeightProportionalRatio = 0.0;

    void Reset()
    {
        MinShareGuaranteeRatio = 0.0;
        IntegralGuaranteeRatio = 0.0;
        WeightProportionalRatio = 0.0;
    }

    double Total() const
    {
        return MinShareGuaranteeRatio + IntegralGuaranteeRatio + WeightProportionalRatio;
    }

    double Guaranteed() const
    {
        return MinShareGuaranteeRatio + IntegralGuaranteeRatio;
    }
};

TString ToString(const TDetailedFairShare& detailedFairShare);

void FormatValue(TStringBuilderBase* builder, const TDetailedFairShare& detailedFairShare, TStringBuf /* format */);

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer);

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
