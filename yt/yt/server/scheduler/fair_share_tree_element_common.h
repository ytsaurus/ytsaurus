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

DEFINE_ENUM(EPrescheduleJobOperationCriterion,
    (All)
    (AggressivelyStarvingOnly)
    (StarvingOnly)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
