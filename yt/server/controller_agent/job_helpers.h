#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/server/scheduler/job.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TBriefJobStatisticsPtr BuildBriefStatistics(std::unique_ptr<TJobSummary> jobSummary);

// Returns true if job proxy wasn't stalling and false otherwise.
// This function is related to the suspicious jobs detection.
bool CheckJobActivity(
    const TBriefJobStatisticsPtr& lhs,
    const TBriefJobStatisticsPtr& rhs,
    i64 cpuUsageThreshold,
    double inputPipeIdleTimeFraction);

// Performs statistics parsing and put it inside jobSummary.
void ParseStatistics(TJobSummary* jobSummary, const NYson::TYsonString& lastObservedStatisticsYson = NYson::TYsonString());

NYson::TYsonString BuildInputPaths(
    const std::vector<NYPath::TRichYPath>& inputPaths,
    const TChunkStripeListPtr& inputStripeList,
    EOperationType operationType,
    EJobType jobType);

////////////////////////////////////////////////////////////////////

struct TScheduleJobStatistics
    : public TIntrinsicRefCounted
    , public IPersistent
{
    void RecordJobResult(const TScheduleJobResultPtr& scheduleJobResult);

    TEnumIndexedVector<int, EScheduleJobFailReason> Failed;
    TDuration Duration;
    i64 Count = 0;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobStatistics)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
