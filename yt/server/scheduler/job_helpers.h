#pragma once

#include "job.h"

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

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
