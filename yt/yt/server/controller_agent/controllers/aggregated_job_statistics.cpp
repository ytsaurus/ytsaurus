#include "aggregated_job_statistics.h"

#include "job_info.h"
#include "task.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TAggregatedJobStatistics::TAggregatedJobStatistics(bool addTaskToSuffix)
    : AddTaskToSuffix_(addTaskToSuffix)
{ }

void TAggregatedJobStatistics::UpdateJobStatistics(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    YT_VERIFY(jobSummary.Statistics);

    auto statistics = *jobSummary.Statistics;
    auto statisticsState = GetStatisticsJobState(joblet, jobSummary.State);
    auto statisticsSuffix = GetStatisticsSuffix(joblet->Task->GetVertexDescriptor(), FormatEnum(statisticsState));
    statistics.AddSuffixToNames(statisticsSuffix);
    JobStatistics_.Merge(statistics);
}

EJobState TAggregatedJobStatistics::GetStatisticsJobState(const TJobletPtr& joblet, EJobState state)
{
    // NB: Completed restarted job is considered as lost in statistics.
    // Actually we have lost previous incarnation of this job, but it was already considered as completed in statistics.
    return joblet->Restarted && state == EJobState::Completed
        ? EJobState::Lost
        : state;
}

TString TAggregatedJobStatistics::GetStatisticsSuffix(const TString& taskName, const TString& state)
{
    if (AddTaskToSuffix_) {
        return Format("/$/%lv/%lv", state, taskName);
    } else {
        return Format("/$/%lv", state);
    }
}

void TAggregatedJobStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, JobStatistics_);
}

std::optional<i64> TAggregatedJobStatistics::FindNumericValue(
    const TString& statisticPath,
    const TString& state,
    const TString& taskName)
{
    using NYT::FindNumericValue;

    return FindNumericValue(JobStatistics_, statisticPath + GetStatisticsSuffix(taskName, state));
}

std::optional<i64> TAggregatedJobStatistics::FindNumericValue(
    const TString& statisticPath,
    EJobState state,
    const TString& taskName)
{
    return FindNumericValue(statisticPath, FormatEnum(state), taskName);
}

std::optional<TSummary> TAggregatedJobStatistics::FindSummary(
    const TString& statisticPath,
    const TString& state,
    const TString& taskName)
{
    using NYT::FindSummary;

    return FindSummary(JobStatistics_, statisticPath + GetStatisticsSuffix(taskName, state));
}

std::optional<TSummary> TAggregatedJobStatistics::FindSummary(
    const TString& statisticPath,
    EJobState state,
    const TString& taskName)
{
    return FindSummary(statisticPath, FormatEnum(state), taskName);
}

void Serialize(const TAggregatedJobStatistics& statistics, IYsonConsumer* consumer)
{
    Serialize(statistics.JobStatistics_, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
