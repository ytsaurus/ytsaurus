#include "aggregated_job_statistics.h"

#include "job_info.h"
#include "task.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TJobStatisticsTags::Persist(const TStreamPersistenceContext& context)
{
    using ::NYT::Persist;

    Persist(context, JobState);
    Persist(context, JobType);
    Persist(context, PoolTree);
}

bool operator<(const TJobStatisticsTags& lhs, const TJobStatisticsTags& rhs)
{
    return std::tie(lhs.JobState, lhs.JobType, lhs.PoolTree) < std::tie(rhs.JobState, rhs.JobType, rhs.PoolTree);
}

bool operator==(const TJobStatisticsTags& lhs, const TJobStatisticsTags& rhs)
{
    return lhs.JobState == rhs.JobState && lhs.JobType == rhs.JobType && lhs.PoolTree == rhs.PoolTree;
}

void Serialize(const TJobStatisticsTags& tags, IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("job_state").Value(tags.JobState)
            .Item("job_type").Value(tags.JobType)
            .Item("pool_tree").Value(tags.PoolTree)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TAggregatedJobStatistics::UpdateJobStatistics(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    YT_VERIFY(jobSummary.Statistics);

    auto statistics = *jobSummary.Statistics;
    auto statisticsState = GetStatisticsJobState(joblet, jobSummary.State);
    auto tags = TJobStatisticsTags{
        .JobState = statisticsState,
        .JobType = joblet->Task->GetVertexDescriptorForJoblet(joblet),
        .PoolTree = joblet->TreeId,
    };
    TaggedJobStatistics_.AppendStatistics(statistics, tags);
}

EJobState TAggregatedJobStatistics::GetStatisticsJobState(const TJobletPtr& joblet, EJobState state)
{
    // NB: Completed restarted job is considered as lost in statistics.
    // Actually we have lost previous incarnation of this job, but it was already considered as completed in statistics.
    return joblet->Restarted && state == EJobState::Completed
        ? EJobState::Lost
        : state;
}

void TAggregatedJobStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, TaggedJobStatistics_);
}

i64 TAggregatedJobStatistics::GetSumByJobStateAndType(
    const TString& statisticPath,
    EJobState jobState,
    const TString& jobType) const
{
    auto* taggedStatistics = TaggedJobStatistics_.FindTaggedSummaries(statisticPath);
    if (!taggedStatistics) {
        return 0;
    }

    i64 result = 0;
    for (const auto& [tags, summary] : *taggedStatistics) {
        if (tags.JobState == jobState && tags.JobType == jobType) {
            result += summary.GetSum();
        }
    }

    return result;
}

std::optional<TSummary> TAggregatedJobStatistics::FindSummaryByJobStateAndType(
    const TString& statisticPath,
    EJobState jobState,
    const TString& jobType) const
{
    auto* taggedStatistics = TaggedJobStatistics_.FindTaggedSummaries(statisticPath);
    if (!taggedStatistics) {
        return std::nullopt;
    }

    bool hasMatched = false;
    TSummary result;
    for (const auto& [tags, summary] : *taggedStatistics) {
        if (tags.JobState == jobState && tags.JobType == jobType) {
            hasMatched = true;
            result.Merge(summary);
        }
    }

    return hasMatched ? std::make_optional(result) : std::nullopt;
}

void TAggregatedJobStatistics::SerializeLegacy(IYsonConsumer* consumer) const
{
    SerializeCustom(
        consumer,
        [] (const TTaggedSummaries& summaries, IYsonConsumer* consumer) {
            if (summaries.size() == 0) {
                THROW_ERROR_EXCEPTION("Unreachable code. Summaries cannot be empty.");
            }

            THashMap<EJobState, THashMap<TString, TSummary>> groupedByJobStateAndType;
            for (const auto& [tags, summary] : summaries) {
                groupedByJobStateAndType[tags.JobState][tags.JobType].Merge(summary);
            }

            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("$").Value(groupedByJobStateAndType)
                .EndMap();
        });
}

void TAggregatedJobStatistics::SerializeCustom(
    IYsonConsumer* consumer,
    const std::function<void(const TTaggedSummaries&, NYson::IYsonConsumer*)>& summariesSerializer) const
{
    SerializeYsonPathsMap<TTaggedSummaries>(
        TaggedJobStatistics_.GetData(),
        consumer,
        summariesSerializer);
}

void Serialize(const TAggregatedJobStatistics& statistics, IYsonConsumer* consumer)
{
    Serialize(statistics.TaggedJobStatistics_, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
