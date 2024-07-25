#include "aggregated_job_statistics.h"

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

i64 TAggregatedJobStatistics::CalculateCustomStatisticsCount() const
{
    i64 count = 0;
    for (const auto& [key, value] : GetData()) {
        if (key.StartsWith("/custom/")) {
            ++count;
        }
    }
    return count;
}

i64 TAggregatedJobStatistics::GetSumByJobStateAndType(
    const TString& statisticPath,
    EJobState jobState,
    const TString& jobType) const
{
    auto* taggedStatistics = FindTaggedSummaries(statisticPath);
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
    auto* taggedStatistics = FindTaggedSummaries(statisticPath);
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
                THROW_ERROR_EXCEPTION("Unreachable code. Summaries cannot be empty");
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
        GetData(),
        consumer,
        summariesSerializer);
}

TAggregatedJobStatistics MergeJobStatistics(const TAggregatedJobStatistics& lhs, const TAggregatedJobStatistics& rhs)
{
    TAggregatedJobStatistics mergedJobStatistics;
    for (const auto& [path, taggedSummaries] : lhs.GetData()) {
        mergedJobStatistics.AppendTaggedSummary(path, taggedSummaries);
    }
    for (const auto& [path, taggedSummaries] : rhs.GetData()) {
        mergedJobStatistics.AppendTaggedSummary(path, taggedSummaries);
    }
    return mergedJobStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
