#pragma once

#include "private.h"

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

struct TJobStatisticsTags
{
    EJobState JobState;
    TString JobType;
    TString PoolTree;

    void Persist(const TStreamPersistenceContext& context);
};

void Serialize(const TJobStatisticsTags& statistics, NYson::IYsonConsumer* consumer);

bool operator<(const TJobStatisticsTags& lhs, const TJobStatisticsTags& rhs);
bool operator==(const TJobStatisticsTags& lhs, const TJobStatisticsTags& rhs);

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Convenient wrapper around TStatistics providing helpers for controller-specific aggregations.
 */
class TAggregatedJobStatistics
    : public TTaggedStatistics<TJobStatisticsTags>
{
public:
    using TTaggedSummaries = THashMap<TJobStatisticsTags, TSummary>;

    i64 CalculateCustomStatisticsCount() const;

    i64 GetSumByJobStateAndType(
        const TString& statisticPath,
        EJobState jobState,
        const TString& jobType) const;

    std::optional<TSummary> FindSummaryByJobStateAndType(
        const TString& statisticPath,
        EJobState state,
        const TString& taskName) const;

    void SerializeCustom(
        NYson::IYsonConsumer* consumer,
        const std::function<void(const TTaggedSummaries&, NYson::IYsonConsumer*)>& summariesSerializer) const;
    void SerializeLegacy(NYson::IYsonConsumer* consumer) const;

private:
    TTaggedStatistics<TJobStatisticsTags> TaggedJobStatistics_;

    friend TAggregatedJobStatistics MergeJobStatistics(const TAggregatedJobStatistics& lhs, const TAggregatedJobStatistics& rhs);
};

TAggregatedJobStatistics MergeJobStatistics(const TAggregatedJobStatistics& lhs, const TAggregatedJobStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

template <>
struct THash<NYT::NControllerAgent::NControllers::TJobStatisticsTags>
{
    size_t operator()(const NYT::NControllerAgent::NControllers::TJobStatisticsTags& tags) const
    {
        size_t res = 0;
        NYT::HashCombine(res, tags.JobState);
        NYT::HashCombine(res, tags.JobType);
        NYT::HashCombine(res, tags.PoolTree);
        return res;
    }
};
