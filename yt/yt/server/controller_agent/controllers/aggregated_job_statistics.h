#pragma once

#include "private.h"

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TAggregatedJobStatistics
{
public:
    explicit TAggregatedJobStatistics(bool addTaskToSuffix);

    void UpdateJobStatistics(const TJobletPtr& joblet, const TJobSummary& jobSummary);

    void Persist(const TPersistenceContext& context);

    std::optional<i64> FindNumericValue(
        const TString& statisticPath,
        EJobState state,
        const TString& taskName);
    std::optional<i64> FindNumericValue(
        const TString& statisticPath,
        const TString& state,
        const TString& taskName);

    std::optional<TSummary> FindSummary(
        const TString& statisticPath,
        EJobState state,
        const TString& taskName);
    std::optional<TSummary> FindSummary(
        const TString& statisticPath,
        const TString& state,
        const TString& taskName);

private:
    TStatistics JobStatistics_;

    const bool AddTaskToSuffix_;

    static EJobState GetStatisticsJobState(const TJobletPtr& joblet, EJobState state);

    TString GetStatisticsSuffix(const TString& taskName, const TString& state);

    friend void Serialize(const TAggregatedJobStatistics& statistics, NYson::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
