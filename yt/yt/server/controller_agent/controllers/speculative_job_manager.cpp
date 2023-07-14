#include "speculative_job_manager.h"
#include "job_info.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/progress_counter.h>
#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <util/generic/algorithm.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

TSpeculativeJobManager::TSpeculativeJobManager(
    ICompetitiveJobManagerHost* host,
    NLogging::TLogger logger,
    int maxSpeculativeJobCount)
    : TCompetitiveJobManagerBase(
        host,
        logger,
        maxSpeculativeJobCount,
        EJobCompetitionType::Speculative,
        EAbortReason::SpeculativeCompetitorResultLost)
{ }

void TSpeculativeJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        BanCookie(joblet->OutputCookie);
        return;
    }

    TCompetitiveJobManagerBase::OnJobScheduled(joblet);
}

void TSpeculativeJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        return;
    }

    OnJobFinished(joblet);
    MarkCompetitionAsCompleted(joblet);

    if (auto competition = FindCompetition(joblet)) {
        auto abortReason = joblet->CompetitionType == EJobCompetitionType::Speculative
            ? EAbortReason::SpeculativeRunWon
            : EAbortReason::SpeculativeRunLost;
        for (auto competitor : competition->Competitors) {
            Host_->AsyncAbortJob(competitor, abortReason);
        }
    }
}

bool TSpeculativeJobManager::OnUnsuccessfulJobFinish(
    const TJobletPtr& joblet,
    const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
    NJobTrackerClient::EJobState /*state*/)
{
    if (!IsRelevant(joblet)) {
        // By default after unsuccessful finish of job a cookie is returned to chunk pool.
        return true;
    }

    auto competition = GetCompetition(joblet);
    bool jobIsLoser = (competition->Status == ECompetitionStatus::HasCompletedJob);

    OnJobFinished(joblet);

    // We are updating our counter for job losers and for non-last jobs only.
    bool jobIsNotLast = (FindCompetition(joblet) != nullptr);
    if (jobIsLoser || jobIsNotLast) {
        updateJobCounter(&competition->ProgressCounterGuard);
        competition->ProgressCounterGuard.SetCategory(EProgressCategory::None);
        return false;
    }
    return true;
}

std::optional<EAbortReason> TSpeculativeJobManager::ShouldAbortCompletingJob(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        return std::nullopt;
    }

    auto competition = GetCompetition(joblet);
    if (competition->Status == ECompetitionStatus::HasCompletedJob) {
        return joblet->CompetitionType == EJobCompetitionType::Speculative
            ? EAbortReason::SpeculativeRunLost
            : EAbortReason::SpeculativeRunWon;
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
