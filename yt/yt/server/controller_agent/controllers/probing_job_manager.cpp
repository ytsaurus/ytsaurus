#include "probing_job_manager.h"
#include "job_info.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <util/generic/algorithm.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TProbingJobManager::TProbingJobManager()
    : ProbingRatio_(0)
    , RandomGenerator_(RandomDevice_())
{ }

TProbingJobManager& TProbingJobManager::operator=(const TProbingJobManager& other)
{
    ProbingRatio_ = other.ProbingRatio_;
    ProbingPoolTreeId_ = other.ProbingPoolTreeId_;
    return *this;
}

TProbingJobManager::TProbingJobManager(
    ICompetitiveJobManagerHost* host,
    NLogging::TLogger logger,
    int maxProbingJobCount,
    std::optional<double> probingRatio,
    std::optional<std::string> probingPoolTreeId)
    : TCompetitiveJobManagerBase(
        host,
        logger,
        maxProbingJobCount,
        EJobCompetitionType::Probing,
        EAbortReason::ProbingCompetitorResultLost)
    , ProbingRatio_(probingRatio)
    , ProbingPoolTreeId_(std::move(probingPoolTreeId))
    , RandomGenerator_(RandomDevice_())
{ }

void TProbingJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        BanCookie(joblet->OutputCookie);
        return;
    }

    TCompetitiveJobManagerBase::OnJobScheduled(joblet);

    if (ProbingRatio_) {
        std::uniform_real_distribution distribution(0.0, 1.0);

        if (!joblet->CompetitionType && distribution(RandomGenerator_) < *ProbingRatio_) {
            TryAddCompetitiveJob(joblet);
        }
    }
}

void TProbingJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        return;
    }

    OnJobFinished(joblet);
    MarkCompetitionAsCompleted(joblet);
}

bool TProbingJobManager::OnUnsuccessfulJobFinish(
    const TJobletPtr& joblet,
    const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
    NJobTrackerClient::EJobState /*state*/)
{
    if (!IsRelevant(joblet)) {
        // By default after unsuccessful finish of job a cookie is returned to chunk pool.
        return true;
    }

    auto competition = GetCompetition(joblet);
    bool returnCookieToChunkPool = true;

    if (competition->IsNonTrivial) {
        if (competition->Status == ECompetitionStatus::TwoCompetitiveJobs) {
            returnCookieToChunkPool = false;
            if (!joblet->CompetitionType) {
                Host_->AsyncAbortJob(competition->GetCompetitorFor(joblet->JobId), EAbortReason::ProbingToUnsuccessfulJob);
            }
        } else if (competition->Status == ECompetitionStatus::HasCompletedJob) {
            returnCookieToChunkPool = false;
        } else if (competition->Status == ECompetitionStatus::SingleJobOnly) {
            returnCookieToChunkPool = true;
        }
    }

    OnJobFinished(joblet);

    // We are updating our counter for jobs.
    if (!returnCookieToChunkPool) {
        updateJobCounter(&competition->ProgressCounterGuard);
        competition->ProgressCounterGuard.SetCategory(EProgressCategory::None);
    }

    return returnCookieToChunkPool;
}

std::optional<EAbortReason> TProbingJobManager::ShouldAbortCompletingJob(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        return std::nullopt;
    }

    auto competition = GetCompetition(joblet);
    if (competition->Status == ECompetitionStatus::HasCompletedJob) {
        return joblet->CompetitionType == EJobCompetitionType::Probing
            ? EAbortReason::ProbingRunLost
            : EAbortReason::ProbingRunWon;
    }
    return std::nullopt;
}

void TProbingJobManager::UpdatePendingJobCount(TCompositePendingJobCount* pendingJobCount) const
{
    if (ProbingPoolTreeId_) {
        pendingJobCount->CountByPoolTree[*ProbingPoolTreeId_] = GetPendingJobCount();
    }
}

void TProbingJobManager::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCompetitiveJobManagerBase>();

    PHOENIX_REGISTER_FIELD(1, ProbingRatio_);
    PHOENIX_REGISTER_FIELD(2, ProbingPoolTreeId_);
}

PHOENIX_DEFINE_TYPE(TProbingJobManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
