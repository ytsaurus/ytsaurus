#include "competitive_job_manager.h"
#include "job_info.h"
#include "serialize.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TCompetitiveJobManager::TCompetitiveJobManager(
    std::function<void(TJobId, EAbortReason)> abortJobCallback,
    const NLogging::TLogger& logger)
    : AbortJobCallback_(std::move(abortJobCallback))
    , JobCounter_(New<TProgressCounter>(0))
    , Logger(logger)
{ }

void TCompetitiveJobManager::AddSpeculativeCandidate(const TJobletPtr& joblet)
{
    if (SpeculativeCandidates_.contains(joblet->OutputCookie)) {
        YT_LOG_DEBUG("We already have this speculative candidate (JobId: %v, Cookie: %v)",
            joblet->JobId,
            joblet->OutputCookie);
        return;
    }
    SpeculativeCandidates_.emplace(joblet->OutputCookie, joblet->InputStripeList->TotalDataWeight);
    PendingDataWeight_ += joblet->InputStripeList->TotalDataWeight;
    JobCounter_->Increment(1);

    YT_LOG_DEBUG("Speculative candidate added (JobId: %v, Cookie: %v)",
        joblet->JobId,
        joblet->OutputCookie);
}

int TCompetitiveJobManager::GetPendingSpeculativeJobCount() const
{
    return JobCounter_->GetPending();
}

int TCompetitiveJobManager::GetTotalSpeculativeJobCount() const
{
    return JobCounter_->GetTotal();
}

NChunkPools::IChunkPoolOutput::TCookie TCompetitiveJobManager::PeekSpeculativeCandidate() const
{
    YCHECK(!SpeculativeCandidates_.empty());
    return SpeculativeCandidates_.begin()->first;
}

void TCompetitiveJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (joblet->Speculative) {
        YT_LOG_DEBUG("Scheduling speculative job (JobId: %v, Cookie: %v)",
            joblet->JobId,
            joblet->OutputCookie);
        auto it = SpeculativeCandidates_.find(joblet->OutputCookie);
        YCHECK(it != SpeculativeCandidates_.end());
        PendingDataWeight_ -= it->second;
        SpeculativeCandidates_.erase(it);
        JobCounter_->Start(1);
    }
    CookieToJobIds_[joblet->OutputCookie].push_back(joblet->JobId);
}

void TCompetitiveJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    OnJobFinished(joblet);
    if (CookieToJobIds_.contains(joblet->OutputCookie)) {
        auto abortReason = joblet->Speculative
            ? EAbortReason::SpeculativeRunWon
            : EAbortReason::SpeculativeRunLost;

        const auto& loserJobIds = CookieToJobIds_[joblet->OutputCookie];
        YT_LOG_DEBUG("Job won competition; aborting other competitors (Cookie: %v, WinnerJobId: %v, LoserJobIds: %v)",
            joblet->OutputCookie,
            joblet->JobId,
            loserJobIds);
        for (const auto& competitiveJobId : loserJobIds) {
            AbortJobCallback_(competitiveJobId, abortReason);
            CompetitiveJobLosers_.insert(competitiveJobId);
        }
    }
}

bool TCompetitiveJobManager::OnJobFailed(const TJobletPtr& joblet)
{
    bool jobIsLoser = CompetitiveJobLosers_.contains(joblet->JobId);
    OnJobFinished(joblet);

    // We are updating our counter for job losers and for non-last jobs only.
    if (jobIsLoser || CookieToJobIds_.contains(joblet->OutputCookie)) {
        JobCounter_->Failed(1);
        JobCounter_->Decrement(1);
        return false;
    }
    return true;
}

bool TCompetitiveJobManager::OnJobAborted(const TJobletPtr& joblet, EAbortReason reason)
{
    bool jobIsLoser = CompetitiveJobLosers_.contains(joblet->JobId);
    OnJobFinished(joblet);

    // We are updating our counter for job losers and for non-last jobs only.
    if (jobIsLoser || CookieToJobIds_.contains(joblet->OutputCookie)) {
        JobCounter_->Aborted(1, reason);
        JobCounter_->Decrement(1);
        return false;
    }
    return true;
}

void TCompetitiveJobManager::OnJobFinished(const TJobletPtr& joblet)
{
    YCHECK(CookieToJobIds_.contains(joblet->OutputCookie));
    auto& competitiveJobs = CookieToJobIds_[joblet->OutputCookie];
    auto jobIt = Find(competitiveJobs, joblet->JobId);
    YCHECK(jobIt != competitiveJobs.end());
    competitiveJobs.erase(jobIt);
    if (competitiveJobs.empty()) {
        CookieToJobIds_.erase(joblet->OutputCookie);
    }

    auto it = SpeculativeCandidates_.find(joblet->OutputCookie);
    if (it != SpeculativeCandidates_.end()) {
        YT_LOG_DEBUG("Canceling speculative request early since original job finished (JobId: %v, Cookie: %v)",
            joblet->JobId,
            joblet->OutputCookie);
        PendingDataWeight_ -= it->second;
        SpeculativeCandidates_.erase(it);
        JobCounter_->Decrement(1);
    }

    // We do not care if losers contains this finished job.
    CompetitiveJobLosers_.erase(joblet->JobId);
}

std::optional<EAbortReason> TCompetitiveJobManager::ShouldAbortJob(const TJobletPtr& joblet) const
{
    if (!CompetitiveJobLosers_.contains(joblet->JobId)) {
        return std::nullopt;
    }

    return joblet->Speculative
        ? EAbortReason::SpeculativeRunLost
        : EAbortReason::SpeculativeRunWon;
}

i64 TCompetitiveJobManager::GetPendingCandidatesDataWeight() const
{
    return PendingDataWeight_;
}

bool TCompetitiveJobManager::IsFinished() const
{
    return SpeculativeCandidates_.empty() && CookieToJobIds_.empty() && CompetitiveJobLosers_.empty();
}

TProgressCounterPtr TCompetitiveJobManager::GetProgressCounter()
{
    return JobCounter_;
}

void TCompetitiveJobManager::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CookieToJobIds_);
    Persist(context, SpeculativeCandidates_);
    Persist(context, CompetitiveJobLosers_);
    Persist(context, PendingDataWeight_);
    Persist(context, JobCounter_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
