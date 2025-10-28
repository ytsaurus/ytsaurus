#include "competitive_job_manager.h"
#include "job_info.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <util/generic/algorithm.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

TCompetitiveJobManagerBase::TCompetitiveJobManagerBase(
    ICompetitiveJobManagerHost* host,
    NLogging::TLogger logger,
    int maxSecondaryJobCount,
    EJobCompetitionType competitionType,
    EAbortReason resultLost)
    : Host_(host)
    , Logger(logger.WithTag("CompetitionType: %v", competitionType))
    , JobCounter_(New<TProgressCounter>())
    , MaxCompetitiveJobCount_(maxSecondaryJobCount)
    , CompetitionType_(competitionType)
    , ResultLost_(resultLost)
{ }

bool TCompetitiveJobManagerBase::TryAddCompetitiveJob(const TJobletPtr& joblet)
{
    auto Logger = this->Logger
        .WithTag("JobId: %v", joblet->JobId)
        .WithTag("Cookie: %v", joblet->OutputCookie);

    if (!IsRelevant(joblet)) {
        YT_LOG_DEBUG("Ignoring competitive job request; job is not relevant");
        return false;
    }

    if (BannedCookies_.contains(joblet->OutputCookie)) {
        YT_LOG_DEBUG("Ignoring competitive job request; cookie is banned");
        return false;
    }

    auto competition = GetOrCrash(CookieToCompetition_, joblet->OutputCookie);
    std::optional<TString> rejectReason;

    if (JobCounter_->GetTotal() >= MaxCompetitiveJobCount_) {
        YT_LOG_DEBUG("Ignoring competitive job request; competitive job limit reached (Limit: %v)", MaxCompetitiveJobCount_);
        return false;
    } else if (CompetitionCandidates_.contains(joblet->OutputCookie)) {
        YT_LOG_DEBUG("Ignoring competitive job request; competition candidate is already in queue");
        return false;
    } else if (competition->Status == ECompetitionStatus::TwoCompetitiveJobs) {
        YT_LOG_DEBUG("Ignoring competitive job request; competitive job is already running");
        return false;
    } else if (competition->Status == ECompetitionStatus::HasCompletedJob) {
        YT_LOG_DEBUG("Ignoring competitive job request; competitive job has already completed");
        return false;
    }

    competition->ProgressCounterGuard.SetCategory(EProgressCategory::Pending);
    competition->PendingDataWeight = joblet->InputStripeList->GetAggregateStatistics().DataWeight;
    InsertOrCrash(CompetitionCandidates_, joblet->OutputCookie);
    PendingDataWeight_ += joblet->InputStripeList->GetAggregateStatistics().DataWeight;
    YT_LOG_DEBUG("Competition request is registered");

    return true;
}

int TCompetitiveJobManagerBase::GetPendingJobCount() const
{
    return JobCounter_->GetPending();
}

int TCompetitiveJobManagerBase::GetTotalJobCount() const
{
    return JobCounter_->GetTotal();
}

NChunkPools::IChunkPoolOutput::TCookie TCompetitiveJobManagerBase::PeekJobCandidate() const
{
    YT_VERIFY(!CompetitionCandidates_.empty());
    return *CompetitionCandidates_.begin();
}

void TCompetitiveJobManagerBase::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        return;
    }

    if (joblet->CompetitionType == CompetitionType_) {
        YT_LOG_DEBUG("Scheduling secondary job (JobId: %v, Cookie: %v)",
            joblet->JobId,
            joblet->OutputCookie);
        auto competition = GetOrCrash(CookieToCompetition_, joblet->OutputCookie);
        YT_VERIFY(competition->Status == ECompetitionStatus::SingleJobOnly);
        competition->Competitors.push_back(joblet->JobId);
        competition->Status = ECompetitionStatus::TwoCompetitiveJobs;
        competition->IsNonTrivial = true;
        competition->ProgressCounterGuard.SetCategory(EProgressCategory::Running);
        PendingDataWeight_ -= competition->PendingDataWeight;
        EraseOrCrash(CompetitionCandidates_, joblet->OutputCookie);
        joblet->CompetitionIds[CompetitionType_] = competition->JobCompetitionId;
        Host_->OnSecondaryJobScheduled(joblet, CompetitionType_);
    } else {
        auto [it, inserted] = CookieToCompetition_.emplace(joblet->OutputCookie, New<TCompetition>());
        YT_VERIFY(inserted);
        const auto& competition = it->second;
        competition->JobCompetitionId = joblet->JobId;
        competition->Competitors.push_back(joblet->JobId);
        competition->ProgressCounterGuard = TProgressCounterGuard(JobCounter_);
        joblet->CompetitionIds[CompetitionType_] = joblet->JobId;
    }
}

bool TCompetitiveJobManagerBase::OnJobAborted(const TJobletPtr& joblet, EAbortReason reason)
{
    return OnUnsuccessfulJobFinish(
        joblet,
        [reason] (TProgressCounterGuard* guard) { guard->OnAborted(reason); },
        NJobTrackerClient::EJobState::Aborted);
}

bool TCompetitiveJobManagerBase::OnJobFailed(const TJobletPtr& joblet)
{
    return OnUnsuccessfulJobFinish(
        joblet,
        [] (TProgressCounterGuard* guard) { guard->OnFailed(); },
        NJobTrackerClient::EJobState::Failed);
}

void TCompetitiveJobManagerBase::OnJobLost(IChunkPoolOutput::TCookie cookie)
{
    auto it = CookieToCompetition_.find(cookie);
    if (it != CookieToCompetition_.end()) {
        YT_LOG_DEBUG("Aborting competitive job from controller since job result is lost (OutputCookie: %v, AbortReason: %v)",
            cookie,
            ResultLost_);
        YT_VERIFY(it->second->Competitors.size() == 1);
        // We should abort job synchronously to prevent occurrence of two original job.
        Host_->AbortJob(it->second->Competitors[0], ResultLost_);
    }
}

void TCompetitiveJobManagerBase::OnJobFinished(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        return;
    }

    auto competition = GetOrCrash(CookieToCompetition_, joblet->OutputCookie);
    auto pendingDataWeight = competition->PendingDataWeight;
    auto jobIt = Find(competition->Competitors, joblet->JobId);
    YT_VERIFY(jobIt != competition->Competitors.end());
    competition->Competitors.erase(jobIt);

    if (competition->Competitors.empty()) {
        CookieToCompetition_.erase(joblet->OutputCookie);
    } else {
        YT_VERIFY(competition->Status == ECompetitionStatus::TwoCompetitiveJobs);
        competition->Status = ECompetitionStatus::SingleJobOnly;
    }

    if (CompetitionCandidates_.erase(joblet->OutputCookie)) {
        YT_LOG_DEBUG("Canceling competitive job request early since original job finished (JobId: %v, Cookie: %v)",
            joblet->JobId,
            joblet->OutputCookie);
        PendingDataWeight_ -= pendingDataWeight;
        competition->ProgressCounterGuard.SetCategory(EProgressCategory::None);
    }
}

void TCompetitiveJobManagerBase::MarkCompetitionAsCompleted(const TJobletPtr& joblet)
{
    auto it = CookieToCompetition_.find(joblet->OutputCookie);
    if (it != CookieToCompetition_.end()) {
        auto competition = it->second;
        competition->Status = ECompetitionStatus::HasCompletedJob;

        YT_LOG_DEBUG("Job completed in non-trivial competition (Cookie: %v, WinnerJobId: %v, LoserJobIds: %v, CompetitionType: %v)",
            joblet->OutputCookie,
            joblet->JobId,
            competition->Competitors,
            joblet->CompetitionType);
    }
}

void TCompetitiveJobManagerBase::BanCookie(IChunkPoolOutput::TCookie cookie)
{
    YT_LOG_DEBUG("Competitive manager is banning cookie (Cookie: %v)", cookie);
    BannedCookies_.insert(cookie);

    if (CompetitionCandidates_.erase(cookie)) {
        auto competition = GetOrCrash(CookieToCompetition_, cookie);
        PendingDataWeight_ -= competition->PendingDataWeight;
        competition->ProgressCounterGuard.SetCategory(EProgressCategory::None);
    }
}

i64 TCompetitiveJobManagerBase::GetPendingCandidatesDataWeight() const
{
    return PendingDataWeight_;
}

bool TCompetitiveJobManagerBase::IsRelevant(const TJobletPtr& joblet) const
{
    return !joblet->CompetitionType || joblet->CompetitionType == CompetitionType_;
}

bool TCompetitiveJobManagerBase::IsFinished() const
{
    return CompetitionCandidates_.empty() && CookieToCompetition_.empty();
}

TProgressCounterPtr TCompetitiveJobManagerBase::GetProgressCounter() const
{
    return JobCounter_;
}

const TCompetitiveJobManagerBase::TCompetitionPtr& TCompetitiveJobManagerBase::GetCompetition(const TJobletPtr& joblet) const
{
    return GetOrCrash(CookieToCompetition_, joblet->OutputCookie);
}

TCompetitiveJobManagerBase::TCompetitionPtr TCompetitiveJobManagerBase::FindCompetition(const TJobletPtr& joblet) const
{
    auto it = CookieToCompetition_.find(joblet->OutputCookie);
    if (it == CookieToCompetition_.end()) {
        return nullptr;
    }
    return it->second;
}

void TCompetitiveJobManagerBase::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Host_);
    PHOENIX_REGISTER_FIELD(2, CookieToCompetition_);
    PHOENIX_REGISTER_FIELD(3, CompetitionCandidates_);
    PHOENIX_REGISTER_FIELD(4, PendingDataWeight_);
    PHOENIX_REGISTER_FIELD(5, JobCounter_);
    PHOENIX_REGISTER_FIELD(6, MaxCompetitiveJobCount_);
    PHOENIX_REGISTER_FIELD(7, CompetitionType_);
    PHOENIX_REGISTER_FIELD(8, Logger);
    PHOENIX_REGISTER_FIELD(9, BannedCookies_);
}

PHOENIX_DEFINE_TYPE(TCompetitiveJobManagerBase);

////////////////////////////////////////////////////////////////////////////////

void TCompetitiveJobManagerBase::TCompetition::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Status);
    PHOENIX_REGISTER_FIELD(2, Competitors);
    PHOENIX_REGISTER_FIELD(3, JobCompetitionId);
    PHOENIX_REGISTER_FIELD(4, PendingDataWeight);
    PHOENIX_REGISTER_FIELD(5, ProgressCounterGuard);
    PHOENIX_REGISTER_FIELD(6, IsNonTrivial);
}

TJobId TCompetitiveJobManagerBase::TCompetition::GetCompetitorFor(TJobId jobId)
{
    YT_VERIFY(Competitors.size() == 2);
    YT_VERIFY(Competitors[0] == jobId || Competitors[1] == jobId);
    return Competitors[0] == jobId
        ? Competitors[1]
        : Competitors[0];
}

PHOENIX_DEFINE_TYPE(TCompetitiveJobManagerBase::TCompetition);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
