#include "multi_job_manager.h"
#include "job_info.h"
#include "task.h"

#include <yt/yt/server/controller_agent/controllers/task_host.h>
#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>
#include <yt/yt/ytlib/scheduler/config.h>

#include <util/generic/algorithm.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

TMultiJobManager::TMultiJobManager(
    TTask* task,
    NLogging::TLogger logger)
    : JobCounter_(New<TProgressCounter>()), Task_(task), Logger(logger)
{ }

i64 TMultiJobManager::GetPendingCandidatesDataWeight() const
{
    return PendingDataWeight_;
}

int TMultiJobManager::GetPendingJobCount() const
{
    return JobCounter_->GetPending();
}

int TMultiJobManager::GetTotalJobCount() const
{
    return JobCounter_->GetTotal();
}

std::pair<NChunkPools::IChunkPoolOutput::TCookie, int> TMultiJobManager::PeekJobCandidate() const
{
    YT_LOG_WARNING("PeekJobCandidate");
    YT_VERIFY(!PendingCookies_.empty());
    auto cookie = *PendingCookies_.begin();
    auto replicas = GetOrCrash(CookieToReplicas_, cookie);
    return {cookie, replicas->Secondaries.size() - replicas->Pending + 1};
}

bool TMultiJobManager::OnJobAborted(const TJobletPtr& joblet, EAbortReason reason)
{
    return OnUnsuccessfulJobFinish(
        joblet,
        reason);
}

bool TMultiJobManager::OnJobFailed(const TJobletPtr& joblet)
{
    return OnUnsuccessfulJobFinish(
        joblet,
        EAbortReason::OperationFailed);
}

void TMultiJobManager::OnJobLost(IChunkPoolOutput::TCookie)
{
}

void TMultiJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    YT_LOG_WARNING("OnJobScheduled %v", joblet->OutputCookieGroupIndex);
    if (!IsRelevant(joblet)) {
        return;
    }
    if (joblet->OutputCookieGroupIndex == 0) {
        auto [it, inserted] = CookieToReplicas_.emplace(joblet->OutputCookie, New<TReplicas>());
        YT_VERIFY(inserted);
        auto replicas = it->second;
        for (int i = 1; i < joblet->CookieGroupSize; i++) {
            auto guard = TProgressCounterGuard(JobCounter_);
            guard.SetCategory(EProgressCategory::Pending);
            replicas->Secondaries.push_back({.ProgressCounterGuard = std::move(guard)});
        }
        replicas->MainJobId = joblet->JobId;
        replicas->Pending = joblet->CookieGroupSize - 1;
        replicas->NotCompleted = joblet->CookieGroupSize;
        InsertOrCrash(PendingCookies_, joblet->OutputCookie);
    } else {
        auto replicas = GetOrCrash(CookieToReplicas_, joblet->OutputCookie);
        auto &secondary = replicas->Secondaries[joblet->OutputCookieGroupIndex - 1];
        secondary.JobId = joblet->JobId;
        secondary.ProgressCounterGuard.SetCategory(EProgressCategory::Running);
        joblet->MainJobId = replicas->MainJobId;
        replicas->Pending--;
        if (!replicas->Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }
    }
}

bool TMultiJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        return true;
    }

    auto replicas = CookieToReplicas_.find(joblet->OutputCookie);
    if (replicas != CookieToReplicas_.end()) {
        if (joblet->OutputCookieGroupIndex != 0) {
            replicas->second->Secondaries[joblet->OutputCookieGroupIndex - 1].ProgressCounterGuard.SetCategory(EProgressCategory::Completed);
        }
        --replicas->second->NotCompleted;
        YT_VERIFY(replicas->second->NotCompleted >= 0);
        if (!replicas->second->NotCompleted) {
            CookieToReplicas_.erase(joblet->OutputCookie);
            return true;
        }
    }
    return false;
}

std::optional<EAbortReason> TMultiJobManager::ShouldAbortCompletingJob(const TJobletPtr&)
{
    return std::nullopt;
}

bool TMultiJobManager::IsFinished() const
{
    return CookieToReplicas_.empty() && PendingCookies_.empty();
}

TProgressCounterPtr TMultiJobManager::GetProgressCounter() const
{
    return JobCounter_;
}

bool TMultiJobManager::IsRelevant(const TJobletPtr& joblet) const
{
    YT_LOG_WARNING("IsRelevant %v", joblet->CookieGroupSize);
    return joblet->CookieGroupSize > 1;
}

void TMultiJobManager::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, CookieToReplicas_);
    PHOENIX_REGISTER_FIELD(2, PendingCookies_);
    PHOENIX_REGISTER_FIELD(3, BannedCookies_);
    PHOENIX_REGISTER_FIELD(4, JobCounter_);
    PHOENIX_REGISTER_FIELD(5, Task_);
    PHOENIX_REGISTER_FIELD(6, Logger);
    PHOENIX_REGISTER_FIELD(7, PendingDataWeight_);
}

PHOENIX_DEFINE_TYPE(TMultiJobManager);

bool TMultiJobManager::OnUnsuccessfulJobFinish(
    const TJobletPtr& joblet,
    EAbortReason abortReason)
{
    YT_LOG_WARNING("OnUnsuccessfulJobFinish");
    if (!IsRelevant(joblet)) {
        // By default after unsuccessful finish of job a cookie is returned to chunk pool.
        return true;
    }

    auto replicas = CookieToReplicas_.find(joblet->OutputCookie);
    if (replicas != CookieToReplicas_.end()) {
        Task_->GetTaskHost()->AsyncAbortJob(replicas->second->MainJobId, abortReason);
        for (auto &secondary : replicas->second->Secondaries) {
            Task_->GetTaskHost()->AsyncAbortJob(secondary.JobId, abortReason);
        }
        CookieToReplicas_.erase(joblet->OutputCookie);
        return true;
    }
    return false;
}

void TMultiJobManager::TSecondary::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobId);
    PHOENIX_REGISTER_FIELD(2, ProgressCounterGuard);
}

PHOENIX_DEFINE_TYPE(TMultiJobManager::TSecondary);

void TMultiJobManager::TReplicas::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, MainJobId);
    PHOENIX_REGISTER_FIELD(2, Secondaries);
    PHOENIX_REGISTER_FIELD(3, Pending);
    PHOENIX_REGISTER_FIELD(4, NotCompleted);
}

PHOENIX_DEFINE_TYPE(TMultiJobManager::TReplicas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

