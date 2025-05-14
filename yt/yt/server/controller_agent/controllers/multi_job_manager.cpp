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
    : JobCounter_(New<TProgressCounter>())
    , Task_(task)
    , Logger(std::move(logger))
{ }

i64 TMultiJobManager::GetPendingCandidatesDataWeight() const
{
    return 0;
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
    YT_VERIFY(!PendingCookies_.empty());
    auto cookie = *PendingCookies_.begin();
    const auto& replicas = GetOrCrash(CookieToReplicas_, cookie);
    return {cookie, replicas.Secondaries.size() - replicas.Pending + 1};
}

bool TMultiJobManager::OnJobAborted(const TJobletPtr& joblet, EAbortReason reason)
{
    return OnUnsuccessfulJobFinish(joblet, reason);
}

bool TMultiJobManager::OnJobFailed(const TJobletPtr& joblet)
{
    return OnUnsuccessfulJobFinish(joblet, EAbortReason::CookieGroupDisbanded);
}

void TMultiJobManager::OnJobLost(IChunkPoolOutput::TCookie)
{ }

void TMultiJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return;
    }
    if (joblet->MultiJob.OutputCookieGroupIndex == 0) {
        auto [it, inserted] = CookieToReplicas_.try_emplace(joblet->OutputCookie);
        YT_VERIFY(inserted);
        auto& replicas = it->second;
        for (int i = 1; i < GetCookieGroupSize(); i++) {
            auto guard = TProgressCounterGuard(JobCounter_);
            guard.SetCategory(EProgressCategory::Pending);
            replicas.Secondaries.push_back({.ProgressCounterGuard = std::move(guard)});
        }
        replicas.MainJobId = joblet->JobId;
        replicas.Pending = GetCookieGroupSize() - 1;
        replicas.NotCompletedCount = GetCookieGroupSize();
        InsertOrCrash(PendingCookies_, joblet->OutputCookie);
    } else {
        auto& replicas = GetOrCrash(CookieToReplicas_, joblet->OutputCookie);
        auto& secondary = replicas.Secondaries[joblet->MultiJob.OutputCookieGroupIndex - 1];
        secondary.JobId = joblet->JobId;
        secondary.ProgressCounterGuard.SetCategory(EProgressCategory::Running);
        joblet->MultiJob.MainJobId = replicas.MainJobId;
        --replicas.Pending;
        if (!replicas.Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }
    }
}

bool TMultiJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return true;
    }

    auto replicasIt = CookieToReplicas_.find(joblet->OutputCookie);
    if (replicasIt != CookieToReplicas_.end()) {
        auto& replicas = replicasIt->second;
        if (joblet->MultiJob.OutputCookieGroupIndex != 0) {
            replicas.Secondaries[joblet->MultiJob.OutputCookieGroupIndex - 1].ProgressCounterGuard.SetCategory(EProgressCategory::Completed);
        }
        --replicas.NotCompletedCount;
        YT_VERIFY(replicas.NotCompletedCount >= 0);
        if (!replicas.NotCompletedCount) {
            CookieToReplicas_.erase(replicasIt);
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

int TMultiJobManager::GetCookieGroupSize() const {
    auto userJobSpec = Task_->GetUserJobSpec();
    return userJobSpec ? userJobSpec->CookieGroupSize : 1;
}

bool TMultiJobManager::IsRelevant() const
{
    return GetCookieGroupSize() > 1;
}

void TMultiJobManager::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, CookieToReplicas_);
    PHOENIX_REGISTER_FIELD(2, PendingCookies_);
    PHOENIX_REGISTER_FIELD(3, JobCounter_);
    PHOENIX_REGISTER_FIELD(4, Task_);
    PHOENIX_REGISTER_FIELD(5, Logger);
}

PHOENIX_DEFINE_TYPE(TMultiJobManager);

bool TMultiJobManager::OnUnsuccessfulJobFinish(
    const TJobletPtr& joblet,
    EAbortReason abortReason)
{
    if (!IsRelevant()) {
        // By default after unsuccessful finish of job a cookie is returned to chunk pool.
        return true;
    }

    auto replicasIt = CookieToReplicas_.find(joblet->OutputCookie);
    if (replicasIt != CookieToReplicas_.end()) {
        auto& replicas = replicasIt->second;
        Task_->GetTaskHost()->AsyncAbortJob(replicas.MainJobId, abortReason);
        for (const auto& secondary : replicas.Secondaries) {
            Task_->GetTaskHost()->AsyncAbortJob(secondary.JobId, abortReason);
        }
        CookieToReplicas_.erase(replicasIt);
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
    PHOENIX_REGISTER_FIELD(4, NotCompletedCount);
}

PHOENIX_DEFINE_TYPE(TMultiJobManager::TReplicas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

