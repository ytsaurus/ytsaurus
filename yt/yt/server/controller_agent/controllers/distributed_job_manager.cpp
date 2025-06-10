#include "distributed_job_manager.h"

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

TDistributedJobManager::TDistributedJobManager(
    TTask* task,
    NLogging::TLogger logger)
    : JobCounter_(New<TProgressCounter>())
    , Task_(task)
    , Logger(std::move(logger))
{ }

i64 TDistributedJobManager::GetPendingCandidatesDataWeight() const
{
    return 0;
}

int TDistributedJobManager::GetPendingJobCount() const
{
    return JobCounter_->GetPending();
}

int TDistributedJobManager::GetTotalJobCount() const
{
    return JobCounter_->GetTotal();
}

std::pair<NChunkPools::IChunkPoolOutput::TCookie, int> TDistributedJobManager::PeekJobCandidate()
{
    YT_VERIFY(!PendingCookies_.empty());
    auto cookie = *PendingCookies_.begin();
    const auto& replicas = GetOrCrash(CookieToReplicas_, cookie);
    return {cookie, replicas.Secondaries.size() - replicas.Pending + 1};
}

bool TDistributedJobManager::OnJobAborted(const TJobletPtr& joblet, EAbortReason /*reason*/)
{
    return OnUnsuccessfulJobFinish(joblet, EAbortReason::CookieGroupDisbanded);
}

bool TDistributedJobManager::OnJobFailed(const TJobletPtr& joblet)
{
    return OnUnsuccessfulJobFinish(joblet, EAbortReason::CookieGroupDisbanded);
}

void TDistributedJobManager::OnJobLost(IChunkPoolOutput::TCookie)
{ }

void TDistributedJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return;
    }
    if (joblet->CookieGroupInfo.OutputIndex == 0) {
        auto it = TryEmplaceOrCrash(CookieToReplicas_, joblet->OutputCookie);
        auto& replicas = it->second;
        for (int i = 1; i < GetCookieGroupSize(); i++) {
            auto guard = TProgressCounterGuard(JobCounter_);
            guard.SetCategory(EProgressCategory::Pending);
            replicas.Secondaries.push_back({.ProgressCounterGuard = std::move(guard)});
        }
        replicas.MainJobId = joblet->JobId;
        joblet->CookieGroupInfo.MainJobId = replicas.MainJobId;
        replicas.Pending = GetCookieGroupSize() - 1;
        replicas.NotCompletedCount = GetCookieGroupSize();
        InsertOrCrash(PendingCookies_, joblet->OutputCookie);
    } else {
        auto& replicas = GetOrCrash(CookieToReplicas_, joblet->OutputCookie);
        auto& secondary = replicas.Secondaries[joblet->CookieGroupInfo.OutputIndex - 1];
        secondary.JobId = joblet->JobId;
        secondary.ProgressCounterGuard.SetCategory(EProgressCategory::Running);
        joblet->CookieGroupInfo.MainJobId = replicas.MainJobId;
        --replicas.Pending;
        if (!replicas.Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }
    }
}

bool TDistributedJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return true;
    }

    auto replicasIt = CookieToReplicas_.find(joblet->OutputCookie);
    if (replicasIt != CookieToReplicas_.end()) {
        auto& replicas = replicasIt->second;
        if (joblet->CookieGroupInfo.OutputIndex != 0) {
            replicas.Secondaries[joblet->CookieGroupInfo.OutputIndex - 1].ProgressCounterGuard.SetCategory(EProgressCategory::Completed);
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

std::optional<EAbortReason> TDistributedJobManager::ShouldAbortCompletingJob(const TJobletPtr&)
{
    return std::nullopt;
}

bool TDistributedJobManager::IsFinished() const
{
    return CookieToReplicas_.empty() && PendingCookies_.empty();
}

TProgressCounterPtr TDistributedJobManager::GetProgressCounter() const
{
    return JobCounter_;
}

int TDistributedJobManager::GetCookieGroupSize() const {
    auto userJobSpec = Task_->GetUserJobSpec();
    return userJobSpec ? userJobSpec->CookieGroupSize : 1;
}

bool TDistributedJobManager::IsRelevant() const
{
    return GetCookieGroupSize() > 1;
}

bool TDistributedJobManager::OnUnsuccessfulJobFinish(
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
        for (auto& secondary : replicas.Secondaries) {
            if (secondary.JobId) {
                Task_->GetTaskHost()->AsyncAbortJob(secondary.JobId, abortReason);
            } else {
                secondary.ProgressCounterGuard.SetCategory(EProgressCategory::None);
            }
        }
        if (replicas.Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }
        CookieToReplicas_.erase(replicasIt);
        return true;
    }
    return false;
}

void TDistributedJobManager::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, CookieToReplicas_);
    PHOENIX_REGISTER_FIELD(2, PendingCookies_);
    PHOENIX_REGISTER_FIELD(3, JobCounter_);
    PHOENIX_REGISTER_FIELD(4, Task_);
    PHOENIX_REGISTER_FIELD(5, Logger);
}

PHOENIX_DEFINE_TYPE(TDistributedJobManager);

void TDistributedJobManager::TSecondary::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobId);
    PHOENIX_REGISTER_FIELD(2, ProgressCounterGuard);
}

PHOENIX_DEFINE_TYPE(TDistributedJobManager::TSecondary);

void TDistributedJobManager::TReplicas::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, MainJobId);
    PHOENIX_REGISTER_FIELD(2, Secondaries);
    PHOENIX_REGISTER_FIELD(3, Pending);
    PHOENIX_REGISTER_FIELD(4, NotCompletedCount);
}

PHOENIX_DEFINE_TYPE(TDistributedJobManager::TReplicas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

