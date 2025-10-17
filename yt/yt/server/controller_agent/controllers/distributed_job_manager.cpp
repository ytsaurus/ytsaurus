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
    const auto& group = GetOrCrash(CookieToGroup_, cookie);
    return {cookie, group.Secondaries.size() - group.Pending + 1};
}

bool TDistributedJobManager::OnJobAborted(const TJobletPtr& joblet, EAbortReason /*reason*/)
{
    return OnUnsuccessfulJobFinish(joblet);
}

bool TDistributedJobManager::OnJobFailed(const TJobletPtr& joblet)
{
    return OnUnsuccessfulJobFinish(joblet);
}

void TDistributedJobManager::OnJobLost(IChunkPoolOutput::TCookie)
{ }

void TDistributedJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return;
    }
    if (joblet->CookieGroupInfo.OutputIndex == 0) {
        auto mainJobId = joblet->JobId;
        auto it = TryEmplaceOrCrash(CookieToGroup_, joblet->OutputCookie);
        auto& group = it->second;

        YT_LOG_DEBUG("Distributed job group created (MainJobId: %v, OutputCookie: %v)", mainJobId, joblet->OutputCookie);

        for (int i = 1; i < GetCookieGroupSize(); i++) {
            auto guard = TProgressCounterGuard(JobCounter_);
            guard.SetCategory(EProgressCategory::Pending);
            group.Secondaries.push_back({.ProgressCounterGuard = std::move(guard)});
        }
        group.MainJobId = mainJobId;
        joblet->CookieGroupInfo.MainJobId = mainJobId;
        group.Pending = GetCookieGroupSize() - 1;
        InsertOrCrash(PendingCookies_, joblet->OutputCookie);
    } else {
        auto& group = GetOrCrash(CookieToGroup_, joblet->OutputCookie);
        auto& secondary = group.Secondaries[joblet->CookieGroupInfo.OutputIndex - 1];
        secondary.JobId = joblet->JobId;
        secondary.ProgressCounterGuard.SetCategory(EProgressCategory::Running);
        joblet->CookieGroupInfo.MainJobId = group.MainJobId;
        --group.Pending;
        if (!group.Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }
    }
}

void TDistributedJobManager::OnOperationRevived()
{
    auto cookieToGroupCopy = CookieToGroup_;
    for (auto& [cookie, group] : cookieToGroupCopy) {
        if (group.Pending) {
            Task_->GetTaskHost()->AbortJob(group.MainJobId, EAbortReason::CookieGroupDisbanded);
        }
    }
}

bool TDistributedJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return true;
    }

    auto groupIt = CookieToGroup_.find(joblet->OutputCookie);
    if (groupIt != CookieToGroup_.end()) {
        auto& group = groupIt->second;
        if (joblet->CookieGroupInfo.OutputIndex != 0) {
            group.Secondaries[joblet->CookieGroupInfo.OutputIndex - 1].ProgressCounterGuard.SetCategory(EProgressCategory::Completed);
        } else {
            YT_LOG_DEBUG("Distributed job group completed (MainJobId: %v, OutputCookie: %v)", group.MainJobId, joblet->OutputCookie);

            YT_VERIFY(joblet->JobId == group.MainJobId);

            CookieToGroup_.erase(groupIt);
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
    return CookieToGroup_.empty() && PendingCookies_.empty();
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

bool TDistributedJobManager::OnUnsuccessfulJobFinish(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        // By default after unsuccessful finish of job a cookie is returned to chunk pool.
        return true;
    }

    auto groupIt = CookieToGroup_.find(joblet->OutputCookie);
    if (groupIt != CookieToGroup_.end()) {
        auto& group = groupIt->second;
        Task_->GetTaskHost()->AsyncAbortJob(group.MainJobId, EAbortReason::CookieGroupDisbanded);
        for (auto& secondary : group.Secondaries) {
            if (secondary.JobId) {
                Task_->GetTaskHost()->AsyncAbortJob(secondary.JobId, EAbortReason::CookieGroupDisbanded);
                secondary.ProgressCounterGuard.OnAborted(EAbortReason::CookieGroupDisbanded);
            } else {
                secondary.ProgressCounterGuard.SetCategory(EProgressCategory::None);
            }
        }
        if (group.Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }

        YT_LOG_DEBUG("Distributed job group aborted (MainJobId: %v, OutputCookie: %v)", group.MainJobId, joblet->OutputCookie);

        CookieToGroup_.erase(groupIt);
        return true;
    }
    return false;
}

void TDistributedJobManager::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, CookieToGroup_);
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

void TDistributedJobManager::TGroup::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, MainJobId);
    PHOENIX_REGISTER_FIELD(2, Secondaries);
    PHOENIX_REGISTER_FIELD(3, Pending);
}

PHOENIX_DEFINE_TYPE(TDistributedJobManager::TGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

