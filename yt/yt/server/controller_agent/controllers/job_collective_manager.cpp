#include "job_collective_manager.h"

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

TJobCollectiveManager::TJobCollectiveManager(
    TTask* task,
    NLogging::TLogger logger)
    : JobCounter_(New<TProgressCounter>())
    , Task_(task)
    , Logger(std::move(logger))
{ }

i64 TJobCollectiveManager::GetPendingCandidatesDataWeight() const
{
    return 0;
}

int TJobCollectiveManager::GetPendingJobCount() const
{
    return JobCounter_->GetPending();
}

int TJobCollectiveManager::GetTotalJobCount() const
{
    return JobCounter_->GetTotal();
}

std::pair<NChunkPools::IChunkPoolOutput::TCookie, int> TJobCollectiveManager::PeekJobCandidate()
{
    YT_VERIFY(!PendingCookies_.empty());
    auto cookie = *PendingCookies_.begin();
    const auto& collective = GetOrCrash(CookieToCollective_, cookie);
        return {cookie, collective.Slaves.size() - collective.Pending + 1};
}

bool TJobCollectiveManager::OnJobAborted(const TJobletPtr& joblet, EAbortReason /*reason*/)
{
    return OnUnsuccessfulJobFinish(joblet);
}

bool TJobCollectiveManager::OnJobFailed(const TJobletPtr& joblet)
{
    return OnUnsuccessfulJobFinish(joblet);
}

void TJobCollectiveManager::OnJobLost(IChunkPoolOutput::TCookie)
{ }

void TJobCollectiveManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return;
    }
    if (joblet->CollectiveInfo.Rank == 0) {
        auto masterJobId = joblet->JobId;
        auto it = EmplaceOrCrash(CookieToCollective_, joblet->OutputCookie, TCollective());
        auto& collective = it->second;

        YT_LOG_DEBUG("Distributed job collective created (MasterJobId: %v, OutputCookie: %v)", masterJobId, joblet->OutputCookie);

        for (int i = 1; i < GetCollectiveSize(); i++) {
            auto guard = TProgressCounterGuard(JobCounter_);
            guard.SetCategory(EProgressCategory::Pending);
            collective.Slaves.push_back({.ProgressCounterGuard = std::move(guard)});
        }
        collective.MasterJobId = masterJobId;
        joblet->CollectiveInfo.CollectiveId = masterJobId.Underlying();
        collective.Pending = GetCollectiveSize() - 1;
        InsertOrCrash(PendingCookies_, joblet->OutputCookie);
    } else {
        auto& collective = GetOrCrash(CookieToCollective_, joblet->OutputCookie);
        auto& slave = collective.Slaves[joblet->CollectiveInfo.Rank - 1];
        slave.JobId = joblet->JobId;
        slave.ProgressCounterGuard.SetCategory(EProgressCategory::Running);
        joblet->CollectiveInfo.CollectiveId = collective.MasterJobId.Underlying();
        --collective.Pending;
        if (!collective.Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }
    }
}

void TJobCollectiveManager::OnOperationRevived()
{
    // COMPAT(pogorelov): Remove in 25.4.
    if (!Task_) {
        return;
    }

    auto cookieToCollectiveCopy = CookieToCollective_;
    for (auto& [cookie, collective] : cookieToCollectiveCopy) {
        if (collective.Pending) {
            Task_->GetTaskHost()->AbortJob(collective.MasterJobId, EAbortReason::DistributedJobGroupDisbanded);
        }
    }
}

bool TJobCollectiveManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        return true;
    }

    auto collectiveIt = CookieToCollective_.find(joblet->OutputCookie);
    if (collectiveIt != CookieToCollective_.end()) {
        auto& collective = collectiveIt->second;
        if (joblet->CollectiveInfo.Rank != 0) {
            collective.Slaves[joblet->CollectiveInfo.Rank - 1].ProgressCounterGuard.SetCategory(EProgressCategory::Completed);
        } else {
            YT_LOG_DEBUG("Distributed job collective completed (MasterJobId: %v, OutputCookie: %v)", collective.MasterJobId, joblet->OutputCookie);

            YT_VERIFY(joblet->JobId == collective.MasterJobId);

            CookieToCollective_.erase(collectiveIt);
            return true;
        }
    }
    return false;
}

std::optional<EAbortReason> TJobCollectiveManager::ShouldAbortCompletingJob(const TJobletPtr&)
{
    return std::nullopt;
}

bool TJobCollectiveManager::IsFinished() const
{
    return CookieToCollective_.empty() && PendingCookies_.empty();
}

TProgressCounterPtr TJobCollectiveManager::GetProgressCounter() const
{
    return JobCounter_;
}

int TJobCollectiveManager::GetCollectiveSize() const
{
    // COMPAT(pogorelov): Remove in 25.4.
    if (!Task_) {
        return 1;
    }
    auto userJobSpec = Task_->GetUserJobSpec();
    return userJobSpec && userJobSpec->CollectiveOptions ? userJobSpec->CollectiveOptions->Size : 1;
}

// COMPAT(pogorelov): Remove in 25.4.
void TJobCollectiveManager::InitializeCounter()
{
    JobCounter_ = New<TProgressCounter>();
}

bool TJobCollectiveManager::IsRelevant() const
{
    return GetCollectiveSize() > 1;
}

bool TJobCollectiveManager::OnUnsuccessfulJobFinish(const TJobletPtr& joblet)
{
    if (!IsRelevant()) {
        // By default after unsuccessful finish of job a cookie is returned to chunk pool.
        return true;
    }

    auto collectiveIt = CookieToCollective_.find(joblet->OutputCookie);
    if (collectiveIt != CookieToCollective_.end()) {
        auto& collective = collectiveIt->second;
        Task_->GetTaskHost()->AsyncAbortJob(collective.MasterJobId, EAbortReason::DistributedJobGroupDisbanded);
        for (auto& slave : collective.Slaves) {
            if (slave.JobId) {
                Task_->GetTaskHost()->AsyncAbortJob(slave.JobId, EAbortReason::DistributedJobGroupDisbanded);
                slave.ProgressCounterGuard.OnAborted(EAbortReason::DistributedJobGroupDisbanded);
            } else {
                slave.ProgressCounterGuard.SetCategory(EProgressCategory::None);
            }
        }
        if (collective.Pending) {
            EraseOrCrash(PendingCookies_, joblet->OutputCookie);
        }

        YT_LOG_DEBUG("Distributed job collective aborted (MasterJobId: %v, OutputCookie: %v)", collective.MasterJobId, joblet->OutputCookie);

        CookieToCollective_.erase(collectiveIt);
        return true;
    }
    return false;
}

void TJobCollectiveManager::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, CookieToCollective_);
    PHOENIX_REGISTER_FIELD(2, PendingCookies_);
    PHOENIX_REGISTER_FIELD(3, JobCounter_);
    PHOENIX_REGISTER_FIELD(4, Task_);
    PHOENIX_REGISTER_FIELD(5, Logger);
}

PHOENIX_DEFINE_TYPE(TJobCollectiveManager);

void TJobCollectiveManager::TSlave::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobId);
    PHOENIX_REGISTER_FIELD(2, ProgressCounterGuard);
}

PHOENIX_DEFINE_TYPE(TJobCollectiveManager::TSlave);

void TJobCollectiveManager::TCollective::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, MasterJobId);
    PHOENIX_REGISTER_FIELD(2, Slaves);
    PHOENIX_REGISTER_FIELD(3, Pending);
}

PHOENIX_DEFINE_TYPE(TJobCollectiveManager::TCollective);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
