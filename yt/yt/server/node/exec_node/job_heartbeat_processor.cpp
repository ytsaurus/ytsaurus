#include "job_heartbeat_processor.h"
#include "bootstrap.h"
#include "private.h"
#include "slot_manager.h"

#include <yt/yt/server/node/exec_node/private.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

using TJobController = NJobAgent::TJobController;
using IJobPtr = NJobAgent::IJobPtr;
using namespace NObjectClient;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TSchedulerJobHeartbeatProcessor::ProcessResponse(
    const TJobController::TRspHeartbeatPtr& response)
{
    ProcessHeartbeatCommonResponsePart(response);

    auto jobIdsToConfirm = FromProto<std::vector<TJobId>>(response->jobs_to_confirm());
    JobIdsToConfirm_.clear();
    if (!std::empty(jobIdsToConfirm)) {
        JobIdsToConfirm_.insert(std::cbegin(jobIdsToConfirm), std::cend(jobIdsToConfirm));
    }

    YT_VERIFY(response->Attachments().empty());

    std::vector<TJobStartInfo> jobWithoutSpecStartInfos;
    jobWithoutSpecStartInfos.reserve(response->jobs_to_start_size());
    for (const auto& startInfo : response->jobs_to_start()) {
        jobWithoutSpecStartInfos.push_back(startInfo);
    }
    Y_UNUSED(WaitFor(RequestJobSpecsAndStartJobs(std::move(jobWithoutSpecStartInfos))));
}

void TSchedulerJobHeartbeatProcessor::PrepareRequest(
    TCellTag cellTag,
    const TJobController::TReqHeartbeatPtr& request)
{
    PrepareHeartbeatCommonRequestPart(request);

    auto* execNodeBootstrap = Bootstrap_->GetExecNodeBootstrap();
    if (execNodeBootstrap->GetSlotManager()->HasFatalAlert()) {
        // NB(psushin): if slot manager is disabled with fatal alert we might have experienced an unrecoverable failure (e.g. hanging Porto)
        // and to avoid inconsistent state with scheduler we decide not to report to it any jobs at all.
        // We also drop all scheduler jobs from |JobMap_|.
        RemoveSchedulerJobsOnFatalAlert();

        request->set_confirmed_job_count(0);

        return;
    }

    const bool totalConfirmation = NeedTotalConfirmation();
    YT_LOG_INFO_IF(totalConfirmation, "Including all stored jobs in heartbeat");

    int confirmedJobCount = 0;
    i64 completedJobsStatisticsSize = 0;

    // A container for all scheduler jobs that are candidate to send statistics. This set contains
    // only the running jobs since all completed/aborted/failed jobs always send their statistics.
    std::vector<std::pair<IJobPtr, TJobStatus*>> runningJobs;

    for (const auto& job : JobController_->GetJobs()) {
        auto jobId = job->GetId();

        if (CellTagFromId(jobId) != cellTag || TypeFromId(jobId) != EObjectType::SchedulerJob) {
            continue;
        }

        auto confirmIt = JobIdsToConfirm_.find(jobId);
        if (job->GetStored() && !totalConfirmation && confirmIt == std::cend(JobIdsToConfirm_)) {
            continue;
        }

        if (job->GetStored() || confirmIt != std::cend(JobIdsToConfirm_)) {
            YT_LOG_DEBUG("Confirming job (JobId: %v, OperationId: %v, Stored: %v, State: %v)",
                jobId,
                job->GetOperationId(),
                job->GetStored(),
                job->GetState());
            ++confirmedJobCount;
        }
        if (confirmIt != std::cend(JobIdsToConfirm_)) {
            JobIdsToConfirm_.erase(confirmIt);
        }

        auto* jobStatus = request->add_jobs();
        FillJobStatus(jobStatus, job);
        switch (job->GetState()) {
            case EJobState::Running:
                *jobStatus->mutable_resource_usage() = job->GetResourceUsage();
                runningJobs.emplace_back(job, jobStatus);
                break;

            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed:
                *jobStatus->mutable_result() = job->GetResult();
                if (auto statistics = job->GetStatistics()) {
                    auto statisticsString = statistics.ToString();
                    completedJobsStatisticsSize += statisticsString.size();
                    job->ResetStatisticsLastSendTime();
                    jobStatus->set_statistics(statisticsString);
                }
                break;

            default:
                break;
        }
    }

    request->set_confirmed_job_count(confirmedJobCount);
    std::sort(
        runningJobs.begin(),
        runningJobs.end(),
        [] (const auto& lhs, const auto& rhs) {
            return lhs.first->GetStatisticsLastSendTime() < rhs.first->GetStatisticsLastSendTime();
        });

    i64 runningJobsStatisticsSize = 0;
    for (const auto& [job, jobStatus] : runningJobs) {
        if (auto statistics = job->GetStatistics()) {
            auto statisticsString = statistics.ToString();
            if (StatisticsThrottlerTryAcquire(statisticsString.size())) {
                runningJobsStatisticsSize += statisticsString.size();
                job->ResetStatisticsLastSendTime();
                jobStatus->set_statistics(statisticsString);
            }
        }
    }

    YT_LOG_DEBUG("Job statistics prepared (RunningJobsStatisticsSize: %v, CompletedJobsStatisticsSize: %v)",
        runningJobsStatisticsSize,
        completedJobsStatisticsSize);

    for (auto [jobId, operationId] : GetSpecFetchFailedJobIds()) {
        auto* jobStatus = request->add_jobs();
        ToProto(jobStatus->mutable_job_id(), jobId);
        ToProto(jobStatus->mutable_operation_id(), operationId);
        jobStatus->set_job_type(static_cast<int>(EJobType::SchedulerUnknown));
        jobStatus->set_state(static_cast<int>(EJobState::Aborted));
        jobStatus->set_phase(static_cast<int>(EJobPhase::Missing));
        jobStatus->set_progress(0.0);

        TJobResult jobResult;
        auto error = TError("Failed to get job spec")
            << TErrorAttribute("abort_reason", NScheduler::EAbortReason::GetSpecFailed);
        ToProto(jobResult.mutable_error(), error);
        *jobStatus->mutable_result() = jobResult;
    }

    if (!std::empty(JobIdsToConfirm_)) {
        YT_LOG_WARNING("Unconfirmed jobs found (UnconfirmedJobCount: %v)", std::size(JobIdsToConfirm_));
        for (auto jobId : JobIdsToConfirm_) {
            YT_LOG_DEBUG("Unconfirmed job (JobId: %v)", jobId);
        }
        ToProto(request->mutable_unconfirmed_jobs(), JobIdsToConfirm_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
