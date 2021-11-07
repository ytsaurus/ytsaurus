#include "bootstrap.h"
#include "controller_agent_connector.h"
#include "job_detail.h"
#include "job_heartbeat_processor.h"
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

    std::vector<TJobId> jobIdsToConfirm;
    jobIdsToConfirm.reserve(response->jobs_to_confirm_size());
    for (auto& jobInfo : *response->mutable_jobs_to_confirm()) {
        auto jobId = FromProto<TJobId>(jobInfo.job_id());
        auto agentInfoOrError = TryParseControllerAgentDescriptor(*jobInfo.mutable_controller_agent_descriptor());
        if (!agentInfoOrError.IsOK()) {
            YT_LOG_WARNING(
                agentInfoOrError,
                "Skip job to confirm since no suitable controller agent address exists (JobId: %v)",
                jobId);
            continue;
        }

        if (const auto job = JobController_->FindJob(jobId)) {
            static_cast<TJob&>(*job).UpdateControllerAgentDescriptor(std::move(agentInfoOrError.Value()));
        }

        jobIdsToConfirm.push_back(jobId);
    }
    
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

    bool shouldSendControllerAgentHeartbeatsOutOfBand = false;

    for (const auto& job : JobController_->GetJobs()) {
        auto jobId = job->GetId();

        if (CellTagFromId(jobId) != cellTag || TypeFromId(jobId) != EObjectType::SchedulerJob) {
            continue;
        }

        auto schedulerJob = StaticPointerCast<TJob>(std::move(job));
        const bool shouldSendStatisticsToScheduler = !schedulerJob->ShouldSendJobInfoToAgent() ||
            !Bootstrap_
                ->GetExecNodeBootstrap()
                ->GetControllerAgentConnectorPool()
                ->AreHeartbeatsEnabled();

        auto confirmIt = JobIdsToConfirm_.find(jobId);
        if (schedulerJob->GetStored() && !totalConfirmation && confirmIt == std::cend(JobIdsToConfirm_)) {
            continue;
        }

        if (schedulerJob->GetStored() || confirmIt != std::cend(JobIdsToConfirm_)) {
            YT_LOG_DEBUG("Confirming job (JobId: %v, OperationId: %v, Stored: %v, State: %v)",
                jobId,
                schedulerJob->GetOperationId(),
                schedulerJob->GetStored(),
                schedulerJob->GetState());
            ++confirmedJobCount;
        }
        if (confirmIt != std::cend(JobIdsToConfirm_)) {
            JobIdsToConfirm_.erase(confirmIt);
        }

        auto* jobStatus = request->add_jobs();
        jobStatus->set_job_execution_completed(schedulerJob->IsJobProxyCompleted());
        FillJobStatus(jobStatus, schedulerJob);
        switch (schedulerJob->GetState()) {
            case EJobState::Running:
                *jobStatus->mutable_resource_usage() = schedulerJob->GetResourceUsage();
                if (shouldSendStatisticsToScheduler) {
                    runningJobs.emplace_back(job, jobStatus);
                }
                break;

            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed:
                *jobStatus->mutable_result() = schedulerJob->GetResult();
                // COMPAT(pogorelov)
                if (shouldSendStatisticsToScheduler) {
                    if (auto statistics = job->GetStatistics()) {
                        auto statisticsString = statistics.ToString();
                        completedJobsStatisticsSize += statisticsString.size();
                        job->ResetStatisticsLastSendTime();
                        jobStatus->set_statistics(statisticsString);
                    }
                } else {
                    Bootstrap_
                        ->GetExecNodeBootstrap()
                        ->GetControllerAgentConnectorPool()
                        ->EnqueueFinishedJob(schedulerJob);
                    shouldSendControllerAgentHeartbeatsOutOfBand = true;
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
            if (TryAcquireStatisticsThrottler(statisticsString.size())) {
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
        jobStatus->mutable_time_statistics();

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

    if (shouldSendControllerAgentHeartbeatsOutOfBand) {
        Bootstrap_
            ->GetExecNodeBootstrap()
            ->GetControllerAgentConnectorPool()
            ->SendOutOfBandHeartbeatsIfNeeded();
    }
}

void TSchedulerJobHeartbeatProcessor::ScheduleHeartbeat(TJobId /*jobId*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
