#include "bootstrap.h"
#include "controller_agent_connector.h"
#include "job_detail.h"
#include "job_heartbeat_processor.h"
#include "private.h"
#include "slot_manager.h"

#include <yt/yt/server/node/exec_node/private.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

using TJobController = NJobAgent::TJobController;
using IJobPtr = NJobAgent::IJobPtr;
using namespace NObjectClient;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

void TSchedulerJobHeartbeatProcessor::ProcessResponse(
    const TString& jobTrackerAddress,
    const TJobController::TRspHeartbeatPtr& response)
{
    YT_VERIFY(jobTrackerAddress.empty());

    ProcessHeartbeatCommonResponsePart(response);

    for (const auto& jobToInterrupt : response->jobs_to_interrupt()) {
        auto timeout = FromProto<TDuration>(jobToInterrupt.timeout());
        auto jobId = FromProto<TJobId>(jobToInterrupt.job_id());

        YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

        if (auto job = JobController_->FindJob(jobId)) {
            auto& schedulerJob = static_cast<TJob&>(*job);

            std::optional<TString> preemptionReason;
            if (jobToInterrupt.has_preemption_reason()) {
                preemptionReason = jobToInterrupt.preemption_reason();
            }
            std::optional<EInterruptReason> interruptionReason;
            if (jobToInterrupt.has_interruption_reason()) {
                interruptionReason = CheckedEnumCast<EInterruptReason>(jobToInterrupt.interruption_reason());
            }
            schedulerJob.Interrupt(timeout, interruptionReason, preemptionReason);
        } else {
            YT_LOG_WARNING("Requested to interrupt a non-existing job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId : response->jobs_to_fail()) {
        auto jobId = FromProto<TJobId>(protoJobId);

        YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

        if (auto job = JobController_->FindJob(jobId)) {
            auto& schedulerJob = static_cast<TJob&>(*job);

            schedulerJob.Fail();
        } else {
            YT_LOG_WARNING("Requested to fail a non-existent job (JobId: %v)",
                jobId);
        }
    }

    for (const auto& protoJobId: response->jobs_to_store()) {
        auto jobId = FromProto<TJobId>(protoJobId);

        YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

        if (auto job = JobController_->FindJob(jobId)) {
            auto& schedulerJob = static_cast<TJob&>(*job);

            YT_LOG_DEBUG("Storing job (JobId: %v)",
                jobId);
            schedulerJob.SetStored(true);
        } else {
            YT_LOG_WARNING("Requested to store a non-existent job (JobId: %v)",
                jobId);
        }
    }

    std::vector<TJobId> jobIdsToConfirm;
    jobIdsToConfirm.reserve(response->jobs_to_confirm_size());
    for (auto& jobInfo : *response->mutable_jobs_to_confirm()) {
        auto jobId = FromProto<TJobId>(jobInfo.job_id());

        YT_VERIFY(TypeFromId(jobId) == EObjectType::SchedulerJob);

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

        // We get vcpu here. Need to replace it with real cpu back.
        auto& resourceLimits = *jobWithoutSpecStartInfos.back().mutable_resource_limits();
        resourceLimits.set_cpu(static_cast<double>(NVectorHdrf::TCpuResource(resourceLimits.cpu() / LastHeartbeatCpuToVCpuFactor_)));
    }

    Y_UNUSED(WaitFor(RequestJobSpecsAndStartJobs(std::move(jobWithoutSpecStartInfos))));
}

void TSchedulerJobHeartbeatProcessor::ReplaceCpuWithVCpu(NNodeTrackerClient::NProto::TNodeResources& resources) const
{
    resources.set_cpu(static_cast<double>(NVectorHdrf::TCpuResource(resources.cpu() * LastHeartbeatCpuToVCpuFactor_)));
    resources.clear_vcpu();
}

void TSchedulerJobHeartbeatProcessor::PrepareRequest(
    TCellTag cellTag,
    const TString& jobTrackerAddress,
    const TJobController::TReqHeartbeatPtr& request)
{
    YT_VERIFY(jobTrackerAddress.empty());

    PrepareHeartbeatCommonRequestPart(request);

    // Only for scheduler `cpu` stores `vcpu` actually.
    // In all resource limits and usages we send and get back vcpu instead of cpu.
    LastHeartbeatCpuToVCpuFactor_ = JobController_->GetCpuToVCpuFactor();
    ReplaceCpuWithVCpu(*request->mutable_resource_limits());
    ReplaceCpuWithVCpu(*request->mutable_resource_usage());

    request->set_supports_interruption_logic(true);

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

    bool shouldSendControllerAgentHeartbeatsOutOfBand = false;

    for (const auto& job : JobController_->GetJobs()) {
        auto jobId = job->GetId();

        if (CellTagFromId(jobId) != cellTag || TypeFromId(jobId) != EObjectType::SchedulerJob) {
            continue;
        }

        auto schedulerJob = StaticPointerCast<TJob>(std::move(job));

        auto confirmIt = JobIdsToConfirm_.find(jobId);
        if (schedulerJob->GetStored() && !totalConfirmation && confirmIt == std::cend(JobIdsToConfirm_)) {
            continue;
        }

        const bool sendConfirmedJobToControllerAgent = schedulerJob->GetStored() &&
            confirmIt == std::cend(JobIdsToConfirm_) &&
            totalConfirmation;

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
        FillSchedulerJobStatus(jobStatus, schedulerJob);
        switch (schedulerJob->GetState()) {
            case EJobState::Running: {
                auto& resourceUsage = *jobStatus->mutable_resource_usage();
                resourceUsage = schedulerJob->GetResourceUsage();
                ReplaceCpuWithVCpu(resourceUsage);
                break;
            }
            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed: {
                const auto& controllerAgentConnector = schedulerJob->GetControllerAgentConnector();
                YT_VERIFY(controllerAgentConnector);

                *jobStatus->mutable_result() = schedulerJob->GetResultWithoutExtension();
                if (!sendConfirmedJobToControllerAgent) {
                    controllerAgentConnector->EnqueueFinishedJob(schedulerJob);
                    shouldSendControllerAgentHeartbeatsOutOfBand = true;
                }
                break;
            }
            default:
                break;
        }
    }

    request->set_confirmed_job_count(confirmedJobCount);

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
            << TErrorAttribute("abort_reason", EAbortReason::GetSpecFailed);
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

void TSchedulerJobHeartbeatProcessor::ScheduleHeartbeat(const IJobPtr& /*job*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
