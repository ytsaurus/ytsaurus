#include "job_tracker.h"

#include "bootstrap.h"
#include "config.h"
#include "controller_agent.h"
#include "operation.h"
#include "private.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NControllerAgent {

static const NLogging::TLogger Logger("JobTracker");

////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NScheduler;
using namespace NNodeTrackerClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////

namespace {

EJobStage JobStateToJobStage(EJobState jobState) noexcept
{
    switch (jobState) {
        case EJobState::Running:
        case EJobState::Waiting:
        case EJobState::Aborting:
            return EJobStage::Running;
        case EJobState::Failed:
        case EJobState::Aborted:
        case EJobState::Completed:
            return EJobStage::Finished;
        default:
            YT_ABORT();
    }
}

template <class TJobContainer>
std::vector<TJobId> CreateJobIdSampleForLogging(const TJobContainer& jobContainer, int maxSampleSize)
{
    std::vector<TJobId> result;

    int sampleSize = std::min(maxSampleSize, static_cast<int>(std::ssize(jobContainer)));

    result.reserve(sampleSize);
    for (int index = 0; index < sampleSize; ++index) {
        result.push_back(jobContainer[index].JobId);
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////

TJobTracker::TJobTracker(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(bootstrap->GetConfig()->ControllerAgent->JobTracker)
    , HeartbeatStatisticsBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/statistics_bytes"))
    , HeartbeatDataStatisticsBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/data_statistics_bytes"))
    , HeartbeatJobResultBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/job_result_bytes"))
    , HeartbeatProtoMessageBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/proto_message_bytes"))
    , HeartbeatEnqueuedControllerEvents_(ControllerAgentProfiler.WithHot().GaugeSummary("/node_heartbeat/enqueued_controller_events"))
    , HeartbeatCount_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/count"))
    , JobTrackerQueue_(New<NConcurrency::TActionQueue>("JobTracker"))
    , ExecNodes_(New<TRefCountedExecNodeDescriptorMap>())
{ }

TFuture<void> TJobTracker::Initialize()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    auto cancelableInvoker = Bootstrap_->GetControllerAgent()->CreateCancelableInvoker(
        GetInvoker());

    // It is called in serialized invoker to prevent reordering with DoCleanup.
    return BIND(
        &TJobTracker::DoInitialize,
        MakeStrong(this),
        Passed(std::move(cancelableInvoker)))
        .AsyncVia(GetInvoker())
        .Run();
}

void TJobTracker::OnSchedulerConnected(TIncarnationId incarnationId)
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    GetCancelableInvoker()->Invoke(BIND(
        &TJobTracker::SetIncarnationId,
        MakeStrong(this),
        incarnationId));
}

void TJobTracker::Cleanup()
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetInvoker()->Invoke(BIND(
        &TJobTracker::DoCleanup,
        MakeStrong(this)));
}

void TJobTracker::ProcessHeartbeat(const TJobTracker::TCtxHeartbeatPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* request = &context->Request();
    auto* response = &context->Response();

    auto incarnationId = FromProto<NScheduler::TIncarnationId>(request->controller_agent_incarnation_id());
    auto nodeDescriptor = FromProto<TNodeDescriptor>(request->node_descriptor());

    TNodeId nodeId = request->node_id();

    THROW_ERROR_EXCEPTION_IF(!incarnationId, EErrorCode::AgentDisconnected, "Controller agent disconnected");

    auto Logger = NControllerAgent::Logger.WithTag(
        "NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeDescriptor.GetDefaultAddress());

    ProfileHeartbeatRequest(request);
    THashMap<TOperationId, std::vector<std::unique_ptr<TJobSummary>>> groupedJobSummaries;
    std::vector<TOperationId> operationIds;
    for (auto& job : *request->mutable_jobs()) {
        auto operationId = FromProto<TOperationId>(job.operation_id());
        auto jobSummary = ParseJobSummary(&job, Logger);
        groupedJobSummaries[operationId].push_back(std::move(jobSummary));
        operationIds.push_back(operationId);
    }

    auto [operationControllers, jobEventsControllerQueue] = GetJobEventsProcessingContext(
        incarnationId,
        std::move(operationIds));

    SwitchTo(GetCancelableInvokerOrThrow());

    TForbidContextSwitchGuard guard;

    UpdateOrRegisterNode(nodeId, nodeDescriptor.GetDefaultAddress());

    auto& nodeJobs = GetOrCrash(RegisteredNodes_, nodeId).Jobs;

    // NB(pogorelov): As checked before, incarnation id did not change from the previous heartbeat,
    // so job life time of job operations must be still controlled by agent.
    {
        auto unconfirmedJobs = NYT::FromProto<std::vector<TJobId>>(request->unconfirmed_jobs());
        TOperationIdToJobIds jobsToAbort;
        for (auto jobId : unconfirmedJobs) {
            if (auto jobToConfirmIt = nodeJobs.JobsToConfirm.find(jobId);
                jobToConfirmIt != std::end(nodeJobs.JobsToConfirm))
            {
                auto operationId = jobToConfirmIt->second;
                YT_LOG_DEBUG(
                    "Job unconfirmed, abort it (JobId: %v, OperationId: %v)",
                    jobId,
                    operationId);
                jobsToAbort[operationId].push_back(jobId);

                nodeJobs.JobsToConfirm.erase(jobToConfirmIt);
            }
        }

        AbortJobs(std::move(jobsToAbort), EAbortReason::Unconfirmed);
    }

    // NB(pogorelov): Lost allocations must be aborted by scheduler now.

    THashSet<TJobId> jobsToSkip;
    jobsToSkip.reserve(std::size(nodeJobs.JobsToAbort) + std::size(nodeJobs.JobsToRelease));

    for (auto [jobId, abortReason] : nodeJobs.JobsToAbort) {
        EmplaceOrCrash(jobsToSkip, jobId);

        YT_LOG_DEBUG(
            "Request node to abort job (JobId: %v)",
            jobId);
        NProto::ToProto(response->add_jobs_to_abort(), TJobToAbort{jobId, abortReason});
    }

    nodeJobs.JobsToAbort.clear();

    {
        std::vector<TJobId> releasedJobs;
        releasedJobs.reserve(std::size(nodeJobs.JobsToRelease));
        for (const auto& [jobId, releaseFlags] : nodeJobs.JobsToRelease) {
            // NB(pogorelov): Sometimes we abort job and release it immediately. Node might release such jobs by itself, but it is less flexible.
            if (auto [it, inserted] = jobsToSkip.emplace(jobId); !inserted) {
                continue;
            }

            releasedJobs.push_back(jobId);

            YT_LOG_DEBUG(
                "Request node to remove job (JobId: %v, ReleaseFlags: %v)",
                jobId,
                releaseFlags);
            ToProto(response->add_jobs_to_remove(), NJobTrackerClient::TJobToRelease{jobId, releaseFlags});
        }

        for (auto jobId : releasedJobs) {
            EraseOrCrash(nodeJobs.JobsToRelease, jobId);
        }
    }

    const auto& heartbeatProcessingLogger = Logger;
    for (auto& [operationId, jobSummaries] : groupedJobSummaries) {
        // TODO(pogorelov): Request to confirm unreceived jobs

        auto Logger = heartbeatProcessingLogger.WithTag(
            "OperationId: %v)",
            operationId);

        auto operationControllerIt = operationControllers.find(operationId);
        if (operationControllerIt == std::end(operationControllers)) {
            YT_LOG_DEBUG("Operation is missing, skip handling job infos from node");

            ToProto(response->add_unknown_operations(), operationId);

            continue;
        }

        auto operationIt = RegisteredOperations_.find(operationId);
        if (operationIt == std::end(RegisteredOperations_)) {
            YT_LOG_DEBUG("Operation is not registered at job controller, skip handling job infos from node");

            ToProto(response->add_unknown_operations(), operationId);

            continue;
        }

        auto operationInfo = operationIt->second;
        if (!operationInfo.JobsReady) {
            YT_LOG_DEBUG("Operation jobs are not ready yet, skip handling job infos from node");

            continue;
        }

        std::vector<std::unique_ptr<TJobSummary>> jobSummariesToSendToOperationController;

        if (operationInfo.ControlJobLifetimeAtControllerAgent) {
            jobSummariesToSendToOperationController.reserve(std::size(jobSummaries));

            for (auto& jobSummary : jobSummaries) {
                auto jobId = jobSummary->Id;

                if (jobsToSkip.contains(jobId)) {
                    continue;
                }

                auto acceptJobSummaryForOperationControllerProcessing = [
                    &jobSummary,
                    &jobSummariesToSendToOperationController
                ] {
                    jobSummariesToSendToOperationController.push_back(std::move(jobSummary));
                };

                auto newJobStage = JobStateToJobStage(jobSummary->State);

                if (auto jobIt = nodeJobs.Jobs.find(jobId); jobIt != std::end(nodeJobs.Jobs)) {
                    auto& jobInfo = jobIt->second;
                    if (newJobStage == jobInfo.Stage) {
                        if (newJobStage == EJobStage::Finished) {
                            ToProto(response->add_jobs_to_store(), TJobToStore{jobId});

                            YT_LOG_DEBUG(
                                "Finished job info received again, do not process it in operation controller (JobId: %v)",
                                jobId);
                        } else {
                            acceptJobSummaryForOperationControllerProcessing();
                        }

                        continue;
                    }

                    if (jobInfo.Stage < newJobStage) {
                        jobInfo.Stage = newJobStage;
                        ToProto(response->add_jobs_to_store(), TJobToStore{jobId});

                        acceptJobSummaryForOperationControllerProcessing();

                        continue;
                    }

                    YT_LOG_DEBUG(
                        "Stale job info received (JobId: %v, CurrentJobStage: %v, ReceivedJobState: %v)",
                        jobId,
                        jobInfo.Stage,
                        jobSummary->State);

                    continue;
                }

                if (auto jobIt = nodeJobs.JobsToConfirm.find(jobId); jobIt != std::end(nodeJobs.JobsToConfirm)) {
                    YT_LOG_DEBUG(
                        "Job confirmed (JobId: %v, JobStage: %v)",
                        jobId,
                        newJobStage);
                    nodeJobs.JobsToConfirm.erase(jobIt);
                    YT_VERIFY(jobIt->second == operationId);
                    EmplaceOrCrash(nodeJobs.Jobs, jobId, TJobInfo{newJobStage, operationId});

                    acceptJobSummaryForOperationControllerProcessing();

                    continue;
                }

                bool shouldAbortJob = newJobStage == EJobStage::Running;

                YT_LOG_DEBUG(
                    "Request node to %v unknown job (JobId: %v, JobState: %v)",
                    shouldAbortJob ? "abort" : "remove",
                    jobId,
                    jobSummary->State);

                // Remove or abort unknown job.
                if (shouldAbortJob) {
                    NProto::ToProto(response->add_jobs_to_abort(), TJobToAbort{jobId, EAbortReason::Unknown});
                } else {
                    ToProto(response->add_jobs_to_remove(), NJobTrackerClient::TJobToRelease{jobId});
                }
            }
        } else {
            jobSummariesToSendToOperationController = std::move(jobSummaries);
        }

        auto operationController = std::move(operationControllerIt->second);

        AccountEnqueuedControllerEvent(+1);
        // Raw pointer is OK since the job tracker never dies.
        auto discountGuard = Finally(std::bind(&TJobTracker::AccountEnqueuedControllerEvent, this, -1));
        operationController->GetCancelableInvoker(jobEventsControllerQueue)->Invoke(
            BIND([
                    Logger,
                    operationController,
                    jobSummaries = std::move(jobSummariesToSendToOperationController),
                    discountGuard = std::move(discountGuard),
                    jobsToSkip = std::move(jobsToSkip)
                ] () mutable {
                    for (auto& jobSummary : jobSummaries) {
                        YT_VERIFY(jobSummary);

                        auto jobId = jobSummary->Id;

                        if (jobsToSkip.contains(jobId)) {
                            continue;
                        }

                        try {
                            operationController->OnJobInfoReceivedFromNode(std::move(jobSummary));
                        } catch (const std::exception& ex) {
                            YT_LOG_WARNING(
                                ex,
                                "Failed to process job info from node (JobId: %v, JobState: %v)",
                                jobId,
                                jobSummary->State);
                        }
                    }
                }));
    }

    for (auto [jobId, operationId] : nodeJobs.JobsToConfirm) {
        YT_LOG_DEBUG(
            "Request node to confirm job (JobId: %v)",
            jobId);
        ToProto(response->add_jobs_to_confirm(), TJobToConfirm{jobId});
    }

    context->Reply();
}

TJobTrackerOperationHandlerPtr TJobTracker::RegisterOperation(
    TOperationId operationId,
    bool controlJobLifetimeAtControllerAgent)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto cancelableInvoker = GetCancelableInvoker();

    cancelableInvoker->Invoke(BIND(
        &TJobTracker::DoRegisterOperation,
        MakeStrong(this),
        operationId,
        controlJobLifetimeAtControllerAgent));

    return New<TJobTrackerOperationHandler>(this, std::move(cancelableInvoker), operationId, controlJobLifetimeAtControllerAgent);
}

void TJobTracker::UnregisterOperation(
    TOperationId operationId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetCancelableInvoker()->Invoke(BIND(
        &TJobTracker::DoUnregisterOperation,
        MakeStrong(this),
        operationId));
}

void TJobTracker::UpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes)
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetCancelableInvoker()->Invoke(BIND(
        &TJobTracker::DoUpdateExecNodes,
        MakeStrong(this),
        Passed(std::move(newExecNodes))));
}

void TJobTracker::UpdateConfig(TJobTrackerConfigPtr config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetInvoker()->Invoke(BIND(
        &TJobTracker::DoUpdateConfig,
        MakeStrong(this),
        Passed(std::move(config))));
}

void TJobTracker::ProfileHeartbeatRequest(const NProto::TReqHeartbeat* request)
{
    i64 totalJobStatisticsSize = 0;
    i64 totalJobDataStatisticsSize = 0;
    i64 totalJobResultSize = 0;
    for (auto& job : request->jobs()) {
        if (job.has_statistics()) {
            totalJobStatisticsSize += std::size(job.statistics());
        }
        if (job.has_result()) {
            totalJobResultSize += job.result().ByteSizeLong();
        }
        for (const auto& dataStatistics : job.output_data_statistics()) {
            totalJobDataStatisticsSize += dataStatistics.ByteSizeLong();
        }
        totalJobStatisticsSize += job.total_input_data_statistics().ByteSizeLong();
    }

    HeartbeatProtoMessageBytes_.Increment(request->ByteSizeLong());
    HeartbeatStatisticsBytes_.Increment(totalJobStatisticsSize);
    HeartbeatDataStatisticsBytes_.Increment(totalJobDataStatisticsSize);
    HeartbeatJobResultBytes_.Increment(totalJobResultSize);
    HeartbeatCount_.Increment();
}

IInvokerPtr TJobTracker::GetInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobTrackerQueue_->GetInvoker();
}

IInvokerPtr TJobTracker::TryGetCancelableInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableInvoker_.Acquire();
}

IInvokerPtr TJobTracker::GetCancelableInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto invoker = TryGetCancelableInvoker();
    YT_VERIFY(invoker);

    return invoker;
}

IInvokerPtr TJobTracker::GetCancelableInvokerOrThrow() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto invoker = TryGetCancelableInvoker();
    THROW_ERROR_EXCEPTION_IF(!invoker, EErrorCode::AgentDisconnected, "Job tracker disconnected");

    return invoker;
}

void TJobTracker::DoUpdateConfig(TJobTrackerConfigPtr config)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    Config_ = std::move(config);
}

void TJobTracker::DoUpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    std::swap(newExecNodes, ExecNodes_);

    NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
        BIND([oldExecNodesToDestroy{std::move(newExecNodes)}] {}));
}

void TJobTracker::AccountEnqueuedControllerEvent(int delta)
{
    auto newValue = EnqueuedControllerEventCount_.fetch_add(delta) + delta;
    HeartbeatEnqueuedControllerEvents_.Update(newValue);
}

void TJobTracker::DoRegisterOperation(
    TOperationId operationId,
    bool controlJobLifetimeAtControllerAgent)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_DEBUG(
        "Registering operation (OperationId: %v, ControlJobLifetimeAtControllerAgent: %v)",
        operationId,
        controlJobLifetimeAtControllerAgent);


    EmplaceOrCrash(
        RegisteredOperations_,
        operationId,
        TOperationInfo{
            .ControlJobLifetimeAtControllerAgent = controlJobLifetimeAtControllerAgent,
            .JobsReady = false,
        });
}

void TJobTracker::DoUnregisterOperation(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_DEBUG(
        "Unregistering operation (OperationId: %v)",
        operationId);

    auto operationIt = GetIteratorOrCrash(RegisteredOperations_, operationId);
    YT_VERIFY(operationIt != std::end(RegisteredOperations_));

    YT_LOG_FATAL_IF(
        !std::empty(operationIt->second.TrackedJobs),
        "Operation has registered jobs at the moment of unregistration (Jobs: %v)",
        operationIt->second.TrackedJobs);

    RegisteredOperations_.erase(operationIt);
}

void TJobTracker::DoRegisterJob(TStartedJobInfo jobInfo, TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromJobId(jobInfo.JobId);
    auto& [nodeJobs, _, nodeAddress] = GetOrRegisterNode(nodeId, jobInfo.NodeAddress);

    YT_LOG_DEBUG(
        "Register job (JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
        jobInfo.JobId,
        operationId,
        nodeId,
        nodeAddress);

    EmplaceOrCrash(nodeJobs.Jobs, jobInfo.JobId, TJobInfo{EJobStage::Running, operationId});

    auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);
    EmplaceOrCrash(operationInfo.TrackedJobs, jobInfo.JobId);
}

void TJobTracker::DoReviveJobs(
    TOperationId operationId,
    std::vector<TStartedJobInfo> jobs)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto& [controlJobLifetimeAtControllerAgent, jobsReady, trackedOperationJobs] = GetOrCrash(RegisteredOperations_, operationId);
    jobsReady = true;

    if (!controlJobLifetimeAtControllerAgent) {
        YT_LOG_DEBUG(
            "Skip revived jobs registration, since job lifetime controlled by scheduler (OperationId: %v)",
            operationId);

        return;
    }

    {
        int loggingJobSampleMaxSize = Config_->LoggingJobSampleSize;

        auto jobIdSample = CreateJobIdSampleForLogging(jobs, loggingJobSampleMaxSize);

        YT_LOG_DEBUG(
            "Revive jobs (OperationId: %v, JobCount: %v, JobSample: %v, JobSampleMaxSize: %v)",
            operationId,
            std::size(jobs),
            jobIdSample,
            loggingJobSampleMaxSize);
    }

    std::vector<TJobId> jobIds;
    jobIds.reserve(std::size(jobs));
    THashSet<TJobId> trackedJobs;
    trackedJobs.reserve(std::size(jobs));

    for (auto& jobInfo : jobs) {
        auto nodeId = NodeIdFromJobId(jobInfo.JobId);
        auto& nodeJobs = GetOrRegisterNode(nodeId, jobInfo.NodeAddress).Jobs;
        EmplaceOrCrash(nodeJobs.JobsToConfirm, jobInfo.JobId, operationId);

        jobIds.push_back(jobInfo.JobId);
        trackedJobs.emplace(jobInfo.JobId);
    }

    YT_LOG_FATAL_IF(
        !std::empty(trackedOperationJobs),
        "Revive jobs of operation that already has jobs (RegisteredJobs: %v, NewJobs: %v)",
        trackedOperationJobs,
        jobIds);

    trackedOperationJobs = std::move(trackedJobs);

    TDelayedExecutor::Submit(
        BIND(
            &TJobTracker::AbortUnconfirmedJobs,
            MakeWeak(this),
            operationId,
            Passed(std::move(jobIds))),
        Config_->JobConfirmationTimeout,
        GetCancelableInvoker());
}

void TJobTracker::DoReleaseJobs(
    TOperationId operationId,
    const std::vector<NJobTrackerClient::TJobToRelease>& jobs)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    {
        int loggingJobSampleMaxSize = Config_->LoggingJobSampleSize;

        std::vector<TJobId> jobSample = CreateJobIdSampleForLogging(jobs, loggingJobSampleMaxSize);

        YT_LOG_DEBUG(
            "Add jobs to release (OperationId: %v, JobCount: %v, JobSample: %v, JobSampleMaxSize: %v)",
            operationId,
            std::size(jobs),
            jobSample,
            loggingJobSampleMaxSize);
    }

    THashMap<TNodeId, std::vector<NJobTrackerClient::TJobToRelease>> jobsByNodes;
    jobsByNodes.reserve(std::size(jobs));

    for (const auto& job : jobs) {
        auto nodeId = NodeIdFromJobId(job.JobId);
        jobsByNodes[nodeId].push_back(job);
    }

    auto& operationJobs = GetOrCrash(RegisteredOperations_, operationId).TrackedJobs;

    for (const auto& [nodeId, jobs] : jobsByNodes) {
        auto* nodeInfo = FindNodeInfo(nodeId);
        if (!nodeInfo) {
            YT_LOG_DEBUG(
                "Skip jobs to release for node that is not connected (NodeId: %v, NodeAddress: %v, ReleasedJobCount: %v)",
                nodeId,
                GetNodeAddressForLogging(nodeId),
                std::size(jobs));
            continue;
        }

        auto& nodeJobs = nodeInfo->Jobs;

        for (const auto& jobToRelease : jobs) {
            if (auto jobIt = nodeJobs.Jobs.find(jobToRelease.JobId);
                jobIt != std::end(nodeJobs.Jobs))
            {
                YT_VERIFY(jobIt->second.OperationId == operationId);

                EraseOrCrash(operationJobs, jobToRelease.JobId);
                nodeJobs.Jobs.erase(jobIt);
            }

            EmplaceOrCrash(nodeJobs.JobsToRelease, jobToRelease.JobId, jobToRelease.ReleaseFlags);
        }
    }
}

void TJobTracker::DoAbortJobOnNode(TJobId jobId, TOperationId operationId, EAbortReason abortReason)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromJobId(jobId);

    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo) {
        YT_LOG_DEBUG(
            "Node is not registered, skip job abort request (JobId: %v, NodeId: %v, NodeAddress: %v, AbortReason: %v)",
            jobId,
            nodeId,
            GetNodeAddressForLogging(nodeId),
            abortReason);
        return;
    }

    auto& nodeJobs = nodeInfo->Jobs;
    const auto& nodeAddress = nodeInfo->NodeAddress;

    YT_LOG_DEBUG(
        "Abort job (JobId: %v, AbortReason: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
        jobId,
        abortReason,
        operationId,
        nodeId,
        nodeAddress);

    auto removeJobFromOperation = [operationId, jobId, this] {
        auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);
        operationInfo.TrackedJobs.erase(jobId);
    };

    if (auto jobIt = nodeJobs.Jobs.find(jobId); jobIt != std::end(nodeJobs.Jobs)) {
        nodeJobs.Jobs.erase(jobIt);
        removeJobFromOperation();
    } else if (auto jobToConfirmIt = nodeJobs.JobsToConfirm.find(jobId);
        jobToConfirmIt != std::end(nodeJobs.JobsToConfirm))
    {
        nodeJobs.JobsToConfirm.erase(jobToConfirmIt);
        removeJobFromOperation();
    }

    // NB(pogorelov): AbortJobOnNode may be called twice on operation finishing.
    nodeJobs.JobsToAbort.emplace(jobId, abortReason);
}

TJobTracker::TNodeInfo& TJobTracker::GetOrRegisterNode(TNodeId nodeId, const TString& nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt != std::end(RegisteredNodes_)) {
        return nodeIt->second;
    }

    return RegisterNode(nodeId, nodeAddress);
}

TJobTracker::TNodeInfo& TJobTracker::RegisterNode(TNodeId nodeId, TString nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_DEBUG(
        "Register node (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);

    auto lease = TLeaseManager::CreateLease(
        Config_->NodeDisconnectionTimeout,
        BIND_NO_PROPAGATE(&TJobTracker::OnNodeHeartbeatLeaseExpired, MakeWeak(this), nodeId)
            .Via(GetCancelableInvoker()));

    auto emplaceIt = EmplaceOrCrash(
        RegisteredNodes_,
        nodeId,
        TNodeInfo{{}, std::move(lease), std::move(nodeAddress)});
    return emplaceIt->second;
}

void TJobTracker::UpdateOrRegisterNode(TNodeId nodeId, const TString& nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt == std::end(RegisteredNodes_)) {
        RegisterNode(nodeId, nodeAddress);
    } else {
        auto& savedAddress = nodeIt->second.NodeAddress;
        if (savedAddress != nodeAddress) {
            YT_LOG_WARNING(
                "Node address has changed (OldAddress: %v, NewAddress: %v)",
                savedAddress,
                nodeAddress);

            savedAddress = nodeAddress;
        }
        TLeaseManager::RenewLease(nodeIt->second.Lease, Config_->NodeDisconnectionTimeout);
    }
}

TJobTracker::TNodeInfo* TJobTracker::FindNodeInfo(TNodeId nodeId)
{
    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt != std::end(RegisteredNodes_)) {
        return &nodeIt->second;
    }

    return nullptr;
}

void TJobTracker::OnNodeHeartbeatLeaseExpired(TNodeId nodeId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    TForbidContextSwitchGuard guard;

    auto& [nodeJobs, _, nodeAddress] = GetOrCrash(RegisteredNodes_, nodeId);

    YT_LOG_DEBUG(
        "Node heartbeat lease expired, remove jobs (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);

    {
        TOperationIdToJobIds jobsToAbort;
        for (const auto& [jobId, jobInfo] : nodeJobs.Jobs) {
            if (jobInfo.Stage == EJobStage::Running &&
                GetOrCrash(RegisteredOperations_, jobInfo.OperationId).ControlJobLifetimeAtControllerAgent)
            {
                YT_LOG_DEBUG(
                    "Abort job since node heartbeat lease expired (NodeId: %v, JobId: %v, OperationId: %v)",
                    nodeId,
                    jobId,
                    jobInfo.OperationId);
                jobsToAbort[jobInfo.OperationId].push_back(jobId);
            }
        }

        for (auto [jobId, operationId] : nodeJobs.JobsToConfirm) {
            YT_LOG_DEBUG(
                "Abort unconfirmed job since node heartbeat lease expired (NodeId: %v, JobId: %v, OperationId: %v)",
                nodeId,
                jobId,
                operationId);

            jobsToAbort[operationId].push_back(jobId);
        }

        for (const auto& [operationId, jobs] : jobsToAbort) {
            auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

            for (auto jobId : jobs) {
                EraseOrCrash(operationInfo.TrackedJobs, jobId);
            }
        }

        AbortJobs(std::move(jobsToAbort), EAbortReason::NodeOffline);
    }

    EraseOrCrash(RegisteredNodes_, nodeId);
}

const TString& TJobTracker::GetNodeAddressForLogging(TNodeId nodeId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = ExecNodes_->find(nodeId); nodeIt == std::end(*ExecNodes_)) {
        static const TString NotReceivedAddress{"<address not received>"};

        return NotReceivedAddress;
    } else {
        return nodeIt->second.Address;
    }
}

void TJobTracker::AbortJobs(TOperationIdToJobIds jobsByOperation, EAbortReason abortReason) const
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (std::empty(jobsByOperation)) {
        return;
    }

    const auto& controllerAgent = Bootstrap_->GetControllerAgent();

    Bootstrap_->GetControlInvoker()->Invoke(BIND(
        [jobsByOperation{std::move(jobsByOperation)}, abortReason, controllerAgent{controllerAgent.Get()}] {
            for (const auto& [operationId, jobs] : jobsByOperation) {
                auto operation = controllerAgent->FindOperation(operationId);

                // Operations unregister from job tracker asynchronously,
                // so it may already be unregistered from ControllerAgent but not from JobTracker.
                if (!operation) {
                    YT_LOG_DEBUG(
                        "Skip jobs abortion since operation does not exist (OperationId: %v)",
                        operationId);
                    continue;
                }

                auto controller = operation->GetController();
                controller->GetCancelableInvoker(controllerAgent->GetConfig()->JobEventsControllerQueue)->Invoke(
                    BIND([controller, jobs{std::move(jobs)}, abortReason] () mutable {
                        for (auto jobId : jobs) {
                            try {
                                controller->AbortJobByJobTracker(jobId, abortReason);
                            } catch (const std::exception& ex) {
                                YT_LOG_FATAL(
                                    ex,
                                    "Failed to abort job by job tracker request (JobId: %v)",
                                    jobId);
                            }
                        }
                    }));
            }
    }));
}

TJobTracker::TJobEventsProcessingContext TJobTracker::GetJobEventsProcessingContext(
    TIncarnationId incarnationId,
    std::vector<TOperationId> operationIds)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& controllerAgent = Bootstrap_->GetControllerAgent();

    auto getOperationIds = [
            operationIds{std::move(operationIds)},
            &controllerAgent,
            incarnationId
        ] {
            if (incarnationId != controllerAgent->GetIncarnationId()) {
                THROW_ERROR_EXCEPTION(EErrorCode::IncarnationMismatch, "Controller agent incarnation mismatch");
            }

            controllerAgent->ValidateConnected();

            TJobEventsProcessingContext result;
            result.OperationControllers.reserve(std::size(operationIds));
            for (auto operationId : operationIds) {
                if (const auto& operation = controllerAgent->FindOperation(operationId)) {
                    auto controller = operation->GetController();
                    YT_VERIFY(controller);
                    result.OperationControllers[operationId] = std::move(controller);
                }
            }

            result.JobEventsControllerQueue = controllerAgent->GetConfig()->JobEventsControllerQueue;

            return result;
        };

    return WaitFor(
        BIND(getOperationIds)
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run())
        .ValueOrThrow();
}

void TJobTracker::AbortUnconfirmedJobs(TOperationId operationId, std::vector<TJobId> jobs)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto operationIt = RegisteredOperations_.find(operationId);
    if (operationIt == std::end(RegisteredOperations_)) {
        YT_LOG_DEBUG(
            "Operation is already finished, skip unconfirmed jobs abortion (OperationId: %v)",
            operationId);

        return;
    }

    std::vector<TJobId> jobsToAbort;

    for (auto jobId : jobs) {
        auto nodeId = NodeIdFromJobId(jobId);

        auto nodeInfo = FindNodeInfo(nodeId);
        if (!nodeInfo) {
            YT_LOG_DEBUG(
                "Node is already disconnected, skip unconfirmed jobs abortion (JobId: %v, NodeId: %v, NodeAddress: %v)",
                jobId,
                nodeId,
                GetNodeAddressForLogging(nodeId));

            continue;
        }

        auto& nodeJobs = nodeInfo->Jobs;
        const auto& nodeAddress = nodeInfo->NodeAddress;

        if (auto unconfirmedJobIt = nodeJobs.JobsToConfirm.find(jobId);
            unconfirmedJobIt != std::end(nodeJobs.JobsToConfirm))
        {
            YT_LOG_DEBUG(
                "Job was not confirmed within timeout, abort it (JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
                jobId,
                unconfirmedJobIt->second,
                nodeId,
               nodeAddress);
            nodeJobs.JobsToConfirm.erase(unconfirmedJobIt);

            jobsToAbort.push_back(jobId);
        }
    }

    for (auto jobId : jobsToAbort) {
        auto& operationInfo = operationIt->second;

        EraseOrCrash(operationInfo.TrackedJobs, jobId);
    }

    TOperationIdToJobIds operationJobsToAbort;
    operationJobsToAbort[operationId] = std::move(jobsToAbort);

    AbortJobs(/*jobsByOperation*/ std::move(operationJobsToAbort), EAbortReason::RevivalConfirmationTimeout);
}

void TJobTracker::DoInitialize(IInvokerPtr cancelableInvoker)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    YT_LOG_DEBUG("Initialize state");

    YT_VERIFY(!CancelableInvoker_.Exchange(cancelableInvoker));
}

void TJobTracker::SetIncarnationId(TIncarnationId incarnationId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_VERIFY(!IncarnationId_);

    YT_LOG_DEBUG("Set new incarnation (IncarnationId: %v)", incarnationId);

    IncarnationId_ = incarnationId;
}

void TJobTracker::DoCleanup()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    YT_LOG_DEBUG("Cleanup state");

    if (auto invoker = CancelableInvoker_.Exchange(IInvokerPtr{}); !invoker) {
        YT_LOG_DEBUG("Job tracker is not initialized, skip cleanup");
        return;
    }

    IncarnationId_ = {};

    // No need to cancel leases.
    RegisteredNodes_.clear();

    RegisteredOperations_.clear();
}

////////////////////////////////////////////////////////////////////

TJobTrackerOperationHandler::TJobTrackerOperationHandler(
    TJobTracker* jobTracker,
    IInvokerPtr cancelableInvoker,
    TOperationId operationId,
    bool controlJobLifetimeAtControllerAgent)
    : JobTracker_(jobTracker)
    , CancelableInvoker_(std::move(cancelableInvoker))
    , OperationId_(operationId)
    , ControlJobLifetimeAtControllerAgent_(controlJobLifetimeAtControllerAgent)
{ }

void TJobTrackerOperationHandler::RegisterJob(TStartedJobInfo jobInfo)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!ControlJobLifetimeAtControllerAgent_) {
        return;
    }

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoRegisterJob,
        MakeStrong(JobTracker_),
        std::move(jobInfo),
        OperationId_));
}

void TJobTrackerOperationHandler::ReviveJobs(std::vector<TStartedJobInfo> jobs)
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoReviveJobs,
        MakeStrong(JobTracker_),
        OperationId_,
        std::move(jobs)));
}

void TJobTrackerOperationHandler::ReleaseJobs(std::vector<NJobTrackerClient::TJobToRelease> jobs)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!ControlJobLifetimeAtControllerAgent_) {
        return;
    }

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoReleaseJobs,
        MakeStrong(JobTracker_),
        OperationId_,
        std::move(jobs)));
}

void TJobTrackerOperationHandler::AbortJobOnNode(
    TJobId jobId,
    NScheduler::EAbortReason abortReason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!ControlJobLifetimeAtControllerAgent_) {
        return;
    }

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoAbortJobOnNode,
        MakeStrong(JobTracker_),
        jobId,
        OperationId_,
        abortReason));
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
