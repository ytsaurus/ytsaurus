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

#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NControllerAgent {

static const NLogging::TLogger Logger("JobTracker");

////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NScheduler;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////

namespace {

EJobStage JobStateToJobStage(EJobState jobState) noexcept
{
    switch (jobState) {
        case EJobState::Running:
        case EJobState::Waiting:
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

class TJobTracker::TJobTrackerNodeOrchidService
    : public TVirtualMapBase
{
public:
    explicit TJobTrackerNodeOrchidService(const TJobTracker* jobTracker)
        : TVirtualMapBase(/*owningNode*/ nullptr)
        , JobTracker_(jobTracker)
    { }

    i64 GetSize() const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        return std::ssize(JobTracker_->RegisteredNodes_);
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        std::vector<TString> keys;
        keys.reserve(std::min(limit, GetSize()));
        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            if (static_cast<i64>(keys.size()) >= limit) {
                break;
            }

            keys.push_back(nodeInfo.NodeAddress);
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto nodeIdIt = JobTracker_->NodeAddressToNodeId_.find(key);
        if (nodeIdIt == std::end(JobTracker_->NodeAddressToNodeId_)) {
            return nullptr;
        }

        auto nodeId = nodeIdIt->second;

        YT_VERIFY(JobTracker_->RegisteredNodes_.contains(nodeId));

        auto producer = TYsonProducer(BIND([
            jobTracker = JobTracker_,
            nodeId
        ] (IYsonConsumer* consumer) {
            auto nodeIt = jobTracker->RegisteredNodes_.find(nodeId);

            if (nodeIt == std::end(jobTracker->RegisteredNodes_)) {
                return;
            }

            const auto& nodeJobs = nodeIt->second.Jobs;

            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("jobs").DoMapFor(nodeJobs.Jobs, [] (TFluentMap fluent, const auto& pair) {
                        const auto& [jobId, jobInfo] = pair;

                        fluent
                            .Item(ToString(jobId)).BeginMap()
                                .Item("operation_id").Value(jobInfo.OperationId)
                                .Item("stage").Value(jobInfo.Stage)
                            .EndMap();
                    })
                    .Item("jobs_waiting_for_confirmation").DoMapFor(nodeJobs.JobsToConfirm, [] (TFluentMap fluent, const auto& pair) {
                        const auto& [jobId, operationId] = pair;

                        fluent
                            .Item(ToString(jobId)).BeginMap()
                                .Item("operation_id").Value(operationId)
                            .EndMap();
                    })
                    .Item("jobs_to_release").DoMapFor(nodeJobs.JobsToRelease, [] (TFluentMap fluent, const auto& pair) {
                        const auto& [jobId, releaseFlags] = pair;

                        fluent
                            .Item(ToString(jobId)).BeginMap()
                                .Item("release_flags").Value(ToString(releaseFlags))
                            .EndMap();
                    })
                    .Item("jobs_to_abort").DoMapFor(nodeJobs.JobsToAbort, [] (TFluentMap fluent, const auto& pair) {
                        const auto& [jobId, abortReason] = pair;

                        fluent
                            .Item(ToString(jobId)).BeginMap()
                                .Item("abort_reason").Value(abortReason)
                            .EndMap();
                    })
                .EndMap();
        }));

        return IYPathService::YPathDesignatedServiceFromProducer(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

class TJobTracker::TJobTrackerJobOrchidService
    : public TVirtualMapBase
{
public:
    explicit TJobTrackerJobOrchidService(const TJobTracker* jobTracker)
        : TVirtualMapBase(/*owningNode*/ nullptr)
        , JobTracker_(jobTracker)
    { }

    i64 GetSize() const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        i64 size = 0;

        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            const auto& nodeJobs = nodeInfo.Jobs;
            size += std::ssize(nodeJobs.Jobs) +
                std::ssize(nodeJobs.JobsToConfirm) +
                std::ssize(nodeJobs.JobsToRelease) +
                std::ssize(nodeJobs.JobsToAbort);
        }
        return size;
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        std::vector<TString> keys;
        keys.reserve(std::min(limit, GetSize()));
        for (const auto& [nodeId, nodeInfo] : JobTracker_->RegisteredNodes_) {
            const auto& nodeJobs = nodeInfo.Jobs;

            for (const auto& [jobId, _] : nodeJobs.Jobs) {
                keys.push_back(ToString(jobId));
                if (static_cast<i64>(keys.size()) >= limit) {
                    return keys;
                }
            }

            for (const auto& [jobId, _] : nodeJobs.JobsToConfirm) {
                keys.push_back(ToString(jobId));
                if (static_cast<i64>(keys.size()) >= limit) {
                    return keys;
                }
            }

            for (const auto& [jobId, _] : nodeJobs.JobsToAbort) {
                keys.push_back(ToString(jobId));
                if (static_cast<i64>(keys.size()) >= limit) {
                    return keys;
                }
            }

            for (const auto& [jobId, _] : nodeJobs.JobsToRelease) {
                keys.push_back(ToString(jobId));
                if (static_cast<i64>(keys.size()) >= limit) {
                    return keys;
                }
            }
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto jobId = TJobId::FromString(key);

        auto nodeId = NodeIdFromJobId(jobId);

        auto nodeInfoIt = JobTracker_->RegisteredNodes_.find(nodeId);
        if (nodeInfoIt == std::end(JobTracker_->RegisteredNodes_)) {
            return nullptr;
        }

        TYsonString jobYson;

        const auto& [nodeJobs, _, nodeAddress] = nodeInfoIt->second;

        if (auto jobIt = nodeJobs.Jobs.find(jobId);
            jobIt != std::end(nodeJobs.Jobs))
        {
            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value(jobIt->second.Stage)
                    .Item("operation_id").Value(jobIt->second.OperationId)
                    .Item("node_address").Value(nodeAddress)
                .EndMap();
        } else if (auto jobToConfirmIt = nodeJobs.JobsToConfirm.find(jobId);
            jobToConfirmIt != std::end(nodeJobs.JobsToConfirm))
        {
            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value("confirmation")
                    .Item("operation_id").Value(jobToConfirmIt->second)
                    .Item("node_address").Value(nodeAddress)
                .EndMap();
        } else if (auto jobToAbortIt = nodeJobs.JobsToAbort.find(jobId);
            jobToAbortIt != std::end(nodeJobs.JobsToAbort))
        {
            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value("aborting")
                    .Item("abort_reason").Value(jobToAbortIt->second)
                    .Item("node_address").Value(nodeAddress)
                .EndMap();
        } else if (auto jobToReleaseIt = nodeJobs.JobsToRelease.find(jobId);
            jobToReleaseIt != std::end(nodeJobs.JobsToRelease))
        {
            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value("releasing")
                    .Item("release_flags").Value(ToString(jobToReleaseIt->second))
                    .Item("node_address").Value(nodeAddress)
                .EndMap();
        }

        if (!jobYson) {
            return nullptr;
        }

        auto producer = TYsonProducer(BIND([yson = std::move(jobYson)] (IYsonConsumer* consumer) {
            consumer->OnRaw(yson);
        }));

        return IYPathService::FromProducer(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

class TJobTracker::TJobTrackerOperationOrchidService
    : public TVirtualMapBase
{
public:
    explicit TJobTrackerOperationOrchidService(const TJobTracker* jobTracker)
        : TVirtualMapBase(/*owningNode*/ nullptr)
        , JobTracker_(jobTracker)
    { }

    i64 GetSize() const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        return std::ssize(JobTracker_->RegisteredOperations_);
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        std::vector<TString> keys;
        keys.reserve(std::min(limit, GetSize()));
        for (const auto& [operationId, operationInfo] : JobTracker_->RegisteredOperations_) {
            if (static_cast<i64>(keys.size()) >= limit) {
                break;
            }

            keys.push_back(ToString(operationId));
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto operationId = TOperationId::FromString(key);
        auto operationInfoIt = JobTracker_->RegisteredOperations_.find(operationId);
        if (operationInfoIt == std::end(JobTracker_->RegisteredOperations_)) {
            return nullptr;
        }

        const auto& operationInfo = operationInfoIt->second;

        auto producer = TYsonProducer(BIND([
            controlJobLifetimeAtControllerAgent = operationInfo.ControlJobLifetimeAtControllerAgent,
            jobsReady = operationInfo.JobsReady,
            operationId,
            jobTracker = JobTracker_
        ] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("job_lifetime_controlled_by_job_tracker").Value(controlJobLifetimeAtControllerAgent)
                    .Item("jobs_ready").Value(jobsReady)
                    .Item("jobs").Do([&] (TFluentAny innerFluent) {
                        const auto& operationIt = jobTracker->RegisteredOperations_.find(operationId);

                        if (operationIt == std::end(jobTracker->RegisteredOperations_)) {
                            return;
                        }

                        innerFluent.DoListFor(operationIt->second.TrackedJobIds, [] (TFluentList fluent, TJobId jobId) {
                            fluent
                                .Item().Value(jobId);
                        });
                    })
                .EndMap();
        }));

        return IYPathService::YPathDesignatedServiceFromProducer(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

////////////////////////////////////////////////////////////////////

TJobTracker::TJobTracker(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(bootstrap->GetConfig()->ControllerAgent->JobTracker)
    , JobEventsControllerQueue_(bootstrap->GetConfig()->ControllerAgent->JobEventsControllerQueue)
    , HeartbeatStatisticsBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/statistics_bytes"))
    , HeartbeatDataStatisticsBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/data_statistics_bytes"))
    , HeartbeatJobResultBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/job_result_bytes"))
    , HeartbeatProtoMessageBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/proto_message_bytes"))
    , HeartbeatEnqueuedControllerEvents_(ControllerAgentProfiler.WithHot().GaugeSummary("/node_heartbeat/enqueued_controller_events"))
    , HeartbeatCount_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/count"))
    , JobTrackerQueue_(New<NConcurrency::TActionQueue>("JobTracker"))
    , ExecNodes_(New<TRefCountedExecNodeDescriptorMap>())
    , OrchidService_(CreateOrchidService())
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

    THROW_ERROR_EXCEPTION_IF(
        !incarnationId,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    auto Logger = NControllerAgent::Logger.WithTag(
        "NodeId: %v, NodeAddress: %v",
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

    THashSet<TAllocationId> allocationIdsRunningOnNode;
    allocationIdsRunningOnNode.reserve(std::size(request->allocations()));

    for (const auto& allocationInfoProto : request->allocations()) {
        allocationIdsRunningOnNode.insert(
            FromProto<TAllocationId>(allocationInfoProto.allocation_id()));
    }

    SwitchTo(GetCancelableInvokerOrThrow());

    if (incarnationId != IncarnationId_) {
        THROW_ERROR_EXCEPTION(
            NControllerAgent::EErrorCode::IncarnationMismatch,
            "Controller agent incarnation mismatch: expected %v, got %v)",
            IncarnationId_,
            incarnationId);
    }

    TForbidContextSwitchGuard guard;

    auto& nodeInfo = UpdateOrRegisterNode(nodeId, nodeDescriptor.GetDefaultAddress());

    auto& nodeJobs = nodeInfo.Jobs;

    // NB(pogorelov): As checked before, incarnation id did not change from the previous heartbeat,
    // so job life time of job operations must be still controlled by agent.
    {
        auto unconfirmedJobs = NYT::FromProto<std::vector<TJobId>>(request->unconfirmed_job_ids());
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

                auto& operationInfo = GetOrCrash(RegisteredOperations_, jobToConfirmIt->second);
                EraseOrCrash(operationInfo.TrackedJobIds, jobId);

                nodeJobs.JobsToConfirm.erase(jobToConfirmIt);
            }
        }

        AbortJobs(std::move(jobsToAbort), EAbortReason::Unconfirmed);
    }

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
            ToProto(response->add_jobs_to_remove(), TJobToRelease{jobId, releaseFlags});
        }

        for (auto jobId : releasedJobs) {
            EraseOrCrash(nodeJobs.JobsToRelease, jobId);
        }
    }

    const auto& heartbeatProcessingLogger = Logger;
    for (auto& [operationId, jobSummaries] : groupedJobSummaries) {
        auto Logger = heartbeatProcessingLogger.WithTag(
            "OperationId: %v",
            operationId);

        auto operationIt = RegisteredOperations_.find(operationId);
        if (operationIt == std::end(RegisteredOperations_)) {
            YT_LOG_DEBUG("Operation is not registered at job controller, skip handling job infos from node");

            ToProto(response->add_unknown_operation_ids(), operationId);

            continue;
        }

        const auto& operationInfo = operationIt->second;

        auto operationController = operationInfo.OperationController.Lock();
        if (!operationController) {
            YT_LOG_DEBUG("Operation controller is already reset, skip handling job infos from node");

            continue;
        }

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
                    YT_VERIFY(jobIt->second == operationId);
                    nodeJobs.JobsToConfirm.erase(jobIt);
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
                    ToProto(response->add_jobs_to_remove(), TJobToRelease{jobId});
                }
            }
        } else {
            jobSummariesToSendToOperationController = std::move(jobSummaries);
        }

        AccountEnqueuedControllerEvent(+1);
        // Raw pointer is OK since the job tracker never dies.
        auto discountGuard = Finally(std::bind(&TJobTracker::AccountEnqueuedControllerEvent, this, -1));
        operationController->GetCancelableInvoker(JobEventsControllerQueue_)->Invoke(
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

    if (Config_->AbortVanishedJobs) {
        TOperationIdToJobIds jobIdsToAbort;
        for (const auto& [jobId, jobInfo] : nodeJobs.Jobs) {
            if (jobInfo.Stage != EJobStage::Running) {
                continue;
            }

            if (!allocationIdsRunningOnNode.contains(AllocationIdFromJobId(jobId))) {
                YT_LOG_DEBUG(
                    "Job vanished, aborting it (JobId: %v, OperationId: %v)",
                    jobId,
                    jobInfo.OperationId);

                jobIdsToAbort[jobInfo.OperationId].push_back(jobId);
            }
        }

        for (const auto& [operationId, jobIds] : jobIdsToAbort) {
            auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

            for (auto jobId : jobIds) {
                EraseOrCrash(nodeJobs.Jobs, jobId);
                EraseOrCrash(operationInfo.TrackedJobIds, jobId);
            }
        }

        AbortJobs(jobIdsToAbort, EAbortReason::Vanished);
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
    bool controlJobLifetimeAtControllerAgent,
    TWeakPtr<IOperationController> operationController)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto cancelableInvoker = GetCancelableInvoker();

    cancelableInvoker->Invoke(BIND(
        &TJobTracker::DoRegisterOperation,
        MakeStrong(this),
        operationId,
        controlJobLifetimeAtControllerAgent,
        Passed(std::move(operationController))));

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

void TJobTracker::UpdateConfig(const TControllerAgentConfigPtr& config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    GetInvoker()->Invoke(BIND(
        &TJobTracker::DoUpdateConfig,
        MakeStrong(this),
        config));
}

NYTree::IYPathServicePtr TJobTracker::GetOrchidService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return OrchidService_;
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
    THROW_ERROR_EXCEPTION_IF(
        !invoker,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Job tracker disconnected");

    return invoker;
}

NYTree::IYPathServicePtr TJobTracker::CreateOrchidService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto service = New<TCompositeMapService>();

    service->AddChild("nodes", New<TJobTrackerNodeOrchidService>(this));

    service->AddChild("jobs", New<TJobTrackerJobOrchidService>(this));

    service->AddChild("operations", New<TJobTrackerOperationOrchidService>(this));

    return service->Via(GetInvoker());
}

void TJobTracker::DoUpdateConfig(const TControllerAgentConfigPtr& config)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    Config_ = config->JobTracker;
    JobEventsControllerQueue_ = config->JobEventsControllerQueue;
}

void TJobTracker::DoUpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    std::swap(newExecNodes, ExecNodes_);

    NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
        BIND([oldExecNodesToDestroy = std::move(newExecNodes)] {}));
}

void TJobTracker::AccountEnqueuedControllerEvent(int delta)
{
    auto newValue = EnqueuedControllerEventCount_.fetch_add(delta) + delta;
    HeartbeatEnqueuedControllerEvents_.Update(newValue);
}

void TJobTracker::DoRegisterOperation(
    TOperationId operationId,
    bool controlJobLifetimeAtControllerAgent,
    TWeakPtr<IOperationController> operationController)
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
            .OperationController = std::move(operationController)
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
        !std::empty(operationIt->second.TrackedJobIds),
        "Operation has registered jobs at the moment of unregistration (OperationId: %v, JobIds: %v)",
        operationId,
        operationIt->second.TrackedJobIds);

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
    EmplaceOrCrash(operationInfo.TrackedJobIds, jobInfo.JobId);
}

void TJobTracker::DoReviveJobs(
    TOperationId operationId,
    std::vector<TStartedJobInfo> jobs)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

    operationInfo.JobsReady = true;

    if (!operationInfo.ControlJobLifetimeAtControllerAgent) {
        YT_LOG_DEBUG(
            "Skip revived jobs registration, since job lifetime controlled by scheduler (OperationId: %v)",
            operationId);

        return;
    }

    {
        int loggingJobSampleMaxSize = Config_->LoggingJobSampleSize;

        auto jobIdSample = CreateJobIdSampleForLogging(jobs, loggingJobSampleMaxSize);

        YT_LOG_DEBUG(
            "Revive jobs (OperationId: %v, JobCount: %v, JobIdSample: %v, SampleMaxSize: %v)",
            operationId,
            std::size(jobs),
            jobIdSample,
            loggingJobSampleMaxSize);
    }

    std::vector<TJobId> jobIds;
    jobIds.reserve(std::size(jobs));
    THashSet<TJobId> trackedJobIds;
    trackedJobIds.reserve(std::size(jobs));

    for (auto& jobInfo : jobs) {
        auto nodeId = NodeIdFromJobId(jobInfo.JobId);
        auto& nodeJobs = GetOrRegisterNode(nodeId, jobInfo.NodeAddress).Jobs;
        EmplaceOrCrash(nodeJobs.JobsToConfirm, jobInfo.JobId, operationId);

        jobIds.push_back(jobInfo.JobId);
        trackedJobIds.emplace(jobInfo.JobId);
    }

    YT_LOG_FATAL_IF(
        !std::empty(operationInfo.TrackedJobIds),
        "Revive jobs of operation that already has jobs (OperationId: %v, RegisteredJobIds: %v, NewJobIds: %v)",
        operationId,
        operationInfo.TrackedJobIds,
        jobIds);

    operationInfo.TrackedJobIds = std::move(trackedJobIds);

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
    const std::vector<TJobToRelease>& jobs)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    {
        int loggingJobSampleMaxSize = Config_->LoggingJobSampleSize;

        std::vector<TJobId> jobIdSample = CreateJobIdSampleForLogging(jobs, loggingJobSampleMaxSize);

        YT_LOG_DEBUG(
            "Add jobs to release (OperationId: %v, JobCount: %v, JobIdSample: %v, SampleMaxSize: %v)",
            operationId,
            std::size(jobs),
            jobIdSample,
            loggingJobSampleMaxSize);
    }

    THashMap<TNodeId, std::vector<TJobToRelease>> nodeIdToJobsToRelease;
    nodeIdToJobsToRelease.reserve(std::size(jobs));

    for (const auto& job : jobs) {
        auto nodeId = NodeIdFromJobId(job.JobId);
        nodeIdToJobsToRelease[nodeId].push_back(job);
    }

    auto& operationJobs = GetOrCrash(RegisteredOperations_, operationId).TrackedJobIds;

    for (const auto& [nodeId, jobs] : nodeIdToJobsToRelease) {
        auto* nodeInfo = FindNodeInfo(nodeId);
        if (!nodeInfo) {
            YT_LOG_DEBUG(
                "Skip jobs to release for node that is not connected (OperationId: %v, NodeId: %v, NodeAddress: %v, ReleasedJobCount: %v)",
                operationId,
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
            "Node is not registered, skip job abort request (JobId: %v, NodeId: %v, NodeAddress: %v, AbortReason: %v, OperationId: %v)",
            jobId,
            nodeId,
            GetNodeAddressForLogging(nodeId),
            abortReason,
            operationId);
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
        operationInfo.TrackedJobIds.erase(jobId);
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

    if (auto nodeIdIt = NodeAddressToNodeId_.find(nodeAddress); nodeIdIt != std::end(NodeAddressToNodeId_)) {
        auto oldNodeId = nodeIdIt->second;
        YT_LOG_WARNING(
            "Node with the same address is already registered, unregister old node and register new (NodeAddress: %v, NewNodeId: %v, OldNodeId: %v)",
            nodeAddress,
            nodeId,
            nodeIdIt->second);

        UnregisterNode(oldNodeId, nodeAddress);
    }

    auto lease = TLeaseManager::CreateLease(
        Config_->NodeDisconnectionTimeout,
        BIND_NO_PROPAGATE(&TJobTracker::OnNodeHeartbeatLeaseExpired, MakeWeak(this), nodeId, nodeAddress)
            .Via(GetCancelableInvoker()));

    EmplaceOrCrash(NodeAddressToNodeId_, nodeAddress, nodeId);

    auto emplaceIt = EmplaceOrCrash(
        RegisteredNodes_,
        nodeId,
        TNodeInfo{{}, std::move(lease), std::move(nodeAddress)});
    return emplaceIt->second;
}

TJobTracker::TNodeInfo& TJobTracker::UpdateOrRegisterNode(TNodeId nodeId, const TString& nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt == std::end(RegisteredNodes_)) {
        return RegisterNode(nodeId, nodeAddress);
    } else {
        auto& nodeInfo = nodeIt->second;
        auto& savedAddress = nodeInfo.NodeAddress;
        if (savedAddress != nodeAddress) {
            YT_LOG_WARNING(
                "Node address has changed, unregister old node and register new (OldAddress: %v, NewAddress: %v)",
                savedAddress,
                nodeAddress);

            UnregisterNode(nodeId, savedAddress);
            return RegisterNode(nodeId, nodeAddress);
        }

        TLeaseManager::RenewLease(nodeInfo.Lease, Config_->NodeDisconnectionTimeout);

        return nodeIt->second;
    }
}

void TJobTracker::UnregisterNode(TNodeId nodeId, const TString& nodeAddress)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    TForbidContextSwitchGuard guard;

    auto nodeIt = RegisteredNodes_.find(nodeId);
    if (nodeIt == std::end(RegisteredNodes_)) {
        YT_LOG_DEBUG(
            "Node is already unregistered (NodeId: %v, NodeAddress: %v)",
            nodeId,
            nodeAddress);

        return;
    }

    const auto& nodeInfo = nodeIt->second;

    const auto& nodeJobs = nodeInfo.Jobs;
    YT_VERIFY(nodeAddress == nodeInfo.NodeAddress);

    YT_LOG_DEBUG(
        "Unregistering node (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);

    {
        TOperationIdToJobIds jobIdsToAbort;
        TOperationIdToJobIds finishedJobIds;

        for (const auto& [jobId, jobInfo] : nodeJobs.Jobs) {
            if (GetOrCrash(RegisteredOperations_, jobInfo.OperationId).ControlJobLifetimeAtControllerAgent) {
                if (jobInfo.Stage == EJobStage::Running) {
                    YT_LOG_DEBUG(
                        "Abort job since node unregistered (NodeId: %v, JobId: %v, OperationId: %v)",
                        nodeId,
                        jobId,
                        jobInfo.OperationId);
                    jobIdsToAbort[jobInfo.OperationId].push_back(jobId);
                } else {
                    YT_LOG_DEBUG(
                        "Remove finished job from operation info since node unregistered (NodeId: %v, JobId: %v, OperationId: %v)",
                        nodeId,
                        jobId,
                        jobInfo.OperationId);
                    finishedJobIds[jobInfo.OperationId].push_back(jobId);
                }
            }
        }

        for (auto [jobId, operationId] : nodeJobs.JobsToConfirm) {
            YT_LOG_DEBUG(
                "Abort unconfirmed job since node unregistered (NodeId: %v, JobId: %v, OperationId: %v)",
                nodeId,
                jobId,
                operationId);

            jobIdsToAbort[operationId].push_back(jobId);
        }

        for (const auto& [operationId, jobIds] : jobIdsToAbort) {
            auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

            for (auto jobId : jobIds) {
                EraseOrCrash(operationInfo.TrackedJobIds, jobId);
            }
        }
        for (const auto& [operationId, jobIds] : finishedJobIds) {
            auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

            for (auto jobId : jobIds) {
                EraseOrCrash(operationInfo.TrackedJobIds, jobId);
            }
        }

        AbortJobs(std::move(jobIdsToAbort), EAbortReason::NodeOffline);
    }

    EraseOrCrash(NodeAddressToNodeId_, nodeAddress);

    RegisteredNodes_.erase(nodeIt);
}

TJobTracker::TNodeInfo* TJobTracker::FindNodeInfo(TNodeId nodeId)
{
    if (auto nodeIt = RegisteredNodes_.find(nodeId); nodeIt != std::end(RegisteredNodes_)) {
        return &nodeIt->second;
    }

    return nullptr;
}

void TJobTracker::OnNodeHeartbeatLeaseExpired(TNodeId nodeId, const TString& nodeAddress)
{
    YT_LOG_DEBUG(
        "Node heartbeat lease expired, unregister node (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);

    UnregisterNode(nodeId, nodeAddress);
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

void TJobTracker::AbortJobs(TOperationIdToJobIds operationIdToJobIds, EAbortReason abortReason) const
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (std::empty(operationIdToJobIds)) {
        return;
    }

    for (const auto& [operationId, jobIds] : operationIdToJobIds) {
        auto operationIt = RegisteredOperations_.find(operationId);
        if (operationIt == std::end(RegisteredOperations_)) {
            YT_LOG_DEBUG(
                "Operation is not registered, skip jobs abortion (OperationId: %v)",
                operationId);

            continue;
        }

        const auto& operationInfo = operationIt->second;

        YT_VERIFY(operationInfo.ControlJobLifetimeAtControllerAgent);

        auto operationController = operationInfo.OperationController.Lock();
        if (!operationController) {
            YT_LOG_DEBUG(
                "Operation controller is already destroyed, skip jobs abortion (OperationId: %v)",
                operationId);
            continue;
        }

        operationController->GetCancelableInvoker(JobEventsControllerQueue_)->Invoke(
            BIND([operationController, jobs = std::move(jobIds), abortReason] () mutable {
                for (auto jobId : jobs) {
                    try {
                        operationController->AbortJobByJobTracker(jobId, abortReason);
                    } catch (const std::exception& ex) {
                        YT_LOG_FATAL(
                            ex,
                            "Failed to abort job by job tracker request (JobId: %v)",
                            jobId);
                    }
                }
            }));
    }
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

        auto* nodeInfo = FindNodeInfo(nodeId);
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

        EraseOrCrash(operationInfo.TrackedJobIds, jobId);
    }

    TOperationIdToJobIds operationJobIdsToAbort;
    operationJobIdsToAbort[operationId] = std::move(jobsToAbort);

    AbortJobs(
        /*operationIdToJobIds*/ std::move(operationJobIdsToAbort),
        EAbortReason::RevivalConfirmationTimeout);
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

    NodeAddressToNodeId_.clear();

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

void TJobTrackerOperationHandler::ReleaseJobs(std::vector<TJobToRelease> jobs)
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
