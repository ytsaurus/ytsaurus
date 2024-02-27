#include "job_tracker.h"

#include "bootstrap.h"
#include "config.h"
#include "controller_agent.h"
#include "operation.h"
#include "private.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/job_report.h>

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NControllerAgent {

static const NLogging::TLogger Logger("JobTracker");
static const auto JobTrackerProfiler = ControllerAgentProfiler.WithPrefix("/job_tracker");
static const auto NodeHeartbeatProfiler = JobTrackerProfiler.WithPrefix("/node_heartbeat");

////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NScheduler;
using namespace NNodeTrackerClient;
using namespace NTracing;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////

namespace {

EJobStage JobStageFromJobState(EJobState jobState) noexcept
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
            if (std::ssize(keys) >= limit) {
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

            auto jobYsonBuilder = [] <class TJobInfo> (TFluentMap fluent, TJobId jobId, TJobInfo jobInfo) {
                fluent
                    .Item(ToString(jobId)).BeginMap()
                        .Item("operation_id").Value(jobInfo.OperationId)
                        .Item("stage").Value(
                            std::holds_alternative<TRunningJobInfo>(jobInfo.Info)
                                ? EJobStage::Running
                                : EJobStage::Finished)
                    .EndMap();
            };

            auto jobToConfirmYsonBuilder = [] (TFluentMap fluent, TJobId jobId, TJobToConfirmInfo jobToConfirmInfo) {
                fluent
                    .Item(ToString(jobId)).BeginMap()
                        .Item("operation_id").Value(jobToConfirmInfo.OperationId)
                    .EndMap();
            };

            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("jobs").DoMap([&] (TFluentMap fluent) {
                        nodeJobs.ForEachJob(
                            std::bind(
                                jobYsonBuilder,
                                fluent,
                                std::placeholders::_1,
                                std::placeholders::_2));
                    })
                    .Item("jobs_waiting_for_confirmation").DoMap([&] (TFluentMap fluent) {
                        nodeJobs.ForEachJobToConfirm(
                            std::bind(
                                jobToConfirmYsonBuilder,
                                fluent,
                                std::placeholders::_1,
                                std::placeholders::_2));
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

        return IYPathService::FromProducerLazy(std::move(producer));
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
            size += nodeJobs.GetJobCount() +
                nodeJobs.GetJobToConfirmCount() +
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

            nodeJobs.ForEachJob([&] <class TJobInfo> (TJobId jobId, TJobInfo) {
                if (std::ssize(keys) < limit) {
                    keys.push_back(ToString(jobId));
                }
            });

            if (std::ssize(keys) >= limit) {
                return keys;
            }

            nodeJobs.ForEachJobToConfirm([&] (TJobId jobId, TJobToConfirmInfo) {
                if (std::ssize(keys) < limit) {
                    keys.push_back(ToString(jobId));
                }
            });

            if (std::ssize(keys) >= limit) {
                return keys;
            }

            for (const auto& [jobId, _] : nodeJobs.JobsToAbort) {
                keys.push_back(ToString(jobId));
                if (std::ssize(keys) >= limit) {
                    return keys;
                }
            }

            for (const auto& [jobId, _] : nodeJobs.JobsToRelease) {
                keys.push_back(ToString(jobId));
                if (std::ssize(keys) >= limit) {
                    return keys;
                }
            }
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto jobId = TJobId(TGuid::FromString(key));

        auto nodeId = NodeIdFromJobId(jobId);

        auto nodeInfoIt = JobTracker_->RegisteredNodes_.find(nodeId);
        if (nodeInfoIt == std::end(JobTracker_->RegisteredNodes_)) {
            return nullptr;
        }

        TYsonString jobYson;

        const auto& [
            nodeJobs,
            _,
            registrationId,
            nodeAddress
        ] = nodeInfoIt->second;

        //! NB(arkady-e1ppa): const_cast's below are used instead of proper
        //! const& overloads because they would require us to have a separate type
        //! of const iterators or removal of "const&" from JobTracker.
        //! I've decided that we should safely use const_cast over
        //! adding more complexity to our iterators.
        if (auto jobIt = const_cast<TNodeJobs&>(nodeJobs).FindJob(jobId)) {
            auto operationId = jobIt->Allocation->second.OperationId;

            auto traceContextGuard = CreateOperationTraceContextGuard(
                "JobTrackerJobOrchidService",
                operationId);

            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value(
                        TNodeJobs::IsRunning(*jobIt)
                            ? EJobStage::Running
                            : EJobStage::Finished)
                    .Item("operation_id").Value(operationId)
                    .Item("node_address").Value(nodeAddress)
                .EndMap();
        } else if (auto jobToConfirmIt = const_cast<TNodeJobs&>(nodeJobs).FindJobToConfirm(jobId)) {
            auto operationId = jobToConfirmIt->ConcreteJob->second.OperationId;
            auto traceContextGuard = CreateOperationTraceContextGuard(
                "JobTrackerJobOrchidService",
                operationId);

            jobYson = BuildYsonStringFluently().BeginMap()
                    .Item("stage").Value("confirmation")
                    .Item("operation_id").Value(operationId)
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
            if (std::ssize(keys) >= limit) {
                break;
            }

            keys.push_back(ToString(operationId));
        }

        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        VERIFY_INVOKER_AFFINITY(JobTracker_->GetInvoker());

        auto operationId = TOperationId(TGuid::FromString(key));
        auto operationInfoIt = JobTracker_->RegisteredOperations_.find(operationId);
        if (operationInfoIt == std::end(JobTracker_->RegisteredOperations_)) {
            return nullptr;
        }

        const auto& operationInfo = operationInfoIt->second;

        auto traceContextGuard = CreateOperationTraceContextGuard(
            "JobTrackerOperationOrchidService",
            operationId);

        auto producer = TYsonProducer(BIND([
            jobsReady = operationInfo.JobsReady,
            operationId,
            jobTracker = JobTracker_
        ] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
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

        return IYPathService::FromProducerLazy(std::move(producer));
    }

private:
    const TJobTracker* const JobTracker_;
};

////////////////////////////////////////////////////////////////////

TJobTracker::TJobTracker(TBootstrap* bootstrap, TJobReporterPtr jobReporter)
    : Bootstrap_(bootstrap)
    , JobReporter_(std::move(jobReporter))
    , Config_(bootstrap->GetConfig()->ControllerAgent->JobTracker)
    , JobEventsControllerQueue_(bootstrap->GetConfig()->ControllerAgent->JobEventsControllerQueue)
    , HeartbeatStatisticsBytes_(NodeHeartbeatProfiler.WithHot().Counter("/statistics_bytes"))
    , HeartbeatDataStatisticsBytes_(NodeHeartbeatProfiler.WithHot().Counter("/data_statistics_bytes"))
    , HeartbeatJobResultBytes_(NodeHeartbeatProfiler.WithHot().Counter("/job_result_bytes"))
    , HeartbeatProtoMessageBytes_(NodeHeartbeatProfiler.WithHot().Counter("/proto_message_bytes"))
    , HeartbeatEnqueuedControllerEvents_(NodeHeartbeatProfiler.WithHot().GaugeSummary("/enqueued_controller_events"))
    , HeartbeatCount_(NodeHeartbeatProfiler.WithHot().Counter("/count"))
    , ReceivedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/job_count"))
    , ReceivedUnknownOperationCount_(NodeHeartbeatProfiler.WithHot().Counter("/unknown_operation_count"))
    , ReceivedRunningJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/running_job_count"))
    , ReceivedStaleRunningJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/stale_running_job_count"))
    , ReceivedFinishedJobCount_(NodeHeartbeatProfiler.Counter("/finished_job_count"))
    , ReceivedDuplicatedFinishedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/duplicated_finished_job_count"))
    , ReceivedUnknownJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/unknown_job_count"))
    , UnconfirmedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/unconfirmed_job_count"))
    , ConfirmedJobCount_(NodeHeartbeatProfiler.WithHot().Counter("/confirmed_job_count"))
    , DisappearedFromNodeJobAbortCount_(NodeHeartbeatProfiler.WithHot().Counter("/disappeared_from_node_job_count"))
    , JobAbortRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_abort_request_count"))
    , JobReleaseRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_release_request_count"))
    , JobInterruptionRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_interruption_request_count"))
    , JobFailureRequestCount_(JobTrackerProfiler.WithHot().Counter("/job_failure_request_count"))
    , NodeRegistrationCount_(JobTrackerProfiler.WithHot().Counter("/node_registration_count"))
    , NodeUnregistrationCount_(JobTrackerProfiler.WithHot().Counter("/node_unregistration_count"))
    , ThrottledRunningJobEventCount_(NodeHeartbeatProfiler.WithHot().Counter("/throttled_running_job_event_count"))
    , ThrottledHeartbeatCount_(NodeHeartbeatProfiler.WithHot().Counter("/throttled_heartbeat_count"))
    , ThrottledOperationCount_(NodeHeartbeatProfiler.WithHot().Counter("/throttled_operation_count"))
    , WrongIncarnationRequestCount_(JobTrackerProfiler.WithHot().Counter("/wrong_incarnation_request_count"))
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

    auto incarnationId = FromProto<TIncarnationId>(request->controller_agent_incarnation_id());
    auto nodeDescriptor = FromProto<TNodeDescriptor>(request->node_descriptor());

    auto nodeId = FromProto<TNodeId>(request->node_id());

    if (nodeId == InvalidNodeId) {
        THROW_ERROR_EXCEPTION(
            "Invalid node id; node is likely offline");
    }

    if (!incarnationId) {
        WrongIncarnationRequestCount_.Increment();
        THROW_ERROR_EXCEPTION(
            NControllerAgent::EErrorCode::AgentDisconnected,
            "Controller agent disconnected");
    }

    ProfileHeartbeatRequest(request);
    THashMap<TOperationId, std::vector<std::unique_ptr<TJobSummary>>> groupedJobSummaries;
    for (auto& job : *request->mutable_jobs()) {
        auto operationId = FromProto<TOperationId>(job.operation_id());

        auto traceContextGuard = CreateOperationTraceContextGuard(
            "ProcessHeartbeat",
            operationId);

        auto jobSummary = ParseJobSummary(&job, Logger);
        groupedJobSummaries[operationId].push_back(std::move(jobSummary));
    }

    THashSet<TAllocationId> allocationIdsRunningOnNode;
    allocationIdsRunningOnNode.reserve(std::size(request->allocations()));

    for (const auto& allocationInfoProto : request->allocations()) {
        allocationIdsRunningOnNode.insert(
            FromProto<TAllocationId>(allocationInfoProto.allocation_id()));
    }

    auto unconfirmedJobs = NYT::FromProto<std::vector<TJobId>>(request->unconfirmed_job_ids());

    THeartbeatProcessingContext heartbeatProcessingContext{
        .Context = context,
        .Logger = NControllerAgent::Logger.WithTag(
            "NodeId: %v, NodeAddress: %v",
            nodeId,
            nodeDescriptor.GetDefaultAddress()),
        .NodeAddress = nodeDescriptor.GetDefaultAddress(),
        .NodeId = nodeId,
        .IncarnationId = incarnationId,
        .Request = THeartbeatRequest{
            .GroupedJobSummaries = std::move(groupedJobSummaries),
            .AllocationIdsRunningOnNode = std::move(allocationIdsRunningOnNode),
            .UnconfirmedJobIds = std::move(unconfirmedJobs),
        },
    };

    BIND(
        &TJobTracker::DoProcessHeartbeat,
        Unretained(this),
        Passed(std::move(heartbeatProcessingContext)))
        .AsyncVia(GetCancelableInvokerOrThrow())
        .Run()
        .SubscribeUnique(BIND([this, context] (TErrorOr<THeartbeatProcessingResult>&& heartbeatProcessingResultOrError) {
            if (!heartbeatProcessingResultOrError.IsOK()) {
                context->Reply(std::move(heartbeatProcessingResultOrError));
                return;
            }
            auto heartbeatProcessingResult = std::move(heartbeatProcessingResultOrError.Value());
            ProfileHeartbeatProperties(heartbeatProcessingResult.Counters);

            heartbeatProcessingResult.Context.Context->Reply();
        })
            .Via(GetCurrentInvoker()));
}

void TJobTracker::SettleJob(const TJobTracker::TCtxSettleJobPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* request = &context->Request();
    auto* response = &context->Response();

    auto incarnationId = FromProto<TIncarnationId>(request->controller_agent_incarnation_id());
    auto nodeDescriptor = FromProto<TNodeDescriptor>(request->node_descriptor());

    auto nodeId = FromProto<TNodeId>(request->node_id());
    auto allocationId = FromProto<TAllocationId>(request->allocation_id());
    auto operationId = FromProto<TOperationId>(request->operation_id());

    if (nodeId == InvalidNodeId) {
        THROW_ERROR_EXCEPTION(
            "Invalid node id; node is likely offline");
    }

    THROW_ERROR_EXCEPTION_IF(
        !incarnationId,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    auto Logger = NControllerAgent::Logger.WithTag(
        "NodeId: %v, NodeAddress: %v, OperationId: %v, AllocationId: %v",
        nodeId,
        nodeDescriptor.GetDefaultAddress(),
        operationId,
        allocationId);

    SwitchTo(GetCancelableInvokerOrThrow());

    THROW_ERROR_EXCEPTION_IF(
        !IncarnationId_,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    if (incarnationId != IncarnationId_) {
        THROW_ERROR_EXCEPTION(
            NControllerAgent::EErrorCode::IncarnationMismatch,
            "Controller agent incarnation mismatch: expected %v, got %v)",
            IncarnationId_,
            incarnationId);
    }

    auto operationIt = RegisteredOperations_.find(operationId);
    if (operationIt == std::end(RegisteredOperations_)) {
        YT_LOG_DEBUG("Operation is not registered in job tracker, do not settle job");

        THROW_ERROR_EXCEPTION("No such operation %v", operationId);
    }

    const auto& operationInfo = operationIt->second;

    auto operationController = operationInfo.OperationController.Lock();
    if (!operationController) {
        YT_LOG_DEBUG("Operation controller is already reset, do not settle job");

        THROW_ERROR_EXCEPTION("Operation %v controller is already reset", operationId);
    }

    if (!operationInfo.JobsReady) {
        YT_LOG_DEBUG("Operation jobs are not ready yet, do not settle job");

        THROW_ERROR_EXCEPTION("Operation %v jobs are not ready yet", operationId);
    }

    auto asyncJobInfo = BIND(
        &IOperationController::SettleJob,
        operationController,
        allocationId)
        .AsyncVia(operationController->GetCancelableInvoker(EOperationControllerQueue::GetJobSpec))
        .Run();

    auto jobInfoOrError = WaitFor(
        asyncJobInfo,
        NRpc::TDispatcher::Get()->GetHeavyInvoker());

    if (!jobInfoOrError.IsOK() || !jobInfoOrError.Value().JobSpecBlob) {
        auto error = !jobInfoOrError.IsOK()
            ? static_cast<TError>(jobInfoOrError)
            : TError("Controller returned empty job spec (has controller crashed?)");
        YT_LOG_DEBUG(
            error,
            "Failed to extract job spec");

        ToProto(response->mutable_error(), error);

        context->Reply();

        return;
    }

    auto& jobInfo = jobInfoOrError.Value();
    ToProto(response->mutable_job_info()->mutable_job_id(), jobInfo.JobId);
    response->Attachments().push_back(std::move(jobInfo.JobSpecBlob));

    context->Reply();
}

TJobTrackerOperationHandlerPtr TJobTracker::RegisterOperation(
    TOperationId operationId,
    TWeakPtr<IOperationController> operationController)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto cancelableInvoker = GetCancelableInvoker();

    cancelableInvoker->Invoke(BIND(
        &TJobTracker::DoRegisterOperation,
        MakeStrong(this),
        operationId,
        Passed(std::move(operationController))));

    return New<TJobTrackerOperationHandler>(this, std::move(cancelableInvoker), operationId);
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
    for (const auto& job : request->jobs()) {
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
    ReceivedJobCount_.Increment(request->jobs_size());
}

void TJobTracker::AccountEnqueuedControllerEvent(int delta)
{
    auto newValue = EnqueuedControllerEventCount_.fetch_add(delta) + delta;
    HeartbeatEnqueuedControllerEvents_.Update(newValue);
}

void TJobTracker::ProfileHeartbeatProperties(const THeartbeatCounters& heartbeatCounters)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ReceivedRunningJobCount_.Increment(heartbeatCounters.RunningJobCount);
    ReceivedStaleRunningJobCount_.Increment(heartbeatCounters.StaleRunningJobCount);
    ReceivedFinishedJobCount_.Increment(heartbeatCounters.FinishedJobCount);
    ReceivedDuplicatedFinishedJobCount_.Increment(heartbeatCounters.DuplicatedFinishedJobCount);
    ReceivedUnknownJobCount_.Increment(heartbeatCounters.UnknownJobCount);
    UnconfirmedJobCount_.Increment(heartbeatCounters.UnconfirmedJobCount);
    ConfirmedJobCount_.Increment(heartbeatCounters.ConfirmedJobCount);
    DisappearedFromNodeJobAbortCount_.Increment(heartbeatCounters.DisappearedFromNodeJobAbortCount);
    JobAbortRequestCount_.Increment(heartbeatCounters.JobAbortRequestCount);
    JobReleaseRequestCount_.Increment(heartbeatCounters.JobReleaseRequestCount);
    JobInterruptionRequestCount_.Increment(heartbeatCounters.JobInterruptionRequestCount);
    JobFailureRequestCount_.Increment(heartbeatCounters.JobFailureRequestCount);
    ThrottledRunningJobEventCount_.Increment(heartbeatCounters.ThrottledRunningJobEventCount);
    ThrottledHeartbeatCount_.Increment(heartbeatCounters.ThrottledRunningJobEventCount > 0);
    ThrottledOperationCount_.Increment(heartbeatCounters.ThrottledOperationCount);
}

bool TJobTracker::TNodeJobs::IsRunning(const TJobIterator& jobIt)
{
    return !jobIt.ConcreteJob.has_value();
}

std::optional<TJobTracker::TNodeJobs::TJobToConfirmIterator>
TJobTracker::TNodeJobs::FindJobToConfirm(TJobId jobId)
{
    auto allocationId = AllocationIdFromJobId(jobId);

    if (auto it = JobsToConfirm.find(allocationId); it != std::end(JobsToConfirm)) {
        if (auto jobIt = it->second.find(jobId); jobIt != std::end(it->second)) {
            return TJobToConfirmIterator{
                .Allocation = it,
                .ConcreteJob = jobIt,
            };
        }
    }

    return std::nullopt;
}

void TJobTracker::TNodeJobs::EraseJobToConfirm(TJobTracker::TNodeJobs::TJobToConfirmIterator iterator)
{
    iterator.Allocation->second.erase(iterator.ConcreteJob);

    if (iterator.Allocation->second.empty()) {
        JobsToConfirm.erase(iterator.Allocation);
    }
}

TJobTracker::TNodeJobs::TJobToConfirmIterator
TJobTracker::TNodeJobs::AddJobToConfirmOrCrash(TJobId jobId, TJobToConfirmInfo jobToConfirmInfo)
{
    auto allocationId = AllocationIdFromJobId(jobId);

    auto allocationIt = JobsToConfirm.emplace(allocationId, THashMap<TJobId, TJobToConfirmInfo>()).first;
    auto jobIt = EmplaceOrCrash(allocationIt->second, jobId, std::move(jobToConfirmInfo));

    return TJobToConfirmIterator{
        .Allocation = allocationIt,
        .ConcreteJob = jobIt,
    };
}

i64 TJobTracker::TNodeJobs::GetJobToConfirmCount() const
{
    i64 count = 0;

    for (const auto& [_, jobsToConfirm] : JobsToConfirm) {
        count += std::ssize(jobsToConfirm);
    }

    return count;
}

template <CInvocable<void(TJobId, TJobTracker::TJobToConfirmInfo)> TFunction>
void TJobTracker::TNodeJobs::ForEachJobToConfirm(const TFunction& func) const
{
    TForbidContextSwitchGuard guard;

    for (const auto& [_, jobsToConfirm] : JobsToConfirm) {
        for (const auto& [jobId, jobToConfirmInfo] : jobsToConfirm) {
            func(jobId, jobToConfirmInfo);
        }
    }
}

std::optional<TJobTracker::TNodeJobs::TJobIterator>
TJobTracker::TNodeJobs::FindJob(TJobId jobId)
{
    auto allocationId = AllocationIdFromJobId(jobId);

    if (auto it = Allocations.find(allocationId); it != std::end(Allocations)) {
        if (it->second.RunningJob.has_value() && it->second.RunningJob->JobId == jobId) {
            return TJobIterator{
                .Allocation = it,
                .ConcreteJob = std::nullopt,
            };
        }

        auto& finishedJobs = it->second.FinishedJobs;

        if (auto jobIt = finishedJobs.find(jobId); jobIt != std::end(finishedJobs)) {
            return TJobIterator{
                .Allocation = it,
                .ConcreteJob = jobIt,
            };
        }
    }

    return std::nullopt;
}

void TJobTracker::TNodeJobs::EraseJobOrCrash(TJobId jobId)
{
    auto it = FindJob(jobId);

    YT_VERIFY(it);
    EraseJob(*it);
}

void TJobTracker::TNodeJobs::EraseJob(TJobIterator jobIt)
{
    auto& allocationInfo = jobIt.Allocation->second;

    if (IsRunning(jobIt)) {
        YT_VERIFY(allocationInfo.RunningJob.has_value());
        allocationInfo.RunningJob.reset();
    } else {
        allocationInfo.FinishedJobs.erase(*jobIt.ConcreteJob);
    }

    if (!allocationInfo.RunningJob.has_value() && allocationInfo.FinishedJobs.empty()) {
        Allocations.erase(jobIt.Allocation);
    }
}

TJobTracker::TNodeJobs::TJobIterator
TJobTracker::TNodeJobs::AddRunningJobOrCrash(TJobId jobId, TOperationId operationId, TRequestedActionInfo requestedActionInfo)
{
    auto allocationId = AllocationIdFromJobId(jobId);

    auto allocationIt = Allocations.emplace(
        allocationId,
        TAllocationInfo{
            .OperationId = operationId,
        }).first;
    auto& allocationInfo = allocationIt->second;

    YT_VERIFY(!allocationInfo.RunningJob.has_value());

    allocationInfo.RunningJob.emplace(TRunningJobInfo{
        .RequestedActionInfo = std::move(requestedActionInfo),
        .JobId = jobId,
    });

    return TJobIterator{
        .Allocation = allocationIt,
        .ConcreteJob = std::nullopt,
    };
}

i64 TJobTracker::TNodeJobs::GetJobCount() const
{
    i64 count = 0;

    for (const auto& [_, allocationInfo] : Allocations) {
        count += std::ssize(allocationInfo.FinishedJobs) + allocationInfo.RunningJob.has_value();
    }

    return count;
}

template <class TFunction>
void TJobTracker::TNodeJobs::ForEachJob(const TFunction& func) const
{
    struct TJobInfoAdaptor
    {
        std::variant<TFinishedJobInfo, TRunningJobInfo> Info;
        const TOperationId OperationId;
    };

    TForbidContextSwitchGuard guard;

    for (const auto& [_, allocationInfo] : Allocations) {
        if (allocationInfo.RunningJob.has_value()) {
            func(
                allocationInfo.RunningJob->JobId,
                TJobInfoAdaptor{
                    .Info = *allocationInfo.RunningJob,
                    .OperationId = allocationInfo.OperationId,
                });
        }

        for (const auto& [jobId, finishedJobInfo] : allocationInfo.FinishedJobs) {
            func(
                jobId,
                TJobInfoAdaptor{
                    .Info = finishedJobInfo,
                    .OperationId = allocationInfo.OperationId,
                });
        }
    }
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

TJobTracker::THeartbeatProcessingResult TJobTracker::DoProcessHeartbeat(
    THeartbeatProcessingContext heartbeatProcessingContext)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    THROW_ERROR_EXCEPTION_IF(
        !IncarnationId_,
        NControllerAgent::EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    if (heartbeatProcessingContext.IncarnationId != IncarnationId_) {
        WrongIncarnationRequestCount_.Increment();
        THROW_ERROR_EXCEPTION(
            NControllerAgent::EErrorCode::IncarnationMismatch,
            "Controller agent incarnation mismatch: expected %v, got %v)",
            IncarnationId_,
            heartbeatProcessingContext.IncarnationId);
    }

    auto* response = &heartbeatProcessingContext.Context->Response();
    auto& Logger = heartbeatProcessingContext.Logger;

    TForbidContextSwitchGuard guard;

    auto& nodeInfo = UpdateOrRegisterNode(
        heartbeatProcessingContext.NodeId,
        heartbeatProcessingContext.NodeAddress);

    auto& nodeJobs = nodeInfo.Jobs;

    THeartbeatProcessingResult heartbeatProcessingResult{};
    auto& heartbeatCounters = heartbeatProcessingResult.Counters;

    {
        TOperationIdToJobsToAbort jobsToAbort;
        for (auto jobId : heartbeatProcessingContext.Request.UnconfirmedJobIds) {
            if (auto jobToConfirmIt = nodeJobs.FindJobToConfirm(jobId)) {
                auto operationId = jobToConfirmIt->ConcreteJob->second.OperationId;

                if (nodeJobs.AbortedAllocations.erase(AllocationIdFromJobId(jobId))) {
                    YT_LOG_DEBUG(
                        "Ignore allocation abort since job is unconfirmed (JobId: %v, AllocationId: %v, OperationId: %v)",
                        jobId,
                        AllocationIdFromJobId(jobId),
                        operationId);
                } else {
                    YT_LOG_DEBUG(
                        "Job unconfirmed, abort it (JobId: %v, OperationId: %v)",
                        jobId,
                        operationId);
                }
                jobsToAbort[operationId].push_back(TJobToAbort{
                    .JobId = jobId,
                    .AbortReason = EAbortReason::Unconfirmed,
                });

                auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);
                EraseOrCrash(operationInfo.TrackedJobIds, jobId);
                nodeJobs.EraseJobToConfirm(*jobToConfirmIt);
            }
        }

        AbortJobs(std::move(jobsToAbort));

        heartbeatCounters.UnconfirmedJobCount = std::ssize(heartbeatProcessingContext.Request.UnconfirmedJobIds);
    }

    THashSet<TJobId> jobsToSkip;
    jobsToSkip.reserve(std::size(nodeJobs.JobsToAbort) + std::size(nodeJobs.JobsToRelease));

    for (auto [jobId, abortReason] : nodeJobs.JobsToAbort) {
        EmplaceOrCrash(jobsToSkip, jobId);

        YT_LOG_DEBUG(
            "Request node to abort job (JobId: %v)",
            jobId);
        NProto::ToProto(
            response->add_jobs_to_abort(),
            TJobToAbort{
                .JobId = jobId,
                .AbortReason = abortReason
            });
        ++heartbeatCounters.JobAbortRequestCount;
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
            ++heartbeatCounters.JobReleaseRequestCount;
        }

        for (auto jobId : releasedJobs) {
            EraseOrCrash(nodeJobs.JobsToRelease, jobId);
        }
    }

    const auto& heartbeatProcessingLogger = Logger;
    for (auto& [operationId, jobSummaries] : heartbeatProcessingContext.Request.GroupedJobSummaries) {
        auto traceContextGuard = CreateOperationTraceContextGuard(
            "ProcessJobSummaries",
            operationId);

        auto Logger = heartbeatProcessingLogger.WithTag(
            "OperationId: %v",
            operationId);

        auto operationIt = RegisteredOperations_.find(operationId);
        if (operationIt == std::end(RegisteredOperations_)) {
            YT_LOG_DEBUG("Operation is not registered at job tracker, skip handling job infos from node");

            ToProto(response->add_unknown_operation_ids(), operationId);

            ++heartbeatCounters.UnknownOperationCount;

            continue;
        }

        auto& operationInfo = operationIt->second;

        auto operationController = operationInfo.OperationController.Lock();
        if (!operationController) {
            YT_LOG_DEBUG("Operation controller is already reset, skip handling job infos from node");

            continue;
        }

        if (!operationInfo.JobsReady) {
            YT_LOG_DEBUG("Operation jobs are not ready yet, skip handling job infos from node");

            continue;
        }

        TJobsToProcessInOperationController jobsToProcessInOperationController;
        jobsToProcessInOperationController.JobSummaries.reserve(std::size(jobSummaries));
        jobsToProcessInOperationController.JobsToAbort.reserve(std::size(nodeJobs.AbortedAllocations));

        const auto& operationLogger = Logger;
        bool shouldSkipRunningJobEvents = operationController->ShouldSkipRunningJobEvents();
        bool throttledAnyEvents = false;

        for (auto& jobSummary : jobSummaries) {
            auto jobId = jobSummary->Id;

            if (jobsToSkip.contains(jobId)) {
                continue;
            }

            auto Logger = operationLogger.WithTag(
                "JobId: %v",
                jobId);

            auto newJobStage = JobStageFromJobState(jobSummary->State);

            if (auto it = nodeJobs.FindJob(jobId)) {
                bool wasJobEventThrottled = !HandleJobInfo(
                    *it,
                    nodeJobs,
                    operationInfo,
                    jobsToProcessInOperationController,
                    response,
                    jobSummary,
                    Logger,
                    heartbeatCounters,
                    shouldSkipRunningJobEvents);
                throttledAnyEvents |= wasJobEventThrottled;

                continue;
            }

            if (auto jobToConfirmIt = nodeJobs.FindJobToConfirm(jobId)) {
                auto& jobToConfirmInfo = jobToConfirmIt->ConcreteJob->second;
                YT_LOG_DEBUG(
                    "Job confirmed (JobStage: %v)",
                    newJobStage);
                YT_VERIFY(jobToConfirmInfo.OperationId == operationId);

                auto jobIt = [&, operationId = operationId] {
                    auto requestedActionInfo = std::move(jobToConfirmInfo.RequestedActionInfo);

                    nodeJobs.EraseJobToConfirm(*jobToConfirmIt);
                    return nodeJobs.AddRunningJobOrCrash(
                        jobId,
                        operationId,
                        std::move(requestedActionInfo));
                }();

                ++heartbeatCounters.ConfirmedJobCount;

                bool wasJobEventThrottled = !HandleJobInfo(
                    jobIt,
                    nodeJobs,
                    operationInfo,
                    jobsToProcessInOperationController,
                    response,
                    jobSummary,
                    Logger,
                    heartbeatCounters,
                    shouldSkipRunningJobEvents);
                throttledAnyEvents |= wasJobEventThrottled;

                continue;
            }

            // Remove or abort unknown job.

            bool shouldAbortJob = newJobStage == EJobStage::Running;

            YT_LOG_DEBUG(
                "Request node to %v unknown job (JobState: %v)",
                shouldAbortJob ? "abort" : "remove",
                jobSummary->State);

            ++heartbeatCounters.UnknownJobCount;

            if (shouldAbortJob) {
                ++heartbeatCounters.JobAbortRequestCount;
                NProto::ToProto(
                    response->add_jobs_to_abort(),
                    TJobToAbort{
                        .JobId = jobId,
                        .AbortReason = EAbortReason::Unknown,
                    });
                ReportUnknownJobInArchive(jobId, operationId, nodeInfo.NodeAddress);
            } else {
                ++heartbeatCounters.JobReleaseRequestCount;
                ToProto(response->add_jobs_to_remove(), TJobToRelease{jobId});
            }
        }

        heartbeatCounters.ThrottledOperationCount += throttledAnyEvents;

        AccountEnqueuedControllerEvent(+1);
        // Raw pointer is OK since the job tracker never dies.
        auto discountGuard = Finally(std::bind(&TJobTracker::AccountEnqueuedControllerEvent, this, -1));
        operationController->GetCancelableInvoker(JobEventsControllerQueue_)->Invoke(
            BIND([
                    Logger,
                    operationController,
                    operationId = operationId,
                    jobsToProcessInOperationController = std::move(jobsToProcessInOperationController),
                    discountGuard = std::move(discountGuard)
                ] () mutable {
                    for (auto& jobSummary : jobsToProcessInOperationController.JobSummaries) {
                        YT_VERIFY(jobSummary);

                        auto jobId = jobSummary->Id;
                        auto jobState = jobSummary->State;

                        try {
                            operationController->OnJobInfoReceivedFromNode(std::move(jobSummary));
                        } catch (const std::exception& ex) {
                            YT_LOG_FATAL(
                                ex,
                                "Failed to process job info from node (JobId: %v, OperationId: %v, JobState: %v)",
                                jobId,
                                operationId,
                                jobState);
                        }
                    }

                    for (const auto& jobToAbort : jobsToProcessInOperationController.JobsToAbort) {
                        try {
                            operationController->AbortJobByJobTracker(jobToAbort.JobId, jobToAbort.AbortReason);
                        } catch (const std::exception& ex) {
                            YT_LOG_FATAL(
                                ex,
                                "Failed to abort job in operation controller during node heartbeat processing"
                                " (JobId: %v, OperationId: %v, AbortReason: %v)",
                                jobToAbort.JobId,
                                operationId,
                                jobToAbort.AbortReason);
                        }
                    }
                }));
    }

    {
        auto now = TInstant::Now();

        TOperationIdToJobsToAbort jobIdsToAbort;

        for (auto& [allocationId, allocationInfo] : nodeJobs.Allocations) {
            if (!allocationInfo.RunningJob.has_value()) {
                if (nodeJobs.AbortedAllocations.erase(allocationId)) {
                    YT_LOG_DEBUG(
                        "Ignore allocation abort since no job is running (AllocationId: %v)",
                        allocationId);
                }
                continue;
            }

            auto& jobInfo = *allocationInfo.RunningJob;

            if (auto it = nodeJobs.AbortedAllocations.find(allocationId);
                it != std::end(nodeJobs.AbortedAllocations))
            {
                auto abortReason = it->second.AbortReason;
                YT_LOG_DEBUG(
                    "Job from aborted allocation not received from node; abort it"
                    " (JobId: %v, AllocationId: %v, OperationId: %v, AbortReason: %v)",
                    jobInfo.JobId,
                    allocationId,
                    allocationInfo.OperationId,
                    abortReason);
                jobIdsToAbort[allocationInfo.OperationId].push_back(TJobToAbort{
                    .JobId = jobInfo.JobId,
                    .AbortReason = abortReason,
                });

                nodeJobs.AbortedAllocations.erase(it);
                continue;
            }

            if (heartbeatProcessingContext.Request.AllocationIdsRunningOnNode.contains(allocationId)) {
                continue;
            }

            if (!jobInfo.DisappearedFromNodeSince) {
                jobInfo.DisappearedFromNodeSince = now;
                continue;
            }

            if (now - jobInfo.DisappearedFromNodeSince < Config_->DurationBeforeJobConsideredDisappearedFromNode) {
                continue;
            }

            YT_LOG_DEBUG(
                "Job disappeared from mode, aborting it (JobId: %v, OperationId: %v, Since: %v)",
                jobInfo.JobId,
                allocationInfo.OperationId,
                jobInfo.DisappearedFromNodeSince);

            ++heartbeatCounters.DisappearedFromNodeJobAbortCount;

            jobIdsToAbort[allocationInfo.OperationId].push_back(TJobToAbort{
                .JobId = jobInfo.JobId,
                .AbortReason = EAbortReason::DisappearedFromNode,
            });
        }

        for (const auto& [operationId, jobsToAbort] : jobIdsToAbort) {
            auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

            for (auto jobToAbort : jobsToAbort) {
                nodeJobs.EraseJobOrCrash(jobToAbort.JobId);
                EraseOrCrash(operationInfo.TrackedJobIds, jobToAbort.JobId);
            }
        }

        AbortJobs(jobIdsToAbort);
    }

    // NB(pogorelov): It is a rare situation, no need to call reserve here.
    THashMap<TAllocationId, TAbortedAllocationSummary> revivingJobsAbortedAllocations;
    nodeJobs.ForEachJobToConfirm([&] (TJobId jobId, TJobToConfirmInfo jobToConfirmInfo) {
        YT_LOG_DEBUG(
            "Request node to confirm job (JobId: %v)",
            jobId);
        ToProto(response->add_jobs_to_confirm(), TJobToConfirm{jobId});

        if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId))) {
            YT_LOG_DEBUG(
                "Postpone processing aborted allocation since job is confirming"
                " (JobId: %v, AllocationId: %v, OperationId: %v, AbortReason: %v)",
                jobId,
                AllocationIdFromJobId(jobId),
                jobToConfirmInfo.OperationId,
                it->second.AbortReason);
            revivingJobsAbortedAllocations[AllocationIdFromJobId(jobId)] = std::move(it->second);
            nodeJobs.AbortedAllocations.erase(it);
        }
    });

    YT_VERIFY(std::empty(nodeJobs.AbortedAllocations));
    nodeJobs.AbortedAllocations = std::move(revivingJobsAbortedAllocations);

    heartbeatProcessingResult.Context = std::move(heartbeatProcessingContext);

    return heartbeatProcessingResult;
}

bool TJobTracker::HandleJobInfo(
    TNodeJobs::TJobIterator jobIt,
    TNodeJobs& nodeJobs,
    TOperationInfo& operationInfo,
    TJobsToProcessInOperationController& jobsToProcessInOperationController,
    TCtxHeartbeat::TTypedResponse* response,
    std::unique_ptr<TJobSummary>& jobSummary,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters,
    bool shouldSkipRunningJobEvents)
{
    if (TNodeJobs::IsRunning(jobIt)) {
        return HandleRunningJobInfo(
            jobIt,
            nodeJobs,
            operationInfo,
            jobsToProcessInOperationController,
            response,
            jobSummary,
            Logger,
            heartbeatCounters,
            shouldSkipRunningJobEvents);
    } else {
        return HandleFinishedJobInfo(
            jobIt,
            nodeJobs,
            operationInfo,
            jobsToProcessInOperationController,
            response,
            jobSummary,
            Logger,
            heartbeatCounters);
    }
}

bool TJobTracker::HandleRunningJobInfo(
    TNodeJobs::TJobIterator jobIt,
    TNodeJobs& nodeJobs,
    TOperationInfo& operationInfo,
    TJobsToProcessInOperationController& jobsToProcessInOperationController,
    TCtxHeartbeat::TTypedResponse* response,
    std::unique_ptr<TJobSummary>& jobSummary,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters,
    bool shouldSkipRunningJobEvents)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());
    auto& allocationInfo = jobIt.Allocation->second;

    auto newJobStage = JobStageFromJobState(jobSummary->State);
    auto jobId = jobSummary->Id;
    const auto& requestedActionInfo = allocationInfo.RunningJob->RequestedActionInfo;

    if (newJobStage == EJobStage::Running) {
        ++heartbeatCounters.RunningJobCount;

        if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId));
            it != std::end(nodeJobs.AbortedAllocations))
        {
            auto abortReason = it->second.AbortReason;
            YT_LOG_DEBUG(
                "Aborting running job of aborted allocation (AllocationId: %v, AbortReason: %v)",
                AllocationIdFromJobId(jobId),
                abortReason);
            jobsToProcessInOperationController.JobsToAbort.push_back({
                .JobId = jobId,
                .AbortReason = abortReason,
            });

            NProto::ToProto(
                response->add_jobs_to_abort(),
                TJobToAbort{
                    .JobId = jobId,
                    .AbortReason = abortReason,
                });

            nodeJobs.EraseJobOrCrash(jobId);
            EraseOrCrash(operationInfo.TrackedJobIds, jobId);

            nodeJobs.AbortedAllocations.erase(it);

            return /*wasHandled*/ true;
        }

        Visit(
            requestedActionInfo,
            [] (TNoActionRequested) {},
            [&] (const TInterruptionRequestOptions& requestOptions) {
                ProcessInterruptionRequest(response, requestOptions, jobId, Logger, heartbeatCounters);
            },
            [&] (const TGracefulAbortRequestOptions& requestOptions) {
                ProcessGracefulAbortRequest(response, requestOptions, jobId, Logger, heartbeatCounters);
            });

        if (shouldSkipRunningJobEvents) {
            YT_LOG_DEBUG("Skipping running job summary because operation controller invoker is overloaded");
            ++heartbeatCounters.ThrottledRunningJobEventCount;

            return /*wasHandled*/ false;
        }

        jobsToProcessInOperationController.JobSummaries.push_back(std::move(jobSummary));

        return /*wasHandled*/ true;
    }

    YT_VERIFY(newJobStage == EJobStage::Finished);

    ++heartbeatCounters.FinishedJobCount;

    if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId));
        it != std::end(nodeJobs.AbortedAllocations))
    {
        YT_LOG_DEBUG(
            "Ignore allocation abort since job is already finished (AllocationId: %v)",
            AllocationIdFromJobId(jobId));

        nodeJobs.AbortedAllocations.erase(it);
    }

    Visit(
        requestedActionInfo,
        [&] (TNoActionRequested) {
            YT_LOG_DEBUG("Received finished job info");
        },
        [&] (const TInterruptionRequestOptions& /*requestOptions*/) {
            YT_LOG_DEBUG("Job is already finished; interruption request ignored");
        },
        [&] (const TGracefulAbortRequestOptions& /*requestOptions*/) {
            YT_LOG_DEBUG("Job is already finished; failure request ignored");
        });

    ToProto(
        response->add_jobs_to_store(),
        TJobToStore{
            .JobId = jobId
        });

    EmplaceOrCrash(
        allocationInfo.FinishedJobs,
        allocationInfo.RunningJob->JobId,
        TFinishedJobInfo());
    allocationInfo.RunningJob.reset();

    jobsToProcessInOperationController.JobSummaries.push_back(std::move(jobSummary));

    return /*wasHandled*/ true;
}

bool TJobTracker::HandleFinishedJobInfo(
    TNodeJobs::TJobIterator /*jobIt*/,
    TNodeJobs& nodeJobs,
    TOperationInfo& /*operationInfo*/,
    TJobsToProcessInOperationController& /*jobsToProcessInOperationController*/,
    TCtxHeartbeat::TTypedResponse* response,
    std::unique_ptr<TJobSummary>& jobSummary,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto newJobStage = JobStageFromJobState(jobSummary->State);
    auto jobId = jobSummary->Id;

    if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId));
        it != std::end(nodeJobs.AbortedAllocations))
    {
        YT_LOG_DEBUG(
            "Ignore allocation abort since job is already finished (AllocationId: %v)",
            AllocationIdFromJobId(jobId));

        nodeJobs.AbortedAllocations.erase(it);
    }

    if (newJobStage < EJobStage::Finished) {
        ++heartbeatCounters.StaleRunningJobCount;

        YT_LOG_DEBUG(
            "Stale job info received (CurrentJobStage: %v, ReceivedJobState: %v)",
            EJobStage::Finished,
            newJobStage);

        return /*wasHandled*/ true;
    }

    ++heartbeatCounters.DuplicatedFinishedJobCount;

    ToProto(
        response->add_jobs_to_store(),
        TJobToStore{
            .JobId = jobId,
        });

    YT_LOG_DEBUG(
        "Finished job info received again, do not process it in operation controller");

    return /*wasHandled*/ true;
}

void TJobTracker::ProcessInterruptionRequest(
    TCtxHeartbeat::TTypedResponse* response,
    const TInterruptionRequestOptions& requestOptions,
    TJobId jobId,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_DEBUG(
        "Request node to interrupt job (InterruptionReason: %v, InterruptionTimeout: %v)",
        requestOptions.Reason,
        requestOptions.Timeout);

    ++heartbeatCounters.JobInterruptionRequestCount;

    auto* protoJobToInterrupt = response->add_jobs_to_interrupt();
    ToProto(protoJobToInterrupt->mutable_job_id(), jobId);
    protoJobToInterrupt->set_timeout(ToProto<i64>(requestOptions.Timeout));
    protoJobToInterrupt->set_reason(ToProto<i32>(requestOptions.Reason));
}

void TJobTracker::ProcessGracefulAbortRequest(
    TCtxHeartbeat::TTypedResponse* response,
    const TGracefulAbortRequestOptions& requestOptions,
    TJobId jobId,
    const NLogging::TLogger& Logger,
    THeartbeatCounters& heartbeatCounters)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_DEBUG("Request node to gracefully abort job");

    ++heartbeatCounters.JobFailureRequestCount;

    if (Config_->EnableGracefulAbort) {
        NProto::ToProto(
            response->add_jobs_to_abort(),
            TJobToAbort{
                .JobId = jobId,
                .AbortReason = requestOptions.Reason,
                .Graceful = true,
            });
    } else {
        auto* protoJobToFail = response->add_jobs_to_fail();
        ToProto(protoJobToFail->mutable_job_id(), jobId);
    }
}

void TJobTracker::DoRegisterOperation(
    TOperationId operationId,
    TWeakPtr<IOperationController> operationController)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    YT_LOG_DEBUG(
        "Registering operation (OperationId: %v)",
        operationId);

    EmplaceOrCrash(
        RegisteredOperations_,
        operationId,
        TOperationInfo{
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
    auto& [
        nodeJobs,
        _,
        registrationId,
        nodeAddress
    ] = GetOrRegisterNode(nodeId, jobInfo.NodeAddress);

    YT_LOG_DEBUG(
        "Register job (JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
        jobInfo.JobId,
        operationId,
        nodeId,
        nodeAddress);

    nodeJobs.AddRunningJobOrCrash(
        jobInfo.JobId,
        operationId,
        TNoActionRequested());

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
        nodeJobs.AddJobToConfirmOrCrash(
            jobInfo.JobId,
            TJobToConfirmInfo{
                .RequestedActionInfo = TNoActionRequested(),
                .OperationId = operationId,
            });

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
            if (auto jobIt = nodeJobs.FindJob(jobToRelease.JobId)) {
                auto jobOperationId = jobIt->Allocation->second.OperationId;

                YT_VERIFY(jobOperationId == operationId);

                EraseOrCrash(operationJobs, jobToRelease.JobId);
                nodeJobs.EraseJob(*jobIt);
            }

            EmplaceOrCrash(nodeJobs.JobsToRelease, jobToRelease.JobId, jobToRelease.ReleaseFlags);
        }
    }
}

void TJobTracker::RequestJobAbortion(TJobId jobId, TOperationId operationId, EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromJobId(jobId);

    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo) {
        YT_LOG_DEBUG(
            "Node is not registered, skip job abortion request (JobId: %v, NodeId: %v, NodeAddress: %v, Reason: %v, OperationId: %v)",
            jobId,
            nodeId,
            GetNodeAddressForLogging(nodeId),
            reason,
            operationId);
        return;
    }

    auto& nodeJobs = nodeInfo->Jobs;
    const auto& nodeAddress = nodeInfo->NodeAddress;

    YT_LOG_DEBUG(
        "Abort job (JobId: %v, AbortReason: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
        jobId,
        reason,
        operationId,
        nodeId,
        nodeAddress);

    auto removeJobFromOperation = [operationId, jobId, this] {
        auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);
        EraseOrCrash(operationInfo.TrackedJobIds, jobId);
    };

    if (auto jobIt = nodeJobs.FindJob(jobId)) {
        nodeJobs.EraseJob(*jobIt);

        removeJobFromOperation();

        if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId));
            it != std::end(nodeJobs.AbortedAllocations))
        {
            YT_LOG_DEBUG(
                "Allocation abort ignored since job is aborted by allocation tracker"
                " (JobId: %v, AllocationId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v, AllocationAbortReason: %v, JobAbortReason: %v)",
                jobId,
                AllocationIdFromJobId(jobId),
                operationId,
                nodeId,
                nodeInfo->NodeAddress,
                it->second.AbortReason,
                reason);

            nodeJobs.AbortedAllocations.erase(it);
        }
    } else if (auto jobToConfirmIt = nodeJobs.FindJobToConfirm(jobId)) {
        nodeJobs.EraseJobToConfirm(*jobToConfirmIt);

        removeJobFromOperation();

        if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId));
            it != std::end(nodeJobs.AbortedAllocations))
        {
            YT_LOG_DEBUG(
                "Allocation abort ignored since reviving job is aborted by allocation tracker"
                " (JobId: %v, AllocationId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v, AllocationAbortReason: %v, JobAbortReason: %v)",
                jobId,
                AllocationIdFromJobId(jobId),
                operationId,
                nodeId,
                nodeInfo->NodeAddress,
                it->second.AbortReason,
                reason);

            nodeJobs.AbortedAllocations.erase(it);
        }
    }

    // NB(pogorelov): AbortJobOnNode may be called twice on operation finishing.
    nodeJobs.JobsToAbort.emplace(jobId, reason);
}

template <class TAction>
void TJobTracker::TryRequestJobAction(
    TJobId jobId,
    TOperationId operationId,
    TAction action,
    TStringBuf actionName)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    auto nodeId = NodeIdFromJobId(jobId);

    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo) {
        YT_LOG_DEBUG(
            "Node is not registered, skip action request "
            "(ActionName: %v, JobId: %v, NodeId: %v, NodeAddress: %v, OperationId: %v)",
            actionName,
            jobId,
            nodeId,
            GetNodeAddressForLogging(nodeId),
            operationId);
        return;
    }

    auto& nodeJobs = nodeInfo->Jobs;
    const auto& nodeAddress = nodeInfo->NodeAddress;

    if (auto jobIt = nodeJobs.FindJob(jobId)) {
        if (TNodeJobs::IsRunning(*jobIt)) {
            YT_LOG_DEBUG(
                "Requesting action (ActionName: %v, JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
                actionName,
                jobId,
                operationId,
                nodeId,
                nodeAddress);

            action(jobIt->Allocation->second.RunningJob->RequestedActionInfo);
        } else {
            YT_LOG_DEBUG(
                "Job is not running; skip action request (JobId: %v, OperationId: %v)",
                jobId,
                operationId);
        }
    } else if (auto jobToConfirmIt = nodeJobs.FindJobToConfirm(jobId)) {
        YT_LOG_DEBUG(
            "Job waiting for confirmation, requesting action "
            "(ActionName: %v, JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
            actionName,
            jobId,
            operationId,
            nodeId,
            nodeAddress);

        action(jobToConfirmIt->ConcreteJob->second.RequestedActionInfo);
    } else {
        YT_LOG_DEBUG(
            "Job not found; skip action request "
            "(ActionName: %v, JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
            actionName,
            jobId,
            operationId,
            nodeId,
            nodeAddress);
    }
}

void TJobTracker::RequestJobInterruption(
    TJobId jobId,
    TOperationId operationId,
    EInterruptReason reason,
    TDuration timeout)
{
    TryRequestJobAction(
        jobId,
        operationId,
        [&] (TRequestedActionInfo& requestedActionInfo) {
            DoRequestJobInterruption(requestedActionInfo, jobId, operationId, reason, timeout);
        },
        /*actionName*/ "interruption");
}

void TJobTracker::DoRequestJobInterruption(
    TRequestedActionInfo& requestedActionInfo,
    TJobId jobId,
    TOperationId operationId,
    NScheduler::EInterruptReason reason,
    TDuration timeout)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    Visit(
        requestedActionInfo,
        [&] (TNoActionRequested) {
            requestedActionInfo = TInterruptionRequestOptions{
                .Reason = reason,
                .Timeout = timeout,
            };
        },
        [&] (const TInterruptionRequestOptions& requestOptions) {
            if (timeout < requestOptions.Timeout) {
                YT_LOG_DEBUG(
                    "Updating interruption request "
                    "(JobId: %v, OperationId: %v, OldTimeout: %v, OldReason: %v, NewTimeout: %v, NewReason: %v)",
                    jobId,
                    operationId,
                    requestOptions.Timeout,
                    requestOptions.Reason,
                    timeout,
                    reason);

                requestedActionInfo = TInterruptionRequestOptions{
                    .Reason = reason,
                    .Timeout = timeout,
                };

                return;
            }

            YT_LOG_DEBUG(
                "Job interruption is already requested with lower timeout; skip new request (JobId: %v, OperationId: %v)",
                jobId,
                operationId);
        },
        [&] (TGracefulAbortRequestOptions& /*requestOptions*/) {
            YT_LOG_FATAL(
                "Unexpected interruption request after graceful abort request (JobId: %v, OperationId: %v)",
                jobId,
                operationId);
        });
}


void TJobTracker::RequestJobGracefulAbort(
    TJobId jobId,
    TOperationId operationId,
    EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    TryRequestJobAction(
        jobId,
        operationId,
        [&] (TRequestedActionInfo& requestedActionInfo) {
            DoRequestJobGracefulAbort(requestedActionInfo, jobId, operationId, reason);
        },
        /*actionName*/ "graceful abort");
}

void TJobTracker::DoRequestJobGracefulAbort(
    TRequestedActionInfo& requestedActionInfo,
    TJobId jobId,
    TOperationId operationId,
    EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    Visit(
        requestedActionInfo,
        [&] (TNoActionRequested) {
            requestedActionInfo = TGracefulAbortRequestOptions{
                .Reason = reason,
            };
        },
        [&] (const TInterruptionRequestOptions& /*requestOptions*/) {
            YT_LOG_DEBUG(
                "Request job graceful abort despite interruption request (JobId: %v, OperationId: %v)",
                jobId,
                operationId);

            requestedActionInfo = TGracefulAbortRequestOptions{
                .Reason = reason,
            };
        },
        [&] (TGracefulAbortRequestOptions& /*requestOptions*/) { });
}

void TJobTracker::ReportUnknownJobInArchive(TJobId jobId, TOperationId operationId, const TString& nodeAddress)
{
    VERIFY_THREAD_AFFINITY_ANY();

    JobReporter_->HandleJobReport(
        TControllerJobReport()
            .OperationId(operationId)
            .JobId(jobId)
            .Address(nodeAddress)
            .ControllerState(EJobState::Aborted));
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

    auto registrationId = TGuid::Create();

    YT_LOG_DEBUG(
        "Register node (NodeId: %v, NodeAddress: %v, RegistrationId: %v, DisconnectionTimeout: %v)",
        nodeId,
        nodeAddress,
        registrationId,
        Config_->NodeDisconnectionTimeout);

    if (auto nodeIdIt = NodeAddressToNodeId_.find(nodeAddress); nodeIdIt != std::end(NodeAddressToNodeId_)) {
        auto oldNodeId = nodeIdIt->second;
        YT_LOG_WARNING(
            "Node with the same address is already registered, unregister old node and register new (NodeAddress: %v, NewNodeId: %v, OldNodeId: %v)",
            nodeAddress,
            nodeId,
            nodeIdIt->second);

        UnregisterNode(oldNodeId, nodeAddress);
    }

    NodeRegistrationCount_.Increment();

    auto lease = TLeaseManager::CreateLease(
        Config_->NodeDisconnectionTimeout,
        BIND_NO_PROPAGATE(&TJobTracker::OnNodeHeartbeatLeaseExpired, MakeWeak(this), registrationId, nodeId, nodeAddress)
            .Via(GetCancelableInvoker()));

    EmplaceOrCrash(NodeAddressToNodeId_, nodeAddress, nodeId);

    auto emplaceIt = EmplaceOrCrash(
        RegisteredNodes_,
        nodeId,
        TNodeInfo{
            .Jobs = {},
            .Lease = std::move(lease),
            .RegistrationId = registrationId,
            .NodeAddress = std::move(nodeAddress),
        });
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

        YT_LOG_DEBUG(
            "Updating node lease (NodeId: %v, NodeAddress: %v, RegistrationId: %v, DisconnectionTimeout: %v)",
            nodeId,
            nodeAddress,
            nodeInfo.RegistrationId,
            Config_->NodeDisconnectionTimeout);

        TLeaseManager::RenewLease(nodeInfo.Lease, Config_->NodeDisconnectionTimeout);

        return nodeIt->second;
    }
}

void TJobTracker::UnregisterNode(TNodeId nodeId, const TString& nodeAddress, TGuid maybeNodeRegistrationId)
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

    if (maybeNodeRegistrationId) {
        if (maybeNodeRegistrationId != nodeInfo.RegistrationId) {
            YT_LOG_DEBUG("Node unregistration skiped because of registration id mismatch "
                "(NodeId: %v, OldRegistrationId: %v, OldAddress: %v, ActualRegistrationId: %v, ActualAddress: %v)",
                nodeId,
                maybeNodeRegistrationId,
                nodeAddress,
                nodeInfo.RegistrationId,
                nodeInfo.NodeAddress);

            return;
        }
    }

    TLeaseManager::CloseLease(nodeInfo.Lease);

    const auto& nodeJobs = nodeInfo.Jobs;
    YT_VERIFY(nodeAddress == nodeInfo.NodeAddress);

    YT_LOG_DEBUG(
        "Unregistering node (NodeId: %v, NodeAddress: %v, RegistrationId: %v)",
        nodeId,
        nodeAddress,
        nodeInfo.RegistrationId);

    {
        TOperationIdToJobsToAbort jobIdsToAbort;
        THashMap<TOperationId, std::vector<TJobId>> finishedJobIds;

        nodeJobs.ForEachJob([&] <class TJobInfo> (TJobId jobId, TJobInfo jobInfo) {
            if (std::holds_alternative<TRunningJobInfo>(jobInfo.Info)) {
                auto abortReason = EAbortReason::NodeOffline;
                if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId));
                    it != std::end(nodeInfo.Jobs.AbortedAllocations))
                {
                    YT_LOG_DEBUG(
                        "Allocation aborted by scheduler; aborting job (NodeId: %v, JobId: %v, AllocationId: %v, OperationId: %v)",
                        nodeId,
                        jobId,
                        AllocationIdFromJobId(jobId),
                        jobInfo.OperationId);
                    abortReason = it->second.AbortReason;
                } else {
                    YT_LOG_DEBUG(
                        "Abort job since node unregistered (NodeId: %v, JobId: %v, OperationId: %v)",
                        nodeId,
                        jobId,
                        jobInfo.OperationId);
                }
                jobIdsToAbort[jobInfo.OperationId].push_back(TJobToAbort{
                    .JobId = jobId,
                    .AbortReason = abortReason,
                });
            } else {
                YT_LOG_DEBUG(
                    "Remove finished job from operation info since node unregistered (NodeId: %v, JobId: %v, OperationId: %v)",
                    nodeId,
                    jobId,
                    jobInfo.OperationId);
                finishedJobIds[jobInfo.OperationId].push_back(jobId);
            }
        });

        nodeJobs.ForEachJobToConfirm([&] (TJobId jobId, TJobToConfirmInfo jobToConfirmInfo) {
            auto abortReason = EAbortReason::NodeOffline;
            if (auto it = nodeJobs.AbortedAllocations.find(AllocationIdFromJobId(jobId));
                it != std::end(nodeInfo.Jobs.AbortedAllocations))
            {
                YT_LOG_DEBUG(
                    "Allocation aborted by scheduler; aborting unconfirmed job (NodeId: %v, JobId: %v, AllocationId: %v, OperationId: %v)",
                    nodeId,
                    jobId,
                    AllocationIdFromJobId(jobId),
                    jobToConfirmInfo.OperationId);
                abortReason = it->second.AbortReason;
            } else {
                YT_LOG_DEBUG(
                    "Abort unconfirmed job since node unregistered (NodeId: %v, JobId: %v, OperationId: %v)",
                    nodeId,
                    jobId,
                    jobToConfirmInfo.OperationId);
            }

            jobIdsToAbort[jobToConfirmInfo.OperationId].push_back(TJobToAbort{
                .JobId = jobId,
                .AbortReason = abortReason,
            });
        });

        for (const auto& [operationId, jobIds] : jobIdsToAbort) {
            auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

            for (auto jobId : jobIds) {
                EraseOrCrash(operationInfo.TrackedJobIds, jobId.JobId);
            }
        }
        for (const auto& [operationId, jobIds] : finishedJobIds) {
            auto& operationInfo = GetOrCrash(RegisteredOperations_, operationId);

            for (auto jobId : jobIds) {
                EraseOrCrash(operationInfo.TrackedJobIds, jobId);
            }
        }

        AbortJobs(std::move(jobIdsToAbort));
    }

    NodeUnregistrationCount_.Increment();

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

// NB(pogorelov): Sometimes nodeId or address may change.
// So we use registrationId to prevent new node unregistration on old lease expiration (CloseLease is racy).
void TJobTracker::OnNodeHeartbeatLeaseExpired(TGuid registrationId, TNodeId nodeId, const TString& nodeAddress)
{
    YT_LOG_DEBUG(
        "Node heartbeat lease expired, unregister node (NodeId: %v, NodeAddress: %v)",
        nodeId,
        nodeAddress);

    UnregisterNode(nodeId, nodeAddress, registrationId);
}

void TJobTracker::OnAllocationsAborted(
    TOperationId operationId,
    std::vector<TAbortedAllocationSummary> abortedAllocations)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (std::empty(abortedAllocations)) {
        return;
    }

    auto Logger = NControllerAgent::Logger.WithTag("OperationId: %v", operationId);

    auto logOperationIsNotRunningEvent = [&] (const auto& operationStatus) {
        YT_LOG_INFO(
            "Received aborted allocations of operation that is %v; ignore it"
            " (IncarnationId: %v, AbortedAllocationCount: %v)",
            operationStatus,
            IncarnationId_,
            std::size(abortedAllocations));
    };

    auto operationIt = RegisteredOperations_.find(operationId);
    if (operationIt == std::end(RegisteredOperations_)) {
        logOperationIsNotRunningEvent("not running");

        return;
    }

    const auto& operationInfo = operationIt->second;

    auto operationController = operationInfo.OperationController.Lock();
    if (!operationController) {
        logOperationIsNotRunningEvent("already finished");

        return;
    }

    YT_LOG_FATAL_UNLESS(
        operationInfo.JobsReady,
        "Unexpected allocation abort during revival (IncarnationId: %v, AllocationIds: %v)",
        IncarnationId_,
        [&] {
            std::vector<TAllocationId> allocationIds;
            for (const auto& abortedAllocationSummary : abortedAllocations) {
                allocationIds.push_back(abortedAllocationSummary.Id);
            }

            return allocationIds;
        }());

    std::vector<TAbortedAllocationSummary> emptyAllocations;
    emptyAllocations.reserve(std::size(abortedAllocations));

    for (auto& abortedAllocationSummary : abortedAllocations) {
        auto nodeId = NodeIdFromAllocationId(abortedAllocationSummary.Id);

        auto logSendingEventToOperationController = [&] (const auto& message) {
            YT_LOG_INFO(
                "%v; send this to operation controller"
                " (AllocationId: %v, NodeId: %v, NodeAddress: %v, AbortReason: %v, AbortionError: %v)",
                message,
                abortedAllocationSummary.Id,
                nodeId,
                GetNodeAddressForLogging(nodeId),
                abortedAllocationSummary.AbortReason,
                abortedAllocationSummary.Error);
        };

        if (!abortedAllocationSummary.Scheduled) {
            logSendingEventToOperationController("Scheduler aborted non scheduled allocation");
            emptyAllocations.push_back(std::move(abortedAllocationSummary));
            continue;
        }

        auto* nodeInfo = FindNodeInfo(nodeId);
        if (!nodeInfo) {
            logSendingEventToOperationController("Scheduler aborted allocation on node that is not registered");
            emptyAllocations.push_back(std::move(abortedAllocationSummary));
            continue;
        }

        auto allocationIt = nodeInfo->Jobs.Allocations.find(abortedAllocationSummary.Id);

        bool noConfirmedRunningJobsOnAllocation =
            allocationIt == std::end(nodeInfo->Jobs.Allocations) ||
            !allocationIt->second.RunningJob.has_value();

        if (noConfirmedRunningJobsOnAllocation &&
            !nodeInfo->Jobs.JobsToConfirm.contains(abortedAllocationSummary.Id))
        {
            logSendingEventToOperationController("Scheduler aborted empty allocation");
            emptyAllocations.push_back(std::move(abortedAllocationSummary));
            continue;
        }

        YT_LOG_INFO(
            "Scheduler aborted allocation on online node; postpone job abortion until node heartbeat"
            " (AllocationId: %v, NodeId: %v, NodeAddress: %v, AbortReason: %v, AbortionError: %v)",
            abortedAllocationSummary.Id,
            nodeId,
            GetNodeAddressForLogging(nodeId),
            abortedAllocationSummary.AbortReason,
            abortedAllocationSummary.Error);
        EmplaceOrCrash(
            nodeInfo->Jobs.AbortedAllocations,
            abortedAllocationSummary.Id,
            std::move(abortedAllocationSummary));
    }

    operationController->GetCancelableInvoker(JobEventsControllerQueue_)->Invoke(
        BIND([
            operationController,
            emptyAllocations = std::move(emptyAllocations),
            Logger = std::move(Logger)
        ] () mutable {
            for (auto& abortedAllocationSummary : emptyAllocations) {
                auto allocationId = abortedAllocationSummary.Id;
                try {
                    operationController->OnAllocationAborted(std::move(abortedAllocationSummary));
                } catch (const std::exception& ex) {
                    YT_LOG_FATAL(
                        ex,
                        "Failed to process aborted allocation in operation controller (AllocationId: %v)",
                        allocationId);
                }
            }
        }));
}

const TString& TJobTracker::GetNodeAddressForLogging(TNodeId nodeId)
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (auto nodeIt = ExecNodes_->find(nodeId); nodeIt == std::end(*ExecNodes_)) {
        static const TString NotReceivedAddress{"<address not received>"};

        return NotReceivedAddress;
    } else {
        return nodeIt->second->Address;
    }
}

void TJobTracker::AbortJobs(TOperationIdToJobsToAbort operationIdToJobsToAbort) const
{
    VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

    if (std::empty(operationIdToJobsToAbort)) {
        return;
    }

    for (const auto& [operationId, jobsToAbort] : operationIdToJobsToAbort) {
        auto operationIt = RegisteredOperations_.find(operationId);
        if (operationIt == std::end(RegisteredOperations_)) {
            YT_LOG_DEBUG(
                "Operation is not registered, skip jobs abortion (OperationId: %v)",
                operationId);

            continue;
        }

        const auto& operationInfo = operationIt->second;

        auto operationController = operationInfo.OperationController.Lock();
        if (!operationController) {
            YT_LOG_DEBUG(
                "Operation controller is already destroyed, skip jobs abortion (OperationId: %v)",
                operationId);
            continue;
        }

        operationController->GetCancelableInvoker(JobEventsControllerQueue_)->Invoke(
            BIND([operationController, jobsToAbort = std::move(jobsToAbort)] () mutable {
                for (auto jobToAbort : jobsToAbort) {
                    try {
                        operationController->AbortJobByJobTracker(jobToAbort.JobId, jobToAbort.AbortReason);
                    } catch (const std::exception& ex) {
                        YT_LOG_FATAL(
                            ex,
                            "Failed to abort job by job tracker request (JobId: %v)",
                            jobToAbort.JobId);
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

    std::vector<TJobToAbort> jobsToAbort;

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

        if (auto unconfirmedJobIt = nodeJobs.FindJobToConfirm(jobId))
        {
            auto operationId = unconfirmedJobIt->ConcreteJob->second.OperationId;
            if (nodeJobs.AbortedAllocations.erase(AllocationIdFromJobId(jobId))) {
                YT_LOG_DEBUG(
                    "Ignore allocation abort since job was not confirmed (JobId: %v, AllocationId: %v, OperationId: %v)",
                    jobId,
                    AllocationIdFromJobId(jobId),
                    operationId);
            } else {
                YT_LOG_DEBUG(
                    "Job was not confirmed within timeout, abort it (JobId: %v, OperationId: %v, NodeId: %v, NodeAddress: %v)",
                    jobId,
                    operationId,
                    nodeId,
                nodeAddress);
            }
            nodeJobs.EraseJobToConfirm(*unconfirmedJobIt);

            jobsToAbort.push_back(TJobToAbort{
                .JobId = jobId,
                .AbortReason = EAbortReason::RevivalConfirmationTimeout
            });
        }
    }

    for (auto jobToAbort : jobsToAbort) {
        auto& operationInfo = operationIt->second;

        EraseOrCrash(operationInfo.TrackedJobIds, jobToAbort.JobId);
    }

    TOperationIdToJobsToAbort grouppedJobsToAbort;
    grouppedJobsToAbort[operationId] = std::move(jobsToAbort);

    AbortJobs(std::move(grouppedJobsToAbort));
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
    TOperationId operationId)
    : JobTracker_(jobTracker)
    , CancelableInvoker_(std::move(cancelableInvoker))
    , OperationId_(operationId)
    , TraceContext_(CreateTraceContextFromCurrent("JobTrackerOperationHandler"))
    , TraceContextFinishGuard_(TraceContext_)
{
    auto operationIdString = ToString(operationId);
    TraceContext_->SetAllocationTags({{OperationIdTag, operationIdString}});
    TraceContext_->AddProfilingTag(OperationIdTag, operationIdString);
}

void TJobTrackerOperationHandler::RegisterJob(TStartedJobInfo jobInfo)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoRegisterJob,
        MakeStrong(JobTracker_),
        std::move(jobInfo),
        OperationId_));
}

void TJobTrackerOperationHandler::ReviveJobs(std::vector<TStartedJobInfo> jobs)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoReviveJobs,
        MakeStrong(JobTracker_),
        OperationId_,
        std::move(jobs)));
}

void TJobTrackerOperationHandler::ReleaseJobs(std::vector<TJobToRelease> jobs)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::DoReleaseJobs,
        MakeStrong(JobTracker_),
        OperationId_,
        std::move(jobs)));
}

void TJobTrackerOperationHandler::RequestJobAbortion(
    TJobId jobId,
    EAbortReason reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::RequestJobAbortion,
        MakeStrong(JobTracker_),
        jobId,
        OperationId_,
        reason));
}

void TJobTrackerOperationHandler::RequestJobInterruption(
    TJobId jobId,
    EInterruptReason reason,
    TDuration timeout)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::RequestJobInterruption,
        MakeStrong(JobTracker_),
        jobId,
        OperationId_,
        reason,
        timeout));
}

void TJobTrackerOperationHandler::RequestJobGracefulAbort(
    TJobId jobId,
    EAbortReason reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::RequestJobGracefulAbort,
        MakeStrong(JobTracker_),
        jobId,
        OperationId_,
        reason));
}

void TJobTrackerOperationHandler::OnAllocationsAborted(
    std::vector<TAbortedAllocationSummary> abortedAllocations)
{
    auto guard = TCurrentTraceContextGuard(TraceContext_);

    CancelableInvoker_->Invoke(BIND(
        &TJobTracker::OnAllocationsAborted,
        MakeStrong(JobTracker_),
        OperationId_,
        Passed(std::move(abortedAllocations))));
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
