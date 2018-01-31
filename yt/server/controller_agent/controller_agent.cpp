#include "controller_agent.h"
#include "operation_controller.h"
#include "master_connector.h"
#include "config.h"
#include "private.h"
#include "operation_controller_host.h"
#include "operation.h"
#include "scheduling_context.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/cache.h>
#include <yt/server/scheduler/operation.h>
#include <yt/server/scheduler/message_queue.h>
#include <yt/server/scheduler/job.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/event_log/event_log.h>

#include <yt/ytlib/scheduler/controller_agent_tracker_service_proxy.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;
using namespace NCellScheduler;
using namespace NConcurrency;
using namespace NYTree;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NEventLog;
using namespace NProfiling;
using namespace NYson;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////

struct TAgentToSchedulerScheduleJobResponse
{
    TJobId JobId;
    TScheduleJobResultPtr Result;
};

////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public ISchedulingContext
{
public:
    TSchedulingContext(
        const NScheduler::NProto::TScheduleJobRequest* request,
        const TExecNodeDescriptor& nodeDescriptor)
        : ResourceLimits_(FromProto<TJobResources>(request->node_resource_limits()))
        , DiskInfo_(request->node_disk_info())
        , JobId_(FromProto<TJobId>(request->job_id()))
        , NodeDescriptor_(nodeDescriptor)
    { }

    virtual const TExecNodeDescriptor& GetNodeDescriptor() const override
    {
        return NodeDescriptor_;
    }

    virtual const TJobResources& ResourceLimits() const override
    {
        return ResourceLimits_;
    }

    virtual const NNodeTrackerClient::NProto::TDiskResources& DiskInfo() const override
    {
        return DiskInfo_;
    }

    virtual TJobId GetJobId() const override
    {
        return JobId_;
    }

    virtual NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }

private:
    const TJobResources ResourceLimits_;
    const NNodeTrackerClient::NProto::TDiskResources& DiskInfo_;
    const TJobId JobId_;
    const TExecNodeDescriptor& NodeDescriptor_;
};

////////////////////////////////////////////////////////////////////

class TControllerAgent::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TControllerAgentConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ControllerThreadPool_(New<TThreadPool>(Config_->ControllerThreadCount, "Controller"))
        , SnapshotIOQueue_(New<TActionQueue>("SnapshotIO"))
        , ChunkLocationThrottlerManager_(New<TThrottlerManager>(
            Config_->ChunkLocationThrottler,
            ControllerAgentLogger))
        , ReconfigurableJobSpecSliceThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->JobSpecSliceThrottler,
            NLogging::TLogger(),
            NProfiling::TProfiler(ControllerAgentProfiler.GetPathPrefix() + "/job_spec_slice_throttler")))
        , JobSpecSliceThrottler_(ReconfigurableJobSpecSliceThrottler_)
        , CoreSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentSafeCoreDumps))
        , EventLogWriter_(New<TEventLogWriter>(
            Config_->EventLog,
            Bootstrap_->GetMasterClient(),
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity)))
        , MasterConnector_(std::make_unique<TMasterConnector>(
            Config_,
            Bootstrap_))
        , SchedulerProxy_(Bootstrap_->GetLocalRpcChannel())
    {
        SchedulerProxy_.SetDefaultTimeout(Config_->ControllerAgentHeartbeatRpcTimeout);

        MasterConnector_->SubscribeMasterConnecting(BIND(
            &TImpl::OnMasterConnecting,
            Unretained(this)));
        MasterConnector_->SubscribeMasterConnected(BIND(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        MasterConnector_->SubscribeMasterDisconnected(BIND(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));
    }

    void ValidateConnected()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!MasterConnector_->IsConnected()) {
            THROW_ERROR_EXCEPTION(GetMasterDisconnectedError());
        }
    }

    TInstant GetConnectionTime() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return MasterConnector_->GetConnectionTime();
    }

    const IInvokerPtr& GetCancelableInvoker()
    {
        return CancelableInvoker_;
    }

    const IInvokerPtr& GetControllerThreadPoolInvoker()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ControllerThreadPool_->GetInvoker();
    }

    const IInvokerPtr& GetSnapshotIOInvoker()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SnapshotIOQueue_->GetInvoker();
    }

    TMasterConnector* GetMasterConnector()
    {
        return MasterConnector_.get();
    }

    const TControllerAgentConfigPtr& GetConfig() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_;
    }

    const NApi::INativeClientPtr& GetClient() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetMasterClient();
    }

    const TNodeDirectoryPtr& GetNodeDirectory()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetNodeDirectory();
    }

    const TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ChunkLocationThrottlerManager_;
    }

    const TCoreDumperPtr& GetCoreDumper() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCoreDumper();
    }

    const TAsyncSemaphorePtr& GetCoreSemaphore() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CoreSemaphore_;
    }

    const TEventLogWriterPtr& GetEventLogWriter() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return EventLogWriter_;
    }

    void UpdateConfig(const TControllerAgentConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Config_ = config;

        ChunkLocationThrottlerManager_->Reconfigure(Config_->ChunkLocationThrottler);

        EventLogWriter_->UpdateConfig(Config_->EventLog);

        SchedulerProxy_.SetDefaultTimeout(Config_->ControllerAgentHeartbeatRpcTimeout);

        ReconfigurableJobSpecSliceThrottler_->Reconfigure(Config_->JobSpecSliceThrottler);

        if (HeartbeatExecutor_) {
            HeartbeatExecutor_->SetPeriod(Config_->ControllerAgentHeartbeatPeriod);
        }

        if (MasterConnector_) {
            MasterConnector_->UpdateConfig(config);
        }

        for (const auto& pair : GetOperations()) {
            const auto& operation = pair.second;
            auto controller = operation->GetController();
            controller->GetCancelableInvoker()->Invoke(
                BIND(&IOperationController::UpdateConfig, controller, config));
        }
    }


    TOperationPtr CreateOperation(const NScheduler::TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agentOperation = New<NControllerAgent::TOperation>(operation.Get());
        auto host = New<TOperationControllerHost>(
            agentOperation.Get(),
            CancelableInvoker_,
            OperationEventsOutbox_,
            JobEventsOutbox_,
            Bootstrap_);
        agentOperation->SetHost(host);
        return agentOperation;
    }

    void RegisterOperation(const TOperationId& operationId, const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(IdToOperation_.emplace(operationId, operation).second);
        LOG_DEBUG("Operation registered (OperationId: %v)", operationId);
    }

    void UnregisterOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(IdToOperation_.erase(operationId) == 1);
        LOG_DEBUG("Operation unregistered (OperationId: %v)", operationId);
    }

    TOperationPtr FindOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = IdToOperation_.find(operationId);
        return it == IdToOperation_.end() ? nullptr : it->second;
    }

    TOperationPtr GetOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);
        YCHECK(operation);

        return operation;
    }

    TOperationPtr GetOperationOrThrow(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);
        if (!operation) {
            THROW_ERROR_EXCEPTION("No such operation %v", operationId);
        }
        return operation;
    }

    const TOperationIdToOperationMap& GetOperations()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return IdToOperation_;
    }


    TFuture<std::vector<TErrorOr<TSharedRef>>> ExtractJobSpecs(const std::vector<TJobSpecRequest>& requests)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TFuture<TSharedRef>> asyncJobSpecs;
        for (const auto& request : requests) {
            LOG_DEBUG("Extracting job spec (OperationId: %v, JobId: %v)",
                request.OperationId,
                request.JobId);

            auto operation = FindOperation(request.OperationId);
            if (!operation) {
                asyncJobSpecs.push_back(MakeFuture<TSharedRef>(TError("No such operation %v",
                    request.OperationId)));
                continue;
            }

            auto controller = operation->GetController();
            auto asyncJobSpec = BIND(&IOperationController::ExtractJobSpec,
                controller,
                request.JobId)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run();

            asyncJobSpecs.push_back(asyncJobSpec);
        }

        return CombineAll(asyncJobSpecs);
    }

    TFuture<TOperationInfo> BuildOperationInfo(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controller = GetOperationOrThrow(operationId)->GetController();
        return BIND(&IOperationController::BuildOperationInfo, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TFuture<TYsonString> BuildJobInfo(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controller = GetOperationOrThrow(operationId)->GetController();
        return BIND(&IOperationController::BuildJobYson, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run(jobId, /* outputStatistics */ true);
    }

    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const TSchedulingTagFilter& filter) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (filter.IsEmpty()) {
            TReaderGuard guard(ExecNodeDescriptorsLock_);
            return CachedExecNodeDescriptors_;
        }

        return CachedExecNodeDescriptorsByTags_->Get(filter);
    }

    int GetExecNodeCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeDescriptorsLock_);
        return static_cast<int>(CachedExecNodeDescriptors_->size());
    }

    const IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobSpecSliceThrottler_;
    }

private:
    TControllerAgentConfigPtr Config_;
    NCellScheduler::TBootstrap* const Bootstrap_;

    const TThreadPoolPtr ControllerThreadPool_;
    const TActionQueuePtr SnapshotIOQueue_;
    const TThrottlerManagerPtr ChunkLocationThrottlerManager_;
    const IReconfigurableThroughputThrottlerPtr ReconfigurableJobSpecSliceThrottler_;
    const IThroughputThrottlerPtr JobSpecSliceThrottler_;
    const TAsyncSemaphorePtr CoreSemaphore_;
    const TEventLogWriterPtr EventLogWriter_;
    const std::unique_ptr<TMasterConnector> MasterConnector_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    TOperationIdToOperationMap IdToOperation_;

    TReaderWriterSpinLock ExecNodeDescriptorsLock_;
    TRefCountedExecNodeDescriptorMapPtr CachedExecNodeDescriptors_ = New<TRefCountedExecNodeDescriptorMap>();
    TIntrusivePtr<TExpiringCache<TSchedulingTagFilter, TRefCountedExecNodeDescriptorMapPtr>> CachedExecNodeDescriptorsByTags_;

    TControllerAgentTrackerServiceProxy SchedulerProxy_;

    TInstant LastExecNodesUpdateTime_;

    TIncarnationId IncarnationId_;

    TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerOperationEvent>> OperationEventsOutbox_;
    TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerJobEvent>> JobEventsOutbox_;
    TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerScheduleJobResponse>> ScheduleJobResposesOutbox_;

    std::unique_ptr<NScheduler::TMessageQueueInbox> JobEventsInbox_;
    std::unique_ptr<NScheduler::TMessageQueueInbox> OperationEventsInbox_;
    std::unique_ptr<NScheduler::TMessageQueueInbox> ScheduleJobRequestsInbox_;

    TPeriodicExecutorPtr HeartbeatExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void OnMasterConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        CancelableContext_ = New<TCancelableContext>();
        // TODO(babenko): better queue
        CancelableInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker(EControlQueue::Default));

        IncarnationId_ = MasterConnector_->GetIncarnationId();

        OperationEventsOutbox_ = New<NScheduler::TMessageQueueOutbox<TAgentToSchedulerOperationEvent>>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: AgentToSchedulerOperations, IncarnationId: %v", IncarnationId_));
        JobEventsOutbox_ = New<NScheduler::TMessageQueueOutbox<TAgentToSchedulerJobEvent>>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: AgentToSchedulerJobs, IncarnationId: %v", IncarnationId_));
        ScheduleJobResposesOutbox_ = New<NScheduler::TMessageQueueOutbox<TAgentToSchedulerScheduleJobResponse>>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: AgentToSchedulerScheduleJobResponses, IncarnationId: %v", IncarnationId_));

        JobEventsInbox_ = std::make_unique<NScheduler::TMessageQueueInbox>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: SchedulerToAgentJobs, IncarnationId: %v", IncarnationId_));
        OperationEventsInbox_ = std::make_unique<NScheduler::TMessageQueueInbox>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: SchedulerToAgentOperation, IncarnationId: %v", IncarnationId_));
        ScheduleJobRequestsInbox_ = std::make_unique<NScheduler::TMessageQueueInbox>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: SchedulerToAgentScheduleJobRequests, IncarnationId: %v", IncarnationId_));
    }

    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CachedExecNodeDescriptorsByTags_ = New<TExpiringCache<TSchedulingTagFilter, TRefCountedExecNodeDescriptorMapPtr>>(
            BIND(&TImpl::CalculateExecNodeDescriptors, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout,
            GetCancelableInvoker());
        CachedExecNodeDescriptorsByTags_->Start();

        HeartbeatExecutor_ = New<TPeriodicExecutor>(
            CancelableInvoker_,
            BIND(&TControllerAgent::TImpl::SendHeartbeat, MakeWeak(this)),
            Config_->ControllerAgentHeartbeatPeriod);
        HeartbeatExecutor_->Start();
    }

    void DoCleanup()
    {
        for (const auto& pair : IdToOperation_) {
            const auto& operation = pair.second;
            auto controller = operation->GetController();
            controller->Cancel();
        }
        IdToOperation_.clear();

        if (CancelableContext_) {
            CancelableContext_->Cancel();
            CancelableContext_.Reset();
        }

        if (CachedExecNodeDescriptorsByTags_) {
            CachedExecNodeDescriptorsByTags_->Stop();
            CachedExecNodeDescriptorsByTags_.Reset();
        }

        if (HeartbeatExecutor_) {
            HeartbeatExecutor_->Stop();
            HeartbeatExecutor_.Reset();
        }

        IncarnationId_ = {};

        OperationEventsOutbox_.Reset();
        JobEventsOutbox_.Reset();
        ScheduleJobResposesOutbox_.Reset();

        JobEventsInbox_.reset();
        OperationEventsInbox_.reset();
        ScheduleJobRequestsInbox_.reset();
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoCleanup();
    }

    // TODO: Move this method to some common place to avoid copy/paste.
    TError GetMasterDisconnectedError()
    {
        return TError(
            NRpc::EErrorCode::Unavailable,
            "Master is not connected");
    }

    TControllerAgentTrackerServiceProxy::TReqHeartbeatPtr PrepareHeartbeatRequest()
    {
        auto req = SchedulerProxy_.Heartbeat();

        ToProto(req->mutable_agent_incarnation_id(), IncarnationId_);

        OperationEventsOutbox_->BuildOutcoming(
            req->mutable_agent_to_scheduler_operation_events(),
            [] (auto* protoEvent, const auto& event) {
                protoEvent->set_event_type(static_cast<int>(event.EventType));
                ToProto(protoEvent->mutable_operation_id(), event.OperationId);
                switch (event.EventType) {
                    case EAgentToSchedulerOperationEventType::Completed:
                        break;
                    case EAgentToSchedulerOperationEventType::Aborted:
                    case EAgentToSchedulerOperationEventType::Failed:
                    case EAgentToSchedulerOperationEventType::Suspended:
                        ToProto(protoEvent->mutable_error(), event.Error);
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            });

        JobEventsOutbox_->BuildOutcoming(
            req->mutable_agent_to_scheduler_job_events(),
            [] (auto* protoEvent, const auto& event) {
                protoEvent->set_event_type(static_cast<int>(event.EventType));
                ToProto(protoEvent->mutable_job_id(), event.JobId);
                if (event.InterruptReason) {
                    protoEvent->set_interrupt_reason(static_cast<int>(*event.InterruptReason));
                }
                if (!event.Error.IsOK()) {
                    ToProto(protoEvent->mutable_error(), event.Error);
                }
            });

        ScheduleJobResposesOutbox_->BuildOutcoming(
            req->mutable_agent_to_scheduler_schedule_job_responses(),
            [] (auto* protoResponse, const auto& response) {
                const auto& scheduleJobResult = *response.Result;
                ToProto(protoResponse->mutable_job_id(), response.JobId);
                if (scheduleJobResult.StartDescriptor) {
                    const auto& startDescriptor = *scheduleJobResult.StartDescriptor;
                    Y_ASSERT(response.JobId == startDescriptor.Id);
                    protoResponse->set_job_type(static_cast<int>(startDescriptor.Type));
                    ToProto(protoResponse->mutable_resource_limits(), startDescriptor.ResourceLimits);
                    protoResponse->set_interruptible(startDescriptor.Interruptible);
                }
                protoResponse->set_duration(ToProto<i64>(scheduleJobResult.Duration));
                for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
                    if (scheduleJobResult.Failed[reason] > 0) {
                        auto* protoCounter = protoResponse->add_failed();
                        protoCounter->set_reason(static_cast<int>(reason));
                        protoCounter->set_value(scheduleJobResult.Failed[reason]);
                    }
                }
            });

        JobEventsInbox_->ReportStatus(req->mutable_scheduler_to_agent_job_events());
        OperationEventsInbox_->ReportStatus(req->mutable_scheduler_to_agent_operation_events());
        ScheduleJobRequestsInbox_->ReportStatus(req->mutable_scheduler_to_agent_schedule_job_responses());

        for (const auto& pair : GetOperations()) {
            const auto& operationId = pair.first;
            const auto& operation = pair.second;
            auto controller = operation->GetController();

            auto* protoOperation = req->add_operations();
            ToProto(protoOperation->mutable_operation_id(), operationId);

            {
                auto jobMetricsDelta = controller->PullJobMetricsDelta();
                ToProto(protoOperation->mutable_job_metrics(), jobMetricsDelta);
            }

            for (const auto& pair : controller->GetAlerts()) {
                auto alertType = pair.first;
                const auto& alert = pair.second;
                auto* protoAlert = protoOperation->add_alerts();
                protoAlert->set_type(static_cast<int>(alertType));
                ToProto(protoAlert->mutable_error(), alert);
            }

            // TODO(ignat): add some backoff.
            protoOperation->set_suspicious_jobs(controller->GetSuspiciousJobsYson().GetData());
            protoOperation->set_pending_job_count(controller->GetPendingJobCount());
            ToProto(protoOperation->mutable_needed_resources(), controller->GetNeededResources());
            ToProto(protoOperation->mutable_min_needed_job_resources(), controller->GetMinNeededJobResources());
        }

        {
            auto now = TInstant::Now();
            bool shouldRequestExecNodes = LastExecNodesUpdateTime_ + Config_->ExecNodesRequestPeriod < now;
            req->set_exec_nodes_requested(shouldRequestExecNodes);
        }

        return req;
    }

    void SendHeartbeat()
    {
        LOG_INFO("Sending heartbeat");

        auto req = PrepareHeartbeatRequest();
        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Heartbeat failed; backing off and retrying");
            Y_UNUSED(WaitFor(TDelayedExecutor::MakeDelayed(Config_->ControllerAgentHeartbeatFailureBackoff)));
            return;
        }

        LOG_INFO("Heartbeat succeeded");
        const auto& rsp = rspOrError.Value();

        OperationEventsOutbox_->HandleStatus(rsp->agent_to_scheduler_operation_events());
        JobEventsOutbox_->HandleStatus(rsp->agent_to_scheduler_job_events());
        ScheduleJobResposesOutbox_->HandleStatus(rsp->agent_to_scheduler_schedule_job_responses());

        HandleJobEvents(rsp);
        HandleOperationEvents(rsp);
        HandleScheduleJobRequests(rsp, GetExecNodeDescriptors({}));

        if (rsp->has_exec_nodes()) {
            auto execNodeDescriptors = New<TRefCountedExecNodeDescriptorMap>();
            for (const auto& protoDescriptor : rsp->exec_nodes().exec_nodes()) {
                YCHECK(execNodeDescriptors->emplace(
                    protoDescriptor.node_id(),
                    FromProto<TExecNodeDescriptor>(protoDescriptor)).second);
            }

            {
                TWriterGuard guard(ExecNodeDescriptorsLock_);
                std::swap(CachedExecNodeDescriptors_, execNodeDescriptors);
            }

            LastExecNodesUpdateTime_ = TInstant::Now();
        }
    }

    void HandleJobEvents(const TControllerAgentTrackerServiceProxy::TRspHeartbeatPtr& rsp)
    {
        THashMap<TOperationPtr, std::vector<NScheduler::NProto::TSchedulerToAgentJobEvent*>> groupedJobEvents;
        JobEventsInbox_->HandleIncoming(
            rsp->mutable_scheduler_to_agent_job_events(),
            [&] (auto* protoEvent) {
                auto operationId = FromProto<TOperationId>(protoEvent->operation_id());
                auto operation = this->FindOperation(operationId);
                if (!operation) {
                    return;
                }
                groupedJobEvents[operation].push_back(protoEvent);
            });

        for (auto& pair : groupedJobEvents) {
            const auto& operation = pair.first;
            auto controller = operation->GetController();
            controller->GetCancelableInvoker()->Invoke(
                BIND([rsp, controller, this_ = MakeStrong(this), protoEvents = std::move(pair.second)] {
                    for (auto* protoEvent : protoEvents) {
                        auto eventType = static_cast<ESchedulerToAgentJobEventType>(protoEvent->event_type());
                        switch (eventType) {
                            case ESchedulerToAgentJobEventType::Started:
                                controller->OnJobStarted(std::make_unique<TStartedJobSummary>(protoEvent));
                                break;
                            case ESchedulerToAgentJobEventType::Completed:
                                controller->OnJobCompleted(std::make_unique<TCompletedJobSummary>(protoEvent));
                                break;
                            case ESchedulerToAgentJobEventType::Failed:
                                controller->OnJobFailed(std::make_unique<TFailedJobSummary>(protoEvent));
                                break;
                            case ESchedulerToAgentJobEventType::Aborted:
                                controller->OnJobAborted(std::make_unique<TAbortedJobSummary>(protoEvent));
                                break;
                            case ESchedulerToAgentJobEventType::Running:
                                controller->OnJobRunning(std::make_unique<TRunningJobSummary>(protoEvent));
                                break;
                            default:
                                Y_UNREACHABLE();
                        }
                    }
                }));
        }
    }

    void HandleOperationEvents(const TControllerAgentTrackerServiceProxy::TRspHeartbeatPtr& rsp)
    {
        OperationEventsInbox_->HandleIncoming(
            rsp->mutable_scheduler_to_agent_operation_events(),
            [&] (const auto* protoEvent) {
                auto eventType = static_cast<ESchedulerToAgentOperationEventType>(protoEvent->event_type());
                auto operationId = FromProto<TOperationId>(protoEvent->operation_id());
                auto operation = this->FindOperation(operationId);
                if (!operation) {
                    return;
                }

                switch (eventType) {
                    case ESchedulerToAgentOperationEventType::Abandon:
                        // TODO(babenko)
                        break;

                    case ESchedulerToAgentOperationEventType::UpdateMinNeededJobResources:
                        operation->GetController()->UpdateMinNeededJobResources();
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            });
    }

    void HandleScheduleJobRequests(
        const TControllerAgentTrackerServiceProxy::TRspHeartbeatPtr& rsp,
        const TRefCountedExecNodeDescriptorMapPtr& execNodeDescriptors)
    {
        auto outbox = ScheduleJobResposesOutbox_;

        auto replyWithFailure = [=] (const TJobId& jobId, EScheduleJobFailReason reason) {
            TAgentToSchedulerScheduleJobResponse response;
            response.JobId = jobId;
            response.Result = New<TScheduleJobResult>();
            response.Result->RecordFail(EScheduleJobFailReason::UnknownNode);
            outbox->Enqueue(std::move(response));
        };

        ScheduleJobRequestsInbox_->HandleIncoming(
            rsp->mutable_scheduler_to_agent_schedule_job_requests(),
            [&] (auto* protoRequest) {
                auto jobId = FromProto<TJobId>(protoRequest->job_id());
                auto operationId = FromProto<TOperationId>(protoRequest->operation_id());
                auto operation = this->FindOperation(operationId);
                if (!operation) {
                    replyWithFailure(jobId, EScheduleJobFailReason::UnknownOperation);
                    LOG_DEBUG("Failed to schedule job due to unknown operation (OperationId: %v, JobId: %v)",
                        operationId,
                        jobId);
                    return;
                }

                auto controller = operation->GetController();
                GuardedInvoke(
                    controller->GetCancelableInvoker(),
                    BIND([=, this_ = MakeStrong(this)] {
                        auto nodeId = NodeIdFromJobId(jobId);
                        auto descriptorIt = execNodeDescriptors->find(nodeId);
                        if (descriptorIt == execNodeDescriptors->end()) {
                            replyWithFailure(jobId, EScheduleJobFailReason::UnknownNode);
                            LOG_DEBUG("Failed to schedule job due to unknown node (OperationId: %v, JobId: %v, NodeId: %v)",
                                operationId,
                                jobId,
                                nodeId);
                            return;
                        }

                        auto jobLimits = FromProto<TJobResourcesWithQuota>(protoRequest->job_resource_limits());
                        const auto& treeId = protoRequest->tree_id();

                        TAgentToSchedulerScheduleJobResponse response;
                        TSchedulingContext context(protoRequest, descriptorIt->second);
                        response.JobId = jobId;
                        response.Result = controller->ScheduleJob(
                            &context,
                            jobLimits,
                            treeId);

                        outbox->Enqueue(std::move(response));
                        LOG_DEBUG("Job schedule response enqueued (OperationId: %v, JobId: %v)",
                            operationId,
                            jobId);
                    }),
                    BIND([=, this_ = MakeStrong(this)] {
                        replyWithFailure(jobId, EScheduleJobFailReason::UnknownOperation);
                        LOG_DEBUG("Failed to schedule job due to operation cancelation (OperationId: %v, JobId: %v)",
                            operationId,
                            jobId);
                    }));
            });
    }


    // TODO(ignat): eliminate this copy/paste from scheduler.cpp somehow.
    TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(const TSchedulingTagFilter& filter) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeDescriptorsLock_);

        auto result = New<TRefCountedExecNodeDescriptorMap>();
        for (const auto& pair : *CachedExecNodeDescriptors_) {
            auto nodeId = pair.first;
            const auto& descriptor = pair.second;
            if (filter.CanSchedule(descriptor.Tags)) {
                YCHECK(result->emplace(nodeId, descriptor).second);
            }
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(
    TControllerAgentConfigPtr config,
    NCellScheduler::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TControllerAgent::~TControllerAgent() = default;

const IInvokerPtr& TControllerAgent::GetControllerThreadPoolInvoker()
{
    return Impl_->GetControllerThreadPoolInvoker();
}

const IInvokerPtr& TControllerAgent::GetSnapshotIOInvoker()
{
    return Impl_->GetSnapshotIOInvoker();
}

TMasterConnector* TControllerAgent::GetMasterConnector()
{
    return Impl_->GetMasterConnector();
}

void TControllerAgent::ValidateConnected() const
{
    Impl_->ValidateConnected();
}

TInstant TControllerAgent::GetConnectionTime() const
{
    return Impl_->GetConnectionTime();
}

const TControllerAgentConfigPtr& TControllerAgent::GetConfig() const
{
    return Impl_->GetConfig();
}

const NApi::INativeClientPtr& TControllerAgent::GetClient() const
{
    return Impl_->GetClient();
}

const TNodeDirectoryPtr& TControllerAgent::GetNodeDirectory()
{
    return Impl_->GetNodeDirectory();
}

const TThrottlerManagerPtr& TControllerAgent::GetChunkLocationThrottlerManager() const
{
    return Impl_->GetChunkLocationThrottlerManager();
}

const TCoreDumperPtr& TControllerAgent::GetCoreDumper() const
{
    return Impl_->GetCoreDumper();
}

const TAsyncSemaphorePtr& TControllerAgent::GetCoreSemaphore() const
{
    return Impl_->GetCoreSemaphore();
}

const TEventLogWriterPtr& TControllerAgent::GetEventLogWriter() const
{
    return Impl_->GetEventLogWriter();
}

void TControllerAgent::UpdateConfig(const TControllerAgentConfigPtr& config)
{
    Impl_->UpdateConfig(config);
}

TOperationPtr TControllerAgent::CreateOperation(const NScheduler::TOperationPtr& operation)
{
    return Impl_->CreateOperation(operation);
}

void TControllerAgent::RegisterOperation(const TOperationId& operationId, const TOperationPtr& operation)
{
    Impl_->RegisterOperation(operationId, operation);
}

void TControllerAgent::UnregisterOperation(const TOperationId& operationId)
{
    Impl_->UnregisterOperation(operationId);
}

TOperationPtr TControllerAgent::FindOperation(const TOperationId& operationId)
{
    return Impl_->FindOperation(operationId);
}

TOperationPtr TControllerAgent::GetOperation(const TOperationId& operationId)
{
    return Impl_->GetOperation(operationId);
}

TOperationPtr TControllerAgent::GetOperationOrThrow(const TOperationId& operationId)
{
    return Impl_->GetOperationOrThrow(operationId);
}

const TOperationIdToOperationMap& TControllerAgent::GetOperations()
{
    return Impl_->GetOperations();
}

TFuture<std::vector<TErrorOr<TSharedRef>>> TControllerAgent::ExtractJobSpecs(
    const std::vector<TJobSpecRequest>& requests)
{
    return Impl_->ExtractJobSpecs(requests);
}

TFuture<TOperationInfo> TControllerAgent::BuildOperationInfo(const TOperationId& operationId)
{
    return Impl_->BuildOperationInfo(operationId);
}

TFuture<TYsonString> TControllerAgent::BuildJobInfo(
    const TOperationId& operationId,
    const TJobId& jobId)
{
    return Impl_->BuildJobInfo(operationId, jobId);
}

int TControllerAgent::GetExecNodeCount() const
{
    return Impl_->GetExecNodeCount();
}

TRefCountedExecNodeDescriptorMapPtr TControllerAgent::GetExecNodeDescriptors(const TSchedulingTagFilter& filter) const
{
    return Impl_->GetExecNodeDescriptors(filter);
}

const IThroughputThrottlerPtr& TControllerAgent::GetJobSpecSliceThrottler() const
{
    return Impl_->GetJobSpecSliceThrottler();
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
