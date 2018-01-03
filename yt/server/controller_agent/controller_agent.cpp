#include "controller_agent.h"
#include "operation_controller.h"
#include "master_connector.h"
#include "config.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/cache.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/ytlib/event_log/event_log.h>

#include <yt/ytlib/scheduler/controller_agent_service_proxy.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/ytree/convert.h>

#include <util/string/join.h>

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

////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerAgentLogger;

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

        MasterConnector_->SubscribeMasterConnected(BIND(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        MasterConnector_->SubscribeMasterDisconnected(BIND(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));
    }

    void ValidateConnected()
    {
        if (!MasterConnector_->IsConnected()) {
            THROW_ERROR_EXCEPTION(GetMasterDisconnectedError());
        }
    }

    TInstant GetConnectionTime() const
    {
        return MasterConnector_->GetConnectionTime();
    }

    const IInvokerPtr& GetInvoker()
    {
        return Bootstrap_->GetControllerAgentInvoker();
    }

    const IInvokerPtr& GetCancelableInvoker()
    {
        return CancelableInvoker_;
    }

    const IInvokerPtr& GetControllerThreadPoolInvoker()
    {
        return ControllerThreadPool_->GetInvoker();
    }

    const IInvokerPtr& GetSnapshotIOInvoker()
    {
        return SnapshotIOQueue_->GetInvoker();
    }

    TMasterConnector* GetMasterConnector()
    {
        return MasterConnector_.get();
    }

    const TControllerAgentConfigPtr& GetConfig() const
    {
        return Config_;
    }

    const NApi::INativeClientPtr& GetMasterClient() const
    {
        return Bootstrap_->GetMasterClient();
    }

    const TNodeDirectoryPtr& GetNodeDirectory()
    {
        return Bootstrap_->GetNodeDirectory();
    }

    const TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const
    {
        return ChunkLocationThrottlerManager_;
    }

    const TCoreDumperPtr& GetCoreDumper() const
    {
        return Bootstrap_->GetCoreDumper();
    }

    const TAsyncSemaphorePtr& GetCoreSemaphore() const
    {
        return CoreSemaphore_;
    }

    const TEventLogWriterPtr& GetEventLogWriter() const
    {
        return EventLogWriter_;
    }

    void UpdateConfig(const TControllerAgentConfigPtr& config)
    {
        Config_ = config;

        ChunkLocationThrottlerManager_->Reconfigure(Config_->ChunkLocationThrottler);
        EventLogWriter_->UpdateConfig(Config_->EventLog);
        SchedulerProxy_.SetDefaultTimeout(Config_->ControllerAgentHeartbeatRpcTimeout);
        if (MasterConnector_) {
            MasterConnector_->UpdateConfig(config);
        }

        for (auto pair : GetControllers()) {
            const auto& controller = pair.second;
            controller->GetCancelableInvoker()->Invoke(
                BIND(&IOperationController::UpdateConfig, controller, config));
        }
    }


    void RegisterController(const TOperationId& operationId, const IOperationControllerPtr& controller)
    {
        TWriterGuard guard(ControllerMapLock_);
        ControllerMap_.emplace(operationId, controller);
    }

    void UnregisterController(const TOperationId& operationId)
    {
        TWriterGuard guard(ControllerMapLock_);
        YCHECK(ControllerMap_.erase(operationId) == 1);
    }

    IOperationControllerPtr FindController(const TOperationId& operationId)
    {
        TReaderGuard guard(ControllerMapLock_);
        auto it = ControllerMap_.find(operationId);
        return it == ControllerMap_.end() ? nullptr : it->second;
    }

    TOperationIdToControllerMap GetControllers()
    {
        TReaderGuard guard(ControllerMapLock_);
        return ControllerMap_;
    }


    std::vector<TErrorOr<TSharedRef>> GetJobSpecs(const std::vector<std::pair<TOperationId, TJobId>>& jobSpecRequests)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControllerAgentInvoker());

        std::vector<TFuture<TSharedRef>> asyncJobSpecs;

        for (const auto& pair : jobSpecRequests) {
            const auto& operationId = pair.first;
            const auto& jobId = pair.second;

            LOG_DEBUG("Retrieving job spec (OperationId: %v, JobId: %v)",
                operationId,
                jobId);

            auto controller = FindController(operationId);

            if (!controller) {
                asyncJobSpecs.push_back(MakeFuture<TSharedRef>(TError("No such operation %v", operationId)));
                continue;
            }

            auto asyncJobSpec = BIND(&IOperationController::ExtractJobSpec,
                controller,
                jobId)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run();

            asyncJobSpecs.push_back(asyncJobSpec);
        }

        auto results = WaitFor(CombineAll(asyncJobSpecs))
            .ValueOrThrow();

        int index = 0;
        for (const auto& result : results) {
            if (!result.IsOK()) {
                const auto& jobId = jobSpecRequests[index].second;
                LOG_DEBUG(result, "Failed to extract job spec (JobId: %v)", jobId);
            }
            ++index;
        }

        return results;
    }

    void BuildOperationInfo(
        const TOperationId& operationId,
        NScheduler::NProto::TRspGetOperationInfo* response)
    {
        auto controller = FindController(operationId);
        if (!controller) {
            return;
        }

        auto asyncResult = BIND(&IOperationController::BuildOperationInfo, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run(response);
        WaitFor(asyncResult)
            .ThrowOnError();
    }

    TYsonString BuildJobInfo(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        auto controller = FindController(operationId);
        if (!controller) {
            THROW_ERROR_EXCEPTION("Operation %v is missing", operationId);
        }

        auto asyncResult = BIND(&IOperationController::BuildJobYson, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run(jobId, /* outputStatistics */ true);

        return WaitFor(asyncResult)
            .ValueOrThrow();
    }

    TFuture<void> GetHeartbeatSentFuture()
    {
        // In the a bit more far future this function will become unnecessary
        // because of changes in processing operation statuses.
        return HeartbeatExecutor_->GetExecutedEvent();
    }

    void AttachJobContext(
        const TYPath& path,
        const TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        MasterConnector_->AttachJobContext(path, chunkId, operationId, jobId);
    }

    TExecNodeDescriptorListPtr GetExecNodeDescriptors(const TSchedulingTagFilter& filter) const
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
        TReaderGuard guard(ExecNodeDescriptorsLock_);

        return CachedExecNodeDescriptors_->Descriptors.size();
    }

    void InterruptJob(const TJobId& jobId, EInterruptReason reason)
    {
        auto guard = Guard(HeartbeatRequestLock_);
        YCHECK(HeartbeatRequest_);
        auto* jobToInterrupt = HeartbeatRequest_->add_jobs_to_interrupt();
        ToProto(jobToInterrupt->mutable_job_id(), jobId);
        jobToInterrupt->set_reason(static_cast<int>(reason));
    }

    void AbortJob(const TJobId& jobId, const TError& error)
    {
        TGuard<TSpinLock> guard(HeartbeatRequestLock_);
        YCHECK(HeartbeatRequest_);
        auto* jobToAbort = HeartbeatRequest_->add_jobs_to_abort();
        ToProto(jobToAbort->mutable_job_id(), jobId);
        ToProto(jobToAbort->mutable_error(), error);
    }

    void FailJob(const TJobId& jobId)
    {
        auto guard = Guard(HeartbeatRequestLock_);
        YCHECK(HeartbeatRequest_);
        auto* jobToFail = HeartbeatRequest_->add_jobs_to_fail();
        ToProto(jobToFail->mutable_job_id(), jobId);
    }

    void ReleaseJobs(
        std::vector<TJobId> jobIds,
        const TOperationId& operationId,
        int controllerSchedulerIncarnation)
    {
        auto guard = Guard(HeartbeatRequestLock_);
        YCHECK(HeartbeatRequest_);
        auto* jobsToRelease = HeartbeatRequest_->add_jobs_to_release();
        ToProto(jobsToRelease->mutable_job_ids(), jobIds);
        ToProto(jobsToRelease->mutable_operation_id(), operationId);
        jobsToRelease->set_controller_scheduler_incarnation(controllerSchedulerIncarnation);
    }

private:
    TControllerAgentConfigPtr Config_;
    NCellScheduler::TBootstrap* const Bootstrap_;

    const TThreadPoolPtr ControllerThreadPool_;
    const TActionQueuePtr SnapshotIOQueue_;
    const TThrottlerManagerPtr ChunkLocationThrottlerManager_;
    const TAsyncSemaphorePtr CoreSemaphore_;
    const TEventLogWriterPtr EventLogWriter_;
    const std::unique_ptr<TMasterConnector> MasterConnector_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    TReaderWriterSpinLock ControllerMapLock_;
    TOperationIdToControllerMap ControllerMap_;

    TReaderWriterSpinLock ExecNodeDescriptorsLock_;
    TExecNodeDescriptorListPtr CachedExecNodeDescriptors_ = New<TExecNodeDescriptorList>();
    TIntrusivePtr<TExpiringCache<TSchedulingTagFilter, TExecNodeDescriptorListPtr>> CachedExecNodeDescriptorsByTags_;

    TControllerAgentServiceProxy SchedulerProxy_;

    TCpuInstant LastExecNodesUpdateTime_ = TCpuInstant();

    TSpinLock HeartbeatRequestLock_;
    TControllerAgentServiceProxy::TReqHeartbeatPtr HeartbeatRequest_;

    TPeriodicExecutorPtr HeartbeatExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CancelableContext_ = New<TCancelableContext>();
        CancelableInvoker_ = CancelableContext_->CreateInvoker(GetInvoker());

        {
            auto guard = Guard(HeartbeatRequestLock_);
            PrepareHeartbeatRequest();
        }

        CachedExecNodeDescriptorsByTags_ = New<TExpiringCache<TSchedulingTagFilter, TExecNodeDescriptorListPtr>>(
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

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& pair : GetControllers()) {
            const auto& operationId = pair.first;
            const auto& controller = pair.second;

            LOG_INFO("Forgetting operation (OperationId: %v)", operationId);
            controller->Forget();
        }

        CancelableContext_->Cancel();

        CachedExecNodeDescriptorsByTags_->Stop();

        HeartbeatExecutor_->Stop();
    }

    // TODO: Move this method to some common place to avoid copy/paste.
    TError GetMasterDisconnectedError()
    {
        return TError(
            NRpc::EErrorCode::Unavailable,
            "Master is not connected");
    }

    void PrepareHeartbeatRequest()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        VERIFY_SPINLOCK_AFFINITY(HeartbeatRequestLock_);

        HeartbeatRequest_ = SchedulerProxy_.Heartbeat();
        ToProto(HeartbeatRequest_->mutable_agent_incarnation_id(), MasterConnector_->GetIncarnationId());
    }

    void SendHeartbeat()
    {
        TControllerAgentServiceProxy::TReqHeartbeatPtr req;
        {
            auto guard = Guard(HeartbeatRequestLock_);
            req = HeartbeatRequest_;
            PrepareHeartbeatRequest();
        }

        auto controllers = GetControllers();
        for (const auto& pair : controllers) {
            const auto& operationId = pair.first;
            const auto& controller = pair.second;

            auto event = controller->PullEvent();
            switch (event.Tag()) {
                case TOperationControllerEvent::TagOf<TNullOperationEvent>():
                    break;

                case TOperationControllerEvent::TagOf<TOperationCompletedEvent>(): {
                    ToProto(req->add_completed_operation_ids(), operationId);
                    break;
                }

                case TOperationControllerEvent::TagOf<TOperationAbortedEvent>(): {
                    const auto& typedEvent = event.As<TOperationAbortedEvent>();
                    auto* proto = req->add_aborted_operations();
                    ToProto(proto->mutable_operation_id(), operationId);
                    ToProto(proto->mutable_error(), typedEvent.Error);
                    break;
                }

                case TOperationControllerEvent::TagOf<TOperationFailedEvent>(): {
                    const auto& typedEvent = event.As<TOperationFailedEvent>();
                    auto failedOperationProto = req->add_failed_operations();
                    ToProto(failedOperationProto->mutable_operation_id(), operationId);
                    ToProto(failedOperationProto->mutable_error(), typedEvent.Error);
                    break;
                }

                case TOperationControllerEvent::TagOf<TOperationSuspendedEvent>(): {
                    const auto& typedEvent = event.As<TOperationSuspendedEvent>();
                    auto* proto = req->add_suspended_operations();
                    ToProto(proto->mutable_operation_id(), operationId);
                    ToProto(proto->mutable_error(), typedEvent.Error);
                    break;
                }

                default:
                    Y_UNREACHABLE();
            }

            {
                auto jobMetricsDelta = controller->PullJobMetricsDelta();
                ToProto(req->add_job_metrics(), jobMetricsDelta);
            }


            {
                auto* operationAlertsProto = req->add_operation_alerts();
                ToProto(operationAlertsProto->mutable_operation_id(), operationId);
                for (const auto& pair : controller->GetAlerts()) {
                    auto alertType = pair.first;
                    const auto& alert = pair.second;
                    auto* protoAlert = operationAlertsProto->add_alerts();
                    protoAlert->set_type(static_cast<int>(alertType));
                    ToProto(protoAlert->mutable_error(), alert);
                }
            }
        }

        auto now = GetCpuInstant();
        bool shouldRequestExecNodes = LastExecNodesUpdateTime_ + DurationToCpuDuration(Config_->ExecNodesRequestPeriod) < now;
        req->set_exec_nodes_requested(shouldRequestExecNodes);

        // TODO(ignat): add some backoff.
        {
            std::vector<TString> suspiciousJobsYsons;
            for (const auto& pair : controllers) {
                const auto& controller = pair.second;
                suspiciousJobsYsons.push_back(controller->GetSuspiciousJobsYson().GetData());
            }
            req->set_suspicious_jobs(JoinSeq("", suspiciousJobsYsons));
        }

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NRpc::EErrorCode::Unavailable)) {
                LOG_DEBUG(rspOrError, "Scheduler is currently unavailable; retrying heartbeat");
            } else {
                LOG_WARNING(rspOrError, "Error reporting heratbeat to scheduler; disconnecting");
                // TODO(ignat): this class is not ready from disconnection inside! Fix it!!!
                //Disconnect();
            }
            return;
        }

        const auto& rsp = rspOrError.Value();

        if (rsp->has_exec_nodes()) {
            auto execNodeDescriptors = New<TExecNodeDescriptorList>();
            FromProto(&execNodeDescriptors->Descriptors, rsp->exec_nodes().exec_nodes());

            {
                TWriterGuard guard(ExecNodeDescriptorsLock_);
                std::swap(CachedExecNodeDescriptors_, execNodeDescriptors);
            }

            LastExecNodesUpdateTime_ = now;
        }
    }

    // TODO(ignat): eliminate this copy/paste from scheduler.cpp somehow.
    TExecNodeDescriptorListPtr CalculateExecNodeDescriptors(const TSchedulingTagFilter& filter) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeDescriptorsLock_);

        auto result = New<TExecNodeDescriptorList>();
        for (const auto& descriptor : CachedExecNodeDescriptors_->Descriptors) {
            if (filter.CanSchedule(descriptor.Tags)) {
                result->Descriptors.push_back(descriptor);
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

const IInvokerPtr& TControllerAgent::GetInvoker()
{
    return Impl_->GetInvoker();
}

const IInvokerPtr& TControllerAgent::GetCancelableInvoker()
{
    return Impl_->GetCancelableInvoker();
}

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

const NApi::INativeClientPtr& TControllerAgent::GetMasterClient() const
{
    return Impl_->GetMasterClient();
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

void TControllerAgent::RegisterController(const TOperationId& operationId, const IOperationControllerPtr& controller)
{
    Impl_->RegisterController(operationId, controller);
}

void TControllerAgent::UnregisterController(const TOperationId& operationId)
{
    Impl_->UnregisterController(operationId);
}

IOperationControllerPtr TControllerAgent::FindController(const TOperationId& operationId)
{
    return Impl_->FindController(operationId);
}

yhash<TOperationId, IOperationControllerPtr> TControllerAgent::GetControllers()
{
    return Impl_->GetControllers();
}

std::vector<TErrorOr<TSharedRef>> TControllerAgent::GetJobSpecs(const std::vector<std::pair<TOperationId, TJobId>>& jobSpecRequests)
{
    return Impl_->GetJobSpecs(jobSpecRequests);
}

void TControllerAgent::BuildOperationInfo(const TOperationId& operationId, NScheduler::NProto::TRspGetOperationInfo* response)
{
    Impl_->BuildOperationInfo(operationId, response);
}

TYsonString TControllerAgent::BuildJobInfo(
    const TOperationId& operationId,
    const TJobId& jobId)
{
    return Impl_->BuildJobInfo(operationId, jobId);
}

TFuture<void> TControllerAgent::GetHeartbeatSentFuture()
{
    return Impl_->GetHeartbeatSentFuture();
}

int TControllerAgent::GetExecNodeCount() const
{
    return Impl_->GetExecNodeCount();
}

TExecNodeDescriptorListPtr TControllerAgent::GetExecNodeDescriptors(const TSchedulingTagFilter& filter) const
{
    return Impl_->GetExecNodeDescriptors(filter);
}

void TControllerAgent::AttachJobContext(
    const TYPath& path,
    const TChunkId& chunkId,
    const TOperationId& operationId,
    const TJobId& jobId)
{
    Impl_->AttachJobContext(path, chunkId, operationId, jobId);
}

void TControllerAgent::InterruptJob(const TJobId& jobId, EInterruptReason reason)
{
    Impl_->InterruptJob(jobId, reason);
}

void TControllerAgent::AbortJob(const TJobId& jobId, const TError& error)
{
    Impl_->AbortJob(jobId, error);
}

void TControllerAgent::FailJob(const TJobId& jobId)
{
    Impl_->FailJob(jobId);
}

void TControllerAgent::ReleaseJobs(
    std::vector<TJobId> jobIds,
    const TOperationId& operationId,
    int controllerSchedulerIncarnation)
{
    Impl_->ReleaseJobs(std::move(jobIds), operationId, controllerSchedulerIncarnation);
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
