#include "controller_agent.h"
#include "operation_controller.h"
#include "master_connector.h"
#include "config.h"
#include "private.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/cache.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/ytlib/event_log/event_log.h>

#include <yt/ytlib/scheduler/controller_agent_tracker_service_proxy.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/actions/cancelable_context.h>

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
        VERIFY_THREAD_AFFINITY(ControlThread);

        ControllerMap_.emplace(operationId, controller);
    }

    void UnregisterController(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(ControllerMap_.erase(operationId) == 1);
    }

    IOperationControllerPtr FindController(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = ControllerMap_.find(operationId);
        return it == ControllerMap_.end() ? nullptr : it->second;
    }

    IOperationControllerPtr GetControllerOrThrow(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controller = FindController(operationId);
        if (!controller) {
            THROW_ERROR_EXCEPTION("No such operation %v", operationId);
        }
        return controller;
    }

    TOperationIdToControllerMap GetControllers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return ControllerMap_;
    }


    TFuture<std::vector<TErrorOr<TSharedRef>>> ExtractJobSpecs(const std::vector<TJobSpecRequest>& requests)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TFuture<TSharedRef>> asyncJobSpecs;
        for (const auto& request : requests) {
            LOG_DEBUG("Retrieving job spec (OperationId: %v, JobId: %v)",
                request.OperationId,
                request.JobId);

            auto controller = FindController(request.OperationId);
            if (!controller) {
                asyncJobSpecs.push_back(MakeFuture<TSharedRef>(TError("No such operation %v",
                    request.OperationId)));
                continue;
            }

            auto asyncJobSpec = BIND(&IOperationController::ExtractJobSpec,
                controller,
                request.JobId)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run();

            asyncJobSpecs.push_back(asyncJobSpec);
        }

        return CombineAll(asyncJobSpecs);
        //int index = 0;
        //for (const auto& result : jobSpecs) {
        //    if (!result.IsOK()) {
        //        const auto& jobId = jobSpecRequests[index].second;
        //        LOG_DEBUG(result, "Failed to extract job spec (JobId: %v)", jobId);
        //    }
        //    ++index;
        //}
        //
        //return jobSpecs;
    }

    TFuture<TOperationInfo> BuildOperationInfo(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controller = GetControllerOrThrow(operationId);
        return BIND(&IOperationController::BuildOperationInfo, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TFuture<TYsonString> BuildJobInfo(
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controller = GetControllerOrThrow(operationId);
        return BIND(&IOperationController::BuildJobYson, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run(jobId, /* outputStatistics */ true);
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
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeDescriptorsLock_);
        return static_cast<int>(CachedExecNodeDescriptors_->Descriptors.size());
    }

    void InterruptJob(const TIncarnationId& incarnationId, const TJobId& jobId, EInterruptReason reason)
    {
        PopulateHeartbeatRequest(
            incarnationId,
            [&] (const auto& request) {
                auto* jobToInterrupt = request->add_jobs_to_interrupt();
                ToProto(jobToInterrupt->mutable_job_id(), jobId);
                jobToInterrupt->set_reason(static_cast<int>(reason));
            });
    }

    void AbortJob(const TIncarnationId& incarnationId, const TJobId& jobId, const TError& error)
    {
        PopulateHeartbeatRequest(
            incarnationId,
            [&] (const auto& request) {
                auto* jobToAbort = request->add_jobs_to_abort();
                ToProto(jobToAbort->mutable_job_id(), jobId);
                ToProto(jobToAbort->mutable_error(), error);
            });
    }

    void FailJob(const TIncarnationId& incarnationId, const TJobId& jobId)
    {
        PopulateHeartbeatRequest(
            incarnationId,
            [&] (const auto& request) {
                auto* jobToFail = request->add_jobs_to_fail();
                ToProto(jobToFail->mutable_job_id(), jobId);
            });
    }

    void ReleaseJobs(const TIncarnationId& incarnationId, const std::vector<TJobId>& jobIds)
    {
        PopulateHeartbeatRequest(
            incarnationId,
            [&] (const auto& request) {
                for (const auto& jobId : jobIds) {
                    auto* jobToRelease = request->add_jobs_to_release();
                    ToProto(jobToRelease->mutable_job_id(), jobId);
                }
            });
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

    TOperationIdToControllerMap ControllerMap_;

    TReaderWriterSpinLock ExecNodeDescriptorsLock_;
    TExecNodeDescriptorListPtr CachedExecNodeDescriptors_ = New<TExecNodeDescriptorList>();
    TIntrusivePtr<TExpiringCache<TSchedulingTagFilter, TExecNodeDescriptorListPtr>> CachedExecNodeDescriptorsByTags_;

    TControllerAgentTrackerServiceProxy SchedulerProxy_;

    TCpuInstant LastExecNodesUpdateTime_ = TCpuInstant();

    TSpinLock HeartbeatRequestLock_;
    TIncarnationId HeartbeatIncarnationId_;
    TControllerAgentTrackerServiceProxy::TReqHeartbeatPtr HeartbeatRequest_;

    TPeriodicExecutorPtr HeartbeatExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CancelableContext_ = New<TCancelableContext>();
        // TODO(babenko): better queue
        CancelableInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker(EControlQueue::Default));

        {
            auto guard = Guard(HeartbeatRequestLock_);
            HeartbeatIncarnationId_ = MasterConnector_->GetIncarnationId();
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

        {
            auto guard = Guard(HeartbeatRequestLock_);
            HeartbeatIncarnationId_ = {};
            HeartbeatRequest_.Reset();
        }
    }

    // TODO: Move this method to some common place to avoid copy/paste.
    TError GetMasterDisconnectedError()
    {
        return TError(
            NRpc::EErrorCode::Unavailable,
            "Master is not connected");
    }

    template <class F>
    void PopulateHeartbeatRequest(const TIncarnationId& incarnationId, F callback)
    {
        auto guard = Guard(HeartbeatRequestLock_);
        if (HeartbeatIncarnationId_ != incarnationId) {
            return;
        }
        YCHECK(HeartbeatRequest_);
        callback(HeartbeatRequest_);
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
        TControllerAgentTrackerServiceProxy::TReqHeartbeatPtr req;
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
                    auto* proto = req->add_completed_operations();
                    ToProto(proto->mutable_operation_id(), operationId);
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

void TControllerAgent::InterruptJob(const TIncarnationId& incarnationId, const TJobId& jobId, EInterruptReason reason)
{
    Impl_->InterruptJob(incarnationId, jobId, reason);
}

void TControllerAgent::AbortJob(const TIncarnationId& incarnationId, const TJobId& jobId, const TError& error)
{
    Impl_->AbortJob(incarnationId, jobId, error);
}

void TControllerAgent::FailJob(const TIncarnationId& incarnationId, const TJobId& jobId)
{
    Impl_->FailJob(incarnationId, jobId);
}

void TControllerAgent::ReleaseJobs(const TIncarnationId& incarnationId, const std::vector<TJobId>& jobIds)
{
    Impl_->ReleaseJobs(incarnationId, jobIds);
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
