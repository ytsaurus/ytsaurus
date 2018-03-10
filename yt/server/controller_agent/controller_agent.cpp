#include "controller_agent.h"
#include "operation_controller.h"
#include "master_connector.h"
#include "config.h"
#include "private.h"
#include "operation_controller_host.h"
#include "operation.h"
#include "scheduling_context.h"
#include "memory_tag_queue.h"
#include "bootstrap.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/sync_expiring_cache.h>
#include <yt/server/scheduler/message_queue.h>
#include <yt/server/scheduler/exec_node.h>
#include <yt/server/scheduler/helpers.h>
#include <yt/server/scheduler/controller_agent_tracker_service_proxy.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/event_log/event_log.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;
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
        TSchedulerConfigPtr config, // TODO(babenko): config
        TBootstrap* bootstrap)
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
            Bootstrap_->GetControlInvoker()))
        , MasterConnector_(std::make_unique<TMasterConnector>(
            Config_,
            Bootstrap_))
        , SchedulerProxy_(Bootstrap_->GetMasterClient()->GetSchedulerChannel())
    { }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->Initialize();
        ScheduleConnect(true);
    }

    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto staticOrchidProducer = BIND(&TImpl::BuildStaticOrchid, MakeStrong(this));
        return IYPathService::FromProducer(staticOrchidProducer)
            ->Via(Bootstrap_->GetControlInvoker())
            ->Cached(Config_->StaticOrchidCacheUpdatePeriod);
    }

    bool IsConnected() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Connected_;
    }

    TInstant GetConnectionTime() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ConnectionTime_.load();
    }

    const TIncarnationId& GetIncarnationId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return IncarnationId_;
    }

    void ValidateConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Connected_) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Scheduler is not connected");
        }
    }

    void ValidateIncarnation(const TIncarnationId& incarnationId) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IncarnationId_ != incarnationId) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Invalid incarnation: expected %v, actual %v",
                incarnationId,
                IncarnationId_);
        }
    }

    void Disconnect(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoDisconnect(error);
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
        VERIFY_THREAD_AFFINITY_ANY();

        return MasterConnector_.get();
    }

    const TSchedulerConfigPtr& GetConfig() const // TODO(babenko): config
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_;
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


    TOperationPtr FindOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        auto it = IdToOperation_.find(operationId);
        return it == IdToOperation_.end() ? nullptr : it->second;
    }

    TOperationPtr GetOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        auto operation = FindOperation(operationId);
        YCHECK(operation);

        return operation;
    }

    TOperationPtr GetOperationOrThrow(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        auto operation = FindOperation(operationId);
        if (!operation) {
            THROW_ERROR_EXCEPTION("No such operation %v", operationId);
        }
        return operation;
    }

    const TOperationIdToOperationMap& GetOperations()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        return IdToOperation_;
    }


    void RegisterOperation(const NProto::TOperationDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        auto operation = New<TOperation>(descriptor);
        const auto& operationId = operation->GetId();
        auto host = New<TOperationControllerHost>(
            operation.Get(),
            CancelableControlInvoker_,
            OperationEventsOutbox_,
            JobEventsOutbox_,
            Bootstrap_);
        operation->SetHost(host);

        operation->SetMemoryTag(MemoryTagQueue_.AssignTagToOperation(operationId));

        try {
            auto controller = CreateControllerForOperation(Config_, operation.Get());
            operation->SetController(controller);
        } catch (...) {
            MemoryTagQueue_.ReclaimOperationTag(operationId);
            throw;
        }

        YCHECK(IdToOperation_.emplace(operationId, operation).second);

        MasterConnector_->StartOperationNodeUpdates(operationId);

        LOG_DEBUG("Operation registered (OperationId: %v)", operationId);
    }

    void UnregisterOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        const auto& controller = operation->GetController();
        if (controller) {
            controller->Cancel();
        }

        YCHECK(IdToOperation_.erase(operation->GetId()) == 1);

        LOG_DEBUG("Operation unregistered (OperationId: %v)", operation->GetId());

        MemoryTagQueue_.ReclaimOperationTag(operation->GetId());
    }

    TFuture<TOperationControllerInitializationResult> InitializeOperation(
        const TOperationPtr& operation,
        const TNullable<TControllerTransactions>& transactions)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        auto callback = transactions
            ? BIND(&IOperationControllerSchedulerHost::InitializeReviving, controller, *transactions)
            : BIND(&IOperationControllerSchedulerHost::InitializeClean, controller);
        return callback
            .AsyncVia(controller->GetCancelableInvoker())
            .Run()
            .Apply(BIND([=] (const TOperationControllerInitializationResult& result) {
                operation->SetTransactions(result.Transactions);
                return result;
            }).AsyncVia(GetCurrentInvoker()));
    }

    TFuture<TOperationControllerPrepareResult> PrepareOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        return BIND(&IOperationControllerSchedulerHost::Prepare, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TFuture<void> MaterializeOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        return BIND(&IOperationControllerSchedulerHost::Materialize, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TFuture<TOperationControllerReviveResult> ReviveOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        return BIND(&IOperationControllerSchedulerHost::Revive, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TFuture<void> CommitOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        return BIND(&IOperationControllerSchedulerHost::Commit, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TFuture<void> CompleteOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        operation->SetTransactions({});

        const auto& controller = operation->GetControllerOrThrow();
        return BIND(&IOperationControllerSchedulerHost::Complete, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TFuture<void> AbortOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        operation->SetTransactions({});

        const auto& controller = operation->GetController();
        if (!controller) {
            LOG_DEBUG("No controller to abort (OperationId: %v)",
                operation->GetId());
            return VoidFuture;
        }

        controller->Cancel();
        return BIND(&IOperationControllerSchedulerHost::Abort, controller)
            .AsyncVia(controller->GetInvoker())
            .Run();
    }

    TFuture<void> DisposeOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

        const auto& controller = operation->GetController();
        if (!controller) {
            LOG_DEBUG("No controller to dispose (OperationId: %v)",
                operation->GetId());
            return VoidFuture;
        }

        return BIND(&IOperationControllerSchedulerHost::Dispose, controller)
            .AsyncVia(controller->GetInvoker())
            .Run();
    }


    TFuture<std::vector<TErrorOr<TSharedRef>>> ExtractJobSpecs(const std::vector<TJobSpecRequest>& requests)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected_);

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
        YCHECK(Connected_);

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
        YCHECK(Connected_);

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

    DEFINE_SIGNAL(void(), SchedulerConnecting);
    DEFINE_SIGNAL(void(), SchedulerConnected);
    DEFINE_SIGNAL(void(), SchedulerDisconnected);

private:
    TSchedulerConfigPtr Config_; // TODO(babenko): config
    TBootstrap* const Bootstrap_;

    const TThreadPoolPtr ControllerThreadPool_;
    const TActionQueuePtr SnapshotIOQueue_;
    const TThrottlerManagerPtr ChunkLocationThrottlerManager_;
    const IReconfigurableThroughputThrottlerPtr ReconfigurableJobSpecSliceThrottler_;
    const IThroughputThrottlerPtr JobSpecSliceThrottler_;
    const TAsyncSemaphorePtr CoreSemaphore_;
    const TEventLogWriterPtr EventLogWriter_;
    const std::unique_ptr<TMasterConnector> MasterConnector_;

    bool Connected_= false;
    bool ConnectScheduled_ = false;
    std::atomic<TInstant> ConnectionTime_ = {TInstant::Zero()};
    TIncarnationId IncarnationId_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableControlInvoker_;

    TOperationIdToOperationMap IdToOperation_;

    TReaderWriterSpinLock ExecNodeDescriptorsLock_;
    TRefCountedExecNodeDescriptorMapPtr CachedExecNodeDescriptors_ = New<TRefCountedExecNodeDescriptorMap>();
    TIntrusivePtr<TSyncExpiringCache<TSchedulingTagFilter, TRefCountedExecNodeDescriptorMapPtr>> CachedExecNodeDescriptorsByTags_;

    TControllerAgentTrackerServiceProxy SchedulerProxy_;

    TInstant LastExecNodesUpdateTime_;
    TInstant LastConfigUpdateTime_;
    TInstant LastOperationAlertsUpdateTime_;

    TIntrusivePtr<TMessageQueueOutbox<TAgentToSchedulerOperationEvent>> OperationEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TAgentToSchedulerJobEvent>> JobEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TAgentToSchedulerScheduleJobResponse>> ScheduleJobResposesOutbox_;

    std::unique_ptr<TMessageQueueInbox> JobEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> OperationEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> ScheduleJobRequestsInbox_;

    TPeriodicExecutorPtr HeartbeatExecutor_;

    TMemoryTagQueue MemoryTagQueue_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void ScheduleConnect(bool immediate)
    {
        if (ConnectScheduled_) {
            return;
        }

        ConnectScheduled_ = true;
        TDelayedExecutor::Submit(
            BIND(&TImpl::DoConnect, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()),
            immediate ? TDuration::Zero() : Config_->ControllerAgentHandshakeFailureBackoff);
    }

    void DoConnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(ConnectScheduled_);
        ConnectScheduled_ = false;

        try {
            OnConnecting();
            SyncClusterDirectory();
            PerformHandshake();
            OnConnected();
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Error connecting to scheduler");
            DoCleanup();
            ScheduleConnect(false);
        }
    }

    void OnConnecting()
    {
        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        LOG_INFO("Connecting to scheduler");

        YCHECK(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();
        CancelableControlInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker());

        SwitchTo(CancelableControlInvoker_);

        SchedulerConnecting_.Fire();
    }

    void SyncClusterDirectory()
    {
        LOG_INFO("Synchronizing cluster directory");

        WaitFor(Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetClusterDirectorySynchronizer()
            ->Sync())
            .ThrowOnError();

        LOG_INFO("Cluster directory synchronized");
    }

    void PerformHandshake()
    {
        LOG_INFO("Sending handshake");

        auto req = SchedulerProxy_.Handshake();
        req->SetTimeout(Config_->ControllerAgentHandshakeRpcTimeout);
        req->set_agent_id(Bootstrap_->GetAgentId());
        ToProto(req->mutable_agent_addresses(), Bootstrap_->GetLocalAddresses());

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        LOG_DEBUG("Handshake succeeded");

        IncarnationId_ = FromProto<TIncarnationId>(rsp->incarnation_id());
        // TODO(babenko): config
        UpdateConfig(ConvertTo<TSchedulerConfigPtr>(TYsonString(rsp->config())));
    }

    void OnConnected()
    {
        Connected_ = true;

        LOG_INFO("Scheduler connected (IncarnationId: %v)",
            IncarnationId_);

        OperationEventsOutbox_ = New<TMessageQueueOutbox<TAgentToSchedulerOperationEvent>>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: AgentToSchedulerOperations, IncarnationId: %v", IncarnationId_));
        JobEventsOutbox_ = New<TMessageQueueOutbox<TAgentToSchedulerJobEvent>>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: AgentToSchedulerJobs, IncarnationId: %v", IncarnationId_));
        ScheduleJobResposesOutbox_ = New<TMessageQueueOutbox<TAgentToSchedulerScheduleJobResponse>>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: AgentToSchedulerScheduleJobResponses, IncarnationId: %v", IncarnationId_));

        JobEventsInbox_ = std::make_unique<TMessageQueueInbox>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: SchedulerToAgentJobs, IncarnationId: %v", IncarnationId_));
        OperationEventsInbox_ = std::make_unique<TMessageQueueInbox>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: SchedulerToAgentOperations, IncarnationId: %v", IncarnationId_));
        ScheduleJobRequestsInbox_ = std::make_unique<TMessageQueueInbox>(
            NLogging::TLogger(ControllerAgentLogger)
                .AddTag("Kind: SchedulerToAgentScheduleJobRequests, IncarnationId: %v", IncarnationId_));

        CachedExecNodeDescriptorsByTags_ = New<TSyncExpiringCache<TSchedulingTagFilter, TRefCountedExecNodeDescriptorMapPtr>>(
            BIND(&TImpl::FilterExecNodes, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout);

        HeartbeatExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TControllerAgent::TImpl::SendHeartbeat, MakeWeak(this)),
            Config_->ControllerAgentHeartbeatPeriod);
        HeartbeatExecutor_->Start();

        SchedulerConnected_.Fire();
    }

    void DoDisconnect(const TError& error) noexcept
    {
        TForbidContextSwitchGuard contextSwitchGuard;

        if (Connected_) {
            LOG_WARNING(error, "Disconnecting scheduler");

            SchedulerDisconnected_.Fire();

            LOG_WARNING("Scheduler disconnected");
        }

        DoCleanup();

        ScheduleConnect(true);
    }

    void DoCleanup()
    {
        Connected_ = false;
        ConnectionTime_.store(TInstant::Zero());
        IncarnationId_ = {};

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
        CancelableControlInvoker_.Reset();

        if (CachedExecNodeDescriptorsByTags_) {
            CachedExecNodeDescriptorsByTags_.Reset();
        }

        if (HeartbeatExecutor_) {
            HeartbeatExecutor_->Stop();
            HeartbeatExecutor_.Reset();
        }

        OperationEventsOutbox_.Reset();
        JobEventsOutbox_.Reset();
        ScheduleJobResposesOutbox_.Reset();

        JobEventsInbox_.reset();
        OperationEventsInbox_.reset();
        ScheduleJobRequestsInbox_.reset();

        MemoryTagQueue_.Reset();
    }

    TControllerAgentTrackerServiceProxy::TReqHeartbeatPtr PrepareHeartbeatRequest(
        bool* execNodesRequested,
        bool* configRequested,
        bool* operationAlertsSent)
    {
        auto req = SchedulerProxy_.Heartbeat();
        req->SetTimeout(Config_->ControllerAgentHeartbeatRpcTimeout);
        req->SetHeavy(true);
        req->set_agent_id(Bootstrap_->GetAgentId());
        ToProto(req->mutable_incarnation_id(), IncarnationId_);

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
        ScheduleJobRequestsInbox_->ReportStatus(req->mutable_scheduler_to_agent_schedule_job_requests());

        auto now = TInstant::Now();
        *execNodesRequested = LastExecNodesUpdateTime_ + Config_->ExecNodesUpdatePeriod < now;
        *configRequested = LastConfigUpdateTime_ + Config_->ConfigUpdatePeriod < now;
        *operationAlertsSent = LastOperationAlertsUpdateTime_ + Config_->OperationAlertsUpdatePeriod < now;

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

            if (*operationAlertsSent) {
                auto* protoAlerts = protoOperation->mutable_alerts();
                for (const auto& pair : controller->GetAlerts()) {
                    auto alertType = pair.first;
                    const auto& alert = pair.second;
                    auto* protoAlert = protoAlerts->add_alerts();
                    protoAlert->set_type(static_cast<int>(alertType));
                    ToProto(protoAlert->mutable_error(), alert);
                }
            }

            // XXX(babenko): avoid sending on each heartbeat
            protoOperation->set_suspicious_jobs(controller->GetSuspiciousJobsYson().GetData());
            protoOperation->set_pending_job_count(controller->GetPendingJobCount());
            ToProto(protoOperation->mutable_needed_resources(), controller->GetNeededResources());
            ToProto(protoOperation->mutable_min_needed_job_resources(), controller->GetMinNeededJobResources());
        }

        req->set_exec_nodes_requested(*execNodesRequested);
        req->set_config_requested(*configRequested);

        return req;
    }

    void SendHeartbeat()
    {
        bool execNodesRequested;
        bool configRequested;
        bool operationAlertsSent;
        auto req = PrepareHeartbeatRequest(
            &execNodesRequested,
            &configRequested,
            &operationAlertsSent);

        LOG_DEBUG("Sending heartbeat (ExecNodesRequested: %v, ConfigRequested: %v, OperationAlertsSent: %v)",
            execNodesRequested,
            configRequested,
            operationAlertsSent);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            if (NRpc::IsRetriableError(rspOrError)) {
                LOG_WARNING(rspOrError, "Error reporting heartbeat to scheduler");
                Y_UNUSED(WaitFor(TDelayedExecutor::MakeDelayed(Config_->ControllerAgentHeartbeatFailureBackoff)));
            } else {
                Disconnect(rspOrError);
            }
            return;
        }

        LOG_DEBUG("Heartbeat succeeded");
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
            LOG_DEBUG("Exec node descriptors updated");
        }

        if (rsp->has_config()) {
            auto config = ConvertTo<TSchedulerConfigPtr>(TYsonString(rsp->config())); // TODO(babenko): config
            UpdateConfig(config);
        }

        for (const auto& protoOperationId : rsp->operation_ids_to_unregister()) {
            auto operationId = FromProto<TOperationId>(protoOperationId);
            auto operation = FindOperation(operationId);
            if (!operation) {
                LOG_DEBUG("Requested to unregister an unknown operation; ignored (OperationId: %v)",
                    operationId);
                continue;
            }
            UnregisterOperation(operation);
        }

        auto now = TInstant::Now();
        if (execNodesRequested) {
            LastExecNodesUpdateTime_ = now;
        }
        if (configRequested) {
            LastConfigUpdateTime_ = now;
        }
        if (operationAlertsSent) {
            LastOperationAlertsUpdateTime_ = now;
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
                    BIND([=, rsp = rsp, this_ = MakeStrong(this)] {
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
                        if (!response.Result) {
                            response.Result = New<TScheduleJobResult>();
                        }

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
    TRefCountedExecNodeDescriptorMapPtr FilterExecNodes(const TSchedulingTagFilter& filter) const
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

        LOG_DEBUG("Exec nodes filtered (Formula: %v, MatchingNodeCount: %v)",
            filter.GetBooleanFormula().GetFormula(),
            result->size());

        return result;
    }

    void UpdateConfig(const TSchedulerConfigPtr& config) // TODO(babenko): config
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto oldConfigNode = ConvertToNode(Config_);
        auto newConfigNode = ConvertToNode(config);
        if (AreNodesEqual(oldConfigNode, newConfigNode)) {
            return;
        }

        Config_ = config;

        ChunkLocationThrottlerManager_->Reconfigure(Config_->ChunkLocationThrottler);

        EventLogWriter_->UpdateConfig(Config_->EventLog);

        ReconfigurableJobSpecSliceThrottler_->Reconfigure(Config_->JobSpecSliceThrottler);

        if (HeartbeatExecutor_) {
            HeartbeatExecutor_->SetPeriod(Config_->ControllerAgentHeartbeatPeriod);
        }

        if (MasterConnector_) {
            MasterConnector_->UpdateConfig(config);
        }

        for (const auto& pair : IdToOperation_) {
            const auto& operation = pair.second;
            auto controller = operation->GetController();
            controller->GetCancelableInvoker()->Invoke(
                BIND(&IOperationController::UpdateConfig, controller, config));
        }

        LOG_INFO("Configuration updated");
    }

    void BuildStaticOrchid(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("connected").Value(Connected_)
                .DoIf(Connected_, [&] (TFluentMap fluent) {
                    fluent
                        .Item("incarnation_id").Value(IncarnationId_)
                        .Item("operations").DoMapFor(IdToOperation_, [&] (TFluentMap fluent, const auto& pair) {
                            const auto& operation = pair.second;
                            fluent
                                .Item(ToString(pair.first)).BeginMap()
                                .Item("type").Value(operation->GetType())
                                .Item("spec").Value(operation->GetSpec())
                                .EndMap();
                        });
                })
                .Item("config").Value(Config_)
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(
    TSchedulerConfigPtr config, // TODO(babenko): config
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TControllerAgent::~TControllerAgent() = default;

void TControllerAgent::Initialize()
{
    Impl_->Initialize();
}

IYPathServicePtr TControllerAgent::GetOrchidService()
{
    return Impl_->GetOrchidService();
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

bool TControllerAgent::IsConnected() const
{
    return Impl_->IsConnected();
}

const TIncarnationId& TControllerAgent::GetIncarnationId() const
{
    return Impl_->GetIncarnationId();
}

TInstant TControllerAgent::GetConnectionTime() const
{
    return Impl_->GetConnectionTime();
}

void TControllerAgent::ValidateConnected() const
{
    Impl_->ValidateConnected();
}

void TControllerAgent::ValidateIncarnation(const TIncarnationId& incarnationId) const
{
    Impl_->ValidateIncarnation(incarnationId);
}

void TControllerAgent::Disconnect(const TError& error)
{
    Impl_->Disconnect(error);
}

const TSchedulerConfigPtr& TControllerAgent::GetConfig() const // TODO(babenko): config
{
    return Impl_->GetConfig();
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

void TControllerAgent::RegisterOperation(const NProto::TOperationDescriptor& descriptor)
{
    Impl_->RegisterOperation(descriptor);
}

TFuture<TOperationControllerInitializationResult> TControllerAgent::InitializeOperation(
    const TOperationPtr& operation,
    const TNullable<TControllerTransactions>& transactions)
{
    return Impl_->InitializeOperation(
        operation,
        transactions);
}

TFuture<TOperationControllerPrepareResult> TControllerAgent::PrepareOperation(const TOperationPtr& operation)
{
    return Impl_->PrepareOperation(operation);
}

TFuture<void> TControllerAgent::MaterializeOperation(const TOperationPtr& operation)
{
    return Impl_->MaterializeOperation(operation);
}

TFuture<TOperationControllerReviveResult> TControllerAgent::ReviveOperation(const TOperationPtr& operation)
{
    return Impl_->ReviveOperation(operation);
}

TFuture<void> TControllerAgent::CommitOperation(const TOperationPtr& operation)
{
    return Impl_->CommitOperation(operation);
}

TFuture<void> TControllerAgent::CompleteOperation(const TOperationPtr& operation)
{
    return Impl_->CompleteOperation(operation);
}

TFuture<void> TControllerAgent::AbortOperation(const TOperationPtr& operation)
{
    return Impl_->AbortOperation(operation);
}

TFuture<void> TControllerAgent::DisposeOperation(const TOperationPtr& operation)
{
    return Impl_->DisposeOperation(operation);
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

DELEGATE_SIGNAL(TControllerAgent, void(), SchedulerConnecting, *Impl_);
DELEGATE_SIGNAL(TControllerAgent, void(), SchedulerConnected, *Impl_);
DELEGATE_SIGNAL(TControllerAgent, void(), SchedulerDisconnected, *Impl_);

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
