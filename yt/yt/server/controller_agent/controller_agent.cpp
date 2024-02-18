#include "controller_agent.h"

#include "config.h"
#include "bootstrap.h"
#include "helpers.h"
#include "job_monitoring_index_manager.h"
#include "job_profiler.h"
#include "job_tracker.h"
#include "master_connector.h"
#include "memory_watchdog.h"
#include "operation.h"
#include "operation_controller.h"
#include "operation_controller_host.h"
#include "private.h"
#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/message_queue.h>
#include <yt/yt/server/lib/scheduler/controller_agent_tracker_service_proxy.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chunk_client/throttler_manager.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/event_log/event_log.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>
#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/service_combiner.h>

#include <yt/yt/build/build.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/cast.h>

#include <tcmalloc/malloc_extension.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NConcurrency;
using namespace NYTree;
using namespace NChunkClient;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NEventLog;
using namespace NProfiling;
using namespace NYson;
using namespace NRpc;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NCoreDump;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////

struct TAgentToSchedulerScheduleAllocationResponse
{
    TAllocationId AllocationId;
    TOperationId OperationId;
    TControllerScheduleAllocationResultPtr Result;
};

using TAgentToSchedulerScheduleAllocationResponseOutboxPtr = TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerScheduleAllocationResponse>>;

////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public ISchedulingContext
{
public:
    TSchedulingContext(
        const NScheduler::NProto::TScheduleAllocationRequest* request,
        const TExecNodeDescriptorPtr& nodeDescriptor,
        const NScheduler::NProto::TScheduleAllocationSpec& scheduleAllocationSpec)
        : DiskResources_(FromProto<TDiskResources>(request->node_disk_resources()))
        , AllocationId_(FromProto<TAllocationId>(request->allocation_id()))
        , NodeDescriptor_(nodeDescriptor)
        , ScheduleAllocationSpec_(scheduleAllocationSpec)
        , PoolPath_(YT_PROTO_OPTIONAL(*request, pool_path))
    { }

    const std::optional<TString>& GetPoolPath() const override
    {
        return PoolPath_;
    }

    const TExecNodeDescriptorPtr& GetNodeDescriptor() const override
    {
        return NodeDescriptor_;
    }

    const TDiskResources& DiskResources() const override
    {
        return DiskResources_;
    }

    TAllocationId GetAllocationId() const override
    {
        return AllocationId_;
    }

    NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }

    const NScheduler::NProto::TScheduleAllocationSpec& GetScheduleAllocationSpec() const override
    {
        return ScheduleAllocationSpec_;
    }

private:
    const NScheduler::TDiskResources DiskResources_;
    const TAllocationId AllocationId_;
    const TExecNodeDescriptorPtr& NodeDescriptor_;
    const NScheduler::NProto::TScheduleAllocationSpec ScheduleAllocationSpec_;
    const std::optional<TString> PoolPath_;
};

////////////////////////////////////////////////////////////////////

class TZombieOperationOrchids
    : public TRefCounted
{
private:
    using TOperationIdToOrchidMap = THashMap<TOperationId, IYPathServicePtr>;
    using TMapIterator = TOperationIdToOrchidMap::iterator;

public:
    explicit TZombieOperationOrchids(TZombieOperationOrchidsConfigPtr config)
        : Config_(std::move(config))
    { }

    void AddOrchid(TOperationId id, IYPathServicePtr orchid)
    {
        if (!Config_->Enable) {
            return;
        }

        YT_LOG_INFO("Operation orchid saved in zombie orchids queue (OperationId: %v)", id);

        auto [iterator, inserted] = IdToOrchid_.emplace(id, std::move(orchid));
        YT_VERIFY(inserted);
        Queue_.emplace(TInstant::Now(), iterator);
        while (static_cast<int>(Queue_.size()) > Config_->Limit) {
            QueuePop();
        }
    }

    const TOperationIdToOrchidMap& GetOperationIdToOrchidMap() const
    {
        return IdToOrchid_;
    }

    void Clean()
    {
        IdToOrchid_.clear();
        Queue_ = {};
    }

    void StartPeriodicCleaning(const IInvokerPtr& invoker)
    {
        if (!Config_->Enable) {
            return;
        }
        CleanExecutor_ = New<TPeriodicExecutor>(
            invoker,
            BIND(&TZombieOperationOrchids::CleanOldOrchids, MakeWeak(this), Config_->CleanPeriod),
            Config_->CleanPeriod);
        CleanExecutor_->Start();
    }

private:
    TZombieOperationOrchidsConfigPtr Config_;
    TOperationIdToOrchidMap IdToOrchid_;
    std::queue<std::pair<TInstant, TMapIterator>> Queue_;

    TPeriodicExecutorPtr CleanExecutor_;

    void CleanOldOrchids(TDuration maxAge)
    {
        auto now = TInstant::Now();
        while (!Queue_.empty() && now > Queue_.front().first + maxAge) {
            QueuePop();
        }
    }

    void QueuePop()
    {
        auto iterator = Queue_.front().second;
        auto operationId = iterator->first;

        YT_VERIFY(!Queue_.empty());
        IdToOrchid_.erase(iterator);
        Queue_.pop();

        YT_LOG_INFO("Operation orchid removed from zombie orchids queue (OperationId: %v)", operationId);
    }
};

DEFINE_REFCOUNTED_TYPE(TZombieOperationOrchids)

////////////////////////////////////////////////////////////////////

class TControllerAgent::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TControllerAgentConfigPtr config,
        INodePtr configNode,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , ControllerThreadPool_(CreateThreadPool(Config_->ControllerThreadCount, "Controller"))
        , JobSpecBuildPool_(CreateThreadPool(Config_->JobSpecBuildThreadCount, "JobSpec"))
        , StatisticsOffloadPool_(CreateThreadPool(Config_->StatisticsOffloadThreadCount, "StatsOffload"))
        , ExecNodesUpdateQueue_(New<TActionQueue>("ExecNodes"))
        , SnapshotIOQueue_(New<TActionQueue>("SnapshotIO"))
        , ChunkLocationThrottlerManager_(New<TThrottlerManager>(
            Config_->ChunkLocationThrottler,
            ControllerAgentLogger))
        , ReconfigurableJobSpecSliceThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->JobSpecSliceThrottler,
            NLogging::TLogger(),
            ControllerAgentProfiler.WithPrefix("/job_spec_slice_throttler")))
        , JobSpecSliceThrottler_(ReconfigurableJobSpecSliceThrottler_)
        , CoreSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentSafeCoreDumps))
        , EventLogWriter_(CreateStaticTableEventLogWriter(
            Config_->EventLog,
            Bootstrap_->GetClient(),
            Bootstrap_->GetControlInvoker()))
        , JobReporter_(New<TJobReporter>(
            Config_->JobReporter,
            Bootstrap_->GetClient()->GetNativeConnection()))
        , MasterConnector_(std::make_unique<TMasterConnector>(
            Config_,
            std::move(configNode),
            Bootstrap_))
        , JobTracker_(New<TJobTracker>(Bootstrap_, JobReporter_))
        , JobProfiler_(New<TJobProfiler>())
        , JobEventsInvoker_(CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker(), "controller_agent"))
        , CachedExecNodeDescriptorsByTags_(New<TSyncExpiringCache<TSchedulingTagFilter, TFilteredExecNodeDescriptors>>(
            BIND_NO_PROPAGATE(&TImpl::FilterExecNodes, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout,
            Bootstrap_->GetControlInvoker()))
        , SchedulerProxy_(Bootstrap_->GetClient()->GetSchedulerChannel())
        , ZombieOperationOrchids_(New<TZombieOperationOrchids>(Config_->ZombieOperationOrchids))
        , JobMonitoringIndexManager_(Config_->UserJobMonitoring->MaxMonitoredUserJobsPerAgent)
    {
        ControllerAgentProfiler.AddFuncGauge("/monitored_user_job_count", MakeStrong(this), [this] {
            return WaitFor(BIND([&] {
                int sum = 0;
                for (const auto& [_, operation] : IdToOperation_) {
                    sum += operation->GetController()->GetMonitoredUserJobCount();
                }
                return sum;
            })
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run())
            .Value();
        });
    }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->Initialize();
        ScheduleConnect(true);
    }

    IYPathServicePtr CreateOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto staticOrchidProducer = BIND_NO_PROPAGATE(&TImpl::BuildStaticOrchid, MakeStrong(this));
        auto staticOrchidService = IYPathService::FromProducer(staticOrchidProducer, Config_->StaticOrchidCacheUpdatePeriod);
        StaticOrchidService_.Reset(dynamic_cast<ICachedYPathService*>(staticOrchidService.Get()));
        YT_VERIFY(StaticOrchidService_);

        auto dynamicOrchidService = GetDynamicOrchidService()
            ->Via(Bootstrap_->GetControlInvoker());

        auto jobTrackerOrchidService = GetJobTrackerOrchidService();

        return New<TServiceCombiner>(
            std::vector<IYPathServicePtr>{
                staticOrchidService->Via(Bootstrap_->GetControlInvoker()),
                std::move(dynamicOrchidService),
                std::move(jobTrackerOrchidService),
            },
            Config_->ControllerOrchidKeysUpdatePeriod);
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

    TIncarnationId GetIncarnationId() const
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
                "Controller agent is not connected");
        }
    }

    void ValidateIncarnation(TIncarnationId incarnationId) const
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

    const IInvokerPtr& GetJobSpecBuildPoolInvoker()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobSpecBuildPool_->GetInvoker();
    }

    const IInvokerPtr& GetStatisticsOffloadInvoker()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return StatisticsOffloadPool_->GetInvoker();
    }

    const IInvokerPtr& GetExecNodesUpdateInvoker()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ExecNodesUpdateQueue_->GetInvoker();
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

    TJobTracker* GetJobTracker() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobTracker_.Get();
    }

    TJobProfiler* GetJobProfiler() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobProfiler_.Get();
    }

    const TMediumDirectoryPtr& GetMediumDirectory() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();
    }

    const TControllerAgentConfigPtr& GetConfig() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_;
    }

    void UpdateConfig(const TControllerAgentConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto oldConfigNode = ConvertToNode(Config_);
        auto newConfigNode = ConvertToNode(config);
        if (AreNodesEqual(oldConfigNode, newConfigNode)) {
            return;
        }

        Config_ = config;

        Bootstrap_->OnDynamicConfigChanged(Config_);

        ControllerThreadPool_->Configure(Config_->ControllerThreadCount);

        JobTracker_->UpdateConfig(Config_);

        ChunkLocationThrottlerManager_->Reconfigure(Config_->ChunkLocationThrottler);

        EventLogWriter_->UpdateConfig(Config_->EventLog);

        ReconfigurableJobSpecSliceThrottler_->Reconfigure(Config_->JobSpecSliceThrottler);

        if (MemoryWatchdog_) {
            MemoryWatchdog_->UpdateConfig(Config_->MemoryWatchdog);
        }

        if (HeartbeatExecutor_) {
            HeartbeatExecutor_->SetPeriod(Config_->SchedulerHeartbeatPeriod);
        }

        if (ScheduleAllocationHeartbeatExecutor_) {
            ScheduleAllocationHeartbeatExecutor_->SetPeriod(Config_->ScheduleAllocationHeartbeatPeriod);
        }

        StaticOrchidService_->SetCachePeriod(Config_->StaticOrchidCacheUpdatePeriod);

        for (const auto& [operationId, operation] : IdToOperation_) {
            auto controller = operation->GetController();
            controller->GetCancelableInvoker()->Invoke(
                BIND(&IOperationController::UpdateConfig, controller, config));
        }

        CachedExecNodeDescriptorsByTags_->SetExpirationTimeout(Config_->SchedulingTagFilterExpireTimeout);

        JobReporter_->UpdateConfig(Config_->JobReporter);

        JobMonitoringIndexManager_.SetMaxSize(Config_->UserJobMonitoring->MaxMonitoredUserJobsPerAgent);
    }


    const TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ChunkLocationThrottlerManager_;
    }

    const ICoreDumperPtr& GetCoreDumper() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCoreDumper();
    }

    const TAsyncSemaphorePtr& GetCoreSemaphore() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CoreSemaphore_;
    }

    const IEventLogWriterPtr& GetEventLogWriter() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return EventLogWriter_;
    }

    const TJobReporterPtr& GetJobReporter() const
    {
        return JobReporter_;
    }

    IInvokerPtr CreateCancelableInvoker(const IInvokerPtr& invoker)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CancelableContext_->CreateInvoker(invoker);
    }

    TOperationPtr FindOperation(TOperationId operationId) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        auto it = IdToOperation_.find(operationId);
        return it == IdToOperation_.end() ? nullptr : it->second;
    }

    TOperationPtr GetOperation(TOperationId operationId) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        auto operation = FindOperation(operationId);
        YT_VERIFY(operation);

        return operation;
    }

    TOperationPtr GetOperationOrThrow(TOperationId operationId) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        auto operation = FindOperation(operationId);
        if (!operation) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchOperation,
                "No such operation %v",
                operationId);
        }
        return operation;
    }

    const TOperationIdToOperationMap& GetOperations() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        return IdToOperation_;
    }


    void RegisterOperation(const NProto::TOperationDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        auto operation = New<TOperation>(descriptor);

        auto operationId = operation->GetId();

        auto parentTraceContext = TryGetCurrentTraceContext();

        auto traceContextGuard = CreateOperationTraceContextGuard(
            "RegisterOperation",
            operationId);

        {
            // TODO(pogorelov): Refactor operation creation.

            auto host = New<TOperationControllerHost>(
                operation.Get(),
                CancelableControlInvoker_,
                Bootstrap_->GetControlInvoker(),
                OperationEventsOutbox_,
                RunningAllocationStatisticsUpdatesOutbox_,
                Bootstrap_);
            operation->SetHost(host);

            auto controller = CreateControllerForOperation(
                Config_,
                operation.Get(),
                parentTraceContext);
            operation->SetController(controller);

            auto jobTrackerOperationHandler = JobTracker_->RegisterOperation(
                operationId,
                MakeWeak(operation->GetController()));

            host->SetJobTrackerOperationHandler(std::move(jobTrackerOperationHandler));
        }

        EmplaceOrCrash(IdToOperation_, operationId, std::move(operation));

        MasterConnector_->RegisterOperation(operationId);

        YT_LOG_DEBUG("Operation registered (OperationId: %v)", operationId);
    }

    TOperationControllerUnregisterResult DoDisposeAndUnregisterOperation(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        TOperationControllerUnregisterResult result;
        {
            if (auto maybeDelay = Config_->TestingOptions->DelayInUnregistration) {
                TDelayedExecutor::WaitForDuration(*maybeDelay);
            }
            auto operation = GetOperationOrThrow(operationId);
            auto controller = operation->GetController();
            if (controller) {
                auto error = WaitFor(BIND(&IOperationControllerSchedulerHost::Dispose, controller)
                    // It is called in regular invoker since controller is canceled
                    // but we want to make some final actions.
                    .AsyncVia(controller->GetInvoker())
                    .Run());

                YT_LOG_FATAL_IF(
                    !error.IsOK(),
                    error,
                    "Unexpected operation disposal fail");

                result.ResidualJobMetrics = controller->PullJobMetricsDelta(/*force*/ true);
            }
        }

        UnregisterOperation(operationId);

        return result;
    }

    TFuture<TOperationControllerUnregisterResult> DisposeAndUnregisterOperation(TOperationId operationId)
    {
        return BIND(&TImpl::DoDisposeAndUnregisterOperation, MakeStrong(this), operationId)
            .AsyncVia(CancelableControlInvoker_)
            .Run();
    }

    void UnregisterOperation(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        auto operation = GetOperationOrThrow(operationId);

        {
            auto controller = operation->GetController();

            controller->Cancel();

            // We carefully destroy controller and log warning if we detect that controller is actually leaked.
            operation->SetController(nullptr);
            auto refCount = ResetAndGetResidualRefCount(controller);
            if (refCount > 0) {
                YT_LOG_WARNING(
                    "Operation is going to be unregistered, but its controller has non-zero residual refcount; memory leak is possible "
                    "(RefCount: %v)",
                    refCount);
            }
        }

        MasterConnector_->UnregisterOperation(operationId);

        if (JobMonitoringIndexManager_.TryRemoveOperation(operationId)) {
            EnqueueJobMonitoringAlertUpdate();
        }

        JobTracker_->UnregisterOperation(operationId);

        EraseOrCrash(IdToOperation_, operationId);

        YT_LOG_DEBUG("Operation unregistered (OperationId: %v)", operationId);
    }

    TFuture<void> UpdateOperationRuntimeParameters(TOperationId operationId, TOperationRuntimeParametersUpdatePtr update)
    {
        auto operation = GetOperationOrThrow(operationId);
        if (update->Acl) {
            operation->SetAcl(*update->Acl);
            const auto& controller = operation->GetController();
            if (controller) {
                return BIND(&IOperationControllerSchedulerHost::UpdateRuntimeParameters, controller, std::move(update))
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
            }
        }

        operation->UpdateJobShellOptions(update->OptionsPerJobShell);

        return VoidFuture;
    }

    template <class TResult>
    void OnHeavyControllerActionFinished(
        TOperationId operationId,
        TControllerEpoch controllerEpoch,
        const TErrorOr<TResult>& resultOrError)
    {
        std::optional<TResult> maybeResult;
        if (resultOrError.IsOK()) {
            maybeResult.emplace(resultOrError.Value());
        }

        OperationEventsOutbox_->Enqueue(TAgentToSchedulerOperationEvent::CreateHeavyControllerActionFinishedEvent(
            operationId,
            controllerEpoch,
            resultOrError,
            maybeResult));
    }

    TFuture<std::optional<TOperationControllerInitializeResult>> InitializeOperation(
        const TOperationPtr& operation,
        const std::optional<TControllerTransactionIds>& transactions)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        auto callback = transactions
            ? BIND(&IOperationControllerSchedulerHost::InitializeReviving, controller, *transactions)
            : BIND(&IOperationControllerSchedulerHost::InitializeClean, controller);
        auto asyncResult = callback
            .AsyncVia(controller->GetCancelableInvoker())
            .Run()
            .Apply(BIND([=] (const TOperationControllerInitializeResult& result) {
                const auto& transactionIds = result.TransactionIds;
                std::vector<TTransactionId> watchTransactionIds({
                    transactionIds.AsyncId,
                    transactionIds.InputId,
                    transactionIds.OutputId,
                    transactionIds.DebugId
                });
                watchTransactionIds.push_back(operation->GetUserTransactionId());

                watchTransactionIds.erase(
                    std::remove_if(
                        watchTransactionIds.begin(),
                        watchTransactionIds.end(),
                        [] (auto transactionId) { return !transactionId; }),
                    watchTransactionIds.end());

                operation->SetWatchTransactionIds(watchTransactionIds);

                return result;
            }).AsyncVia(GetCurrentInvoker()));

        return WithSoftTimeout(
            asyncResult,
            Config_->HeavyRequestImmediateResponseTimeout,
            /*onFinishedAfterTimeout*/ BIND(
                &TImpl::OnHeavyControllerActionFinished<TOperationControllerInitializeResult>,
                MakeWeak(this),
                operation->GetId(),
                operation->GetControllerEpoch())
                .Via(CancelableControlInvoker_));
    }

    TFuture<std::optional<TOperationControllerPrepareResult>> PrepareOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        auto asyncResult = BIND(&IOperationControllerSchedulerHost::Prepare, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();

        return WithSoftTimeout(
            asyncResult,
            Config_->HeavyRequestImmediateResponseTimeout,
            /*onFinishedAfterTimeout*/ BIND(
                &TImpl::OnHeavyControllerActionFinished<TOperationControllerPrepareResult>,
                MakeWeak(this),
                operation->GetId(),
                operation->GetControllerEpoch())
                .Via(CancelableControlInvoker_));
    }

    TFuture<std::optional<TOperationControllerMaterializeResult>> MaterializeOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        auto asyncResult = BIND(&IOperationControllerSchedulerHost::Materialize, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();

        return WithSoftTimeout(
            asyncResult,
            Config_->HeavyRequestImmediateResponseTimeout,
            /*onFinishedAfterTimeout*/ BIND(
                &TImpl::OnHeavyControllerActionFinished<TOperationControllerMaterializeResult>,
                MakeWeak(this),
                operation->GetId(),
                operation->GetControllerEpoch())
                .Via(CancelableControlInvoker_));
    }

    TFuture<std::optional<TOperationControllerReviveResult>> ReviveOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        const auto& controller = operation->GetControllerOrThrow();
        auto asyncResult = BIND(&IOperationControllerSchedulerHost::Revive, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();

        return WithSoftTimeout(
            asyncResult,
            Config_->HeavyRequestImmediateResponseTimeout,
            /*onFinishedAfterTimeout*/ BIND(
                &TImpl::OnHeavyControllerActionFinished<TOperationControllerReviveResult>,
                MakeWeak(this),
                operation->GetId(),
                operation->GetControllerEpoch())
                .Via(CancelableControlInvoker_));
    }

    TFuture<std::optional<TOperationControllerCommitResult>> CommitOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        const auto& controller = operation->GetControllerOrThrow();

        auto getOrchidAndCommit = BIND(
            [controller, this_ = MakeStrong(this)] () {
                controller->Commit();
                // Orchid service runs on uncancellable invokers, so it is legal to use it after context cancellation.
                controller->ZombifyOrchid();
            })
            .AsyncVia(controller->GetInvoker());

        auto saveOrchid = BIND(
            [this, this_ = MakeStrong(this), operationId = operation->GetId(), controller] () {
                auto orchid = controller->GetOrchid();
                if (orchid) {
                    ZombieOperationOrchids_->AddOrchid(operationId, std::move(orchid));
                }

                return TOperationControllerCommitResult{};
            })
            .AsyncVia(GetCurrentInvoker());

        auto asyncResult = getOrchidAndCommit().Apply(saveOrchid);

        return WithSoftTimeout(
            asyncResult,
            Config_->HeavyRequestImmediateResponseTimeout,
            /*onFinishedAfterTimeout*/ BIND(
                &TImpl::OnHeavyControllerActionFinished<TOperationControllerCommitResult>,
                MakeWeak(this),
                operation->GetId(),
                operation->GetControllerEpoch())
                .Via(CancelableControlInvoker_));
    }

    TFuture<void> CompleteOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        operation->SetWatchTransactionIds({});

        const auto& controller = operation->GetControllerOrThrow();
        return BIND(&IOperationControllerSchedulerHost::Complete, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    // NB(eshcherbin): controllerFinalState should be either Aborted or Failed.
    TFuture<void> TerminateOperation(const TOperationPtr& operation, EControllerState controllerFinalState)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        operation->SetWatchTransactionIds({});

        const auto& controller = operation->GetController();
        if (!controller) {
            YT_LOG_DEBUG("No controller to abort (OperationId: %v)",
                operation->GetId());
            return VoidFuture;
        }

        if (!controller->GetCancelableContext()->IsCanceled()) {
            controller->Cancel();
            // Orchid service runs on uncancellable invokers, so it is legal to use it after context cancellation.
            controller->ZombifyOrchid();

            if (auto orchid = controller->GetOrchid()) {
                ZombieOperationOrchids_->AddOrchid(operation->GetId(), std::move(orchid));
            }
        }

        return BIND(&IOperationControllerSchedulerHost::Terminate, controller, controllerFinalState)
            .AsyncVia(controller->GetInvoker())
            .Run();
    }

    TFuture<std::vector<TErrorOr<TJobStartInfo>>> SettleJobs(const std::vector<TSettleJobRequest>& requests)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        std::vector<TFuture<TJobStartInfo>> asyncJobInfos;
        asyncJobInfos.reserve(std::size(requests));
        for (const auto& request : requests) {
            YT_LOG_DEBUG(
                "Settling job (OperationId: %v, AllocationId: %v)",
                request.OperationId,
                request.AllocationId);

            auto operation = FindOperation(request.OperationId);
            if (!operation) {
                asyncJobInfos.push_back(MakeFuture<TJobStartInfo>(TError(
                    "No such operation %v",
                    request.OperationId)));
                continue;
            }

            auto controller = operation->GetController();
            auto asyncJobInfo = BIND(
                &IOperationController::SettleJob,
                controller,
                request.AllocationId)
                .AsyncVia(controller->GetCancelableInvoker(EOperationControllerQueue::GetJobSpec))
                .Run();

            asyncJobInfos.push_back(asyncJobInfo);
        }

        return AllSet(asyncJobInfos);
    }

    TFuture<TOperationInfo> BuildOperationInfo(TOperationId operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Connected_);

        auto operation = GetOperationOrThrow(operationId);
        auto controller = operation->GetController();
        return BIND(&IOperationController::BuildOperationInfo, controller)
            .AsyncVia(controller->GetCancelableInvoker())
            .Run();
    }

    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const TSchedulingTagFilter& filter, bool onlineOnly = false) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (filter.IsEmpty() && !onlineOnly) {
            auto guard = ReaderGuard(ExecNodeDescriptorsLock_);
            return CachedExecNodeDescriptors_;
        }

        auto result = CachedExecNodeDescriptorsByTags_->Get(filter);
        return onlineOnly ? result.Online : result.All;
    }

    TJobResources GetMaxAvailableResources(const TSchedulingTagFilter& filter)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CachedExecNodeDescriptorsByTags_->Get(filter).MaxAvailableResources;
    }

    int GetOnlineExecNodeCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(ExecNodeDescriptorsLock_);
        return OnlineExecNodeCount_;
    }

    const IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobSpecSliceThrottler_;
    }

    void ValidateOperationAccess(
        const TString& user,
        TOperationId operationId,
        EPermission permission)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ValidateConnected();

        NScheduler::ValidateOperationAccess(
            user,
            operationId,
            TAllocationId(),
            permission,
            GetOperationOrThrow(operationId)->GetAcl(),
            Bootstrap_->GetClient(),
            Logger);
    }

    std::optional<TJobMonitoringDescriptor> TryAcquireJobMonitoringDescriptor(TOperationId operationId)
    {
        auto index = JobMonitoringIndexManager_.TryAddIndex(operationId);

        if (!index) {
            YT_LOG_DEBUG(
                "Failed to register job for monitoring: too many monitored jobs per controller agent "
                "(OperationId: %v, MaxMonitoredUserJobsPerAgent: %v)",
                operationId,
                JobMonitoringIndexManager_.GetMaxSize());

            EnqueueJobMonitoringAlertUpdate();
            return std::nullopt;
        }

        return TJobMonitoringDescriptor{IncarnationId_, *index};
    }

    bool ReleaseJobMonitoringDescriptor(TOperationId operationId, TJobMonitoringDescriptor descriptor)
    {
        auto result = JobMonitoringIndexManager_.TryRemoveIndex(operationId, descriptor.Index);
        if (JobMonitoringIndexManager_.GetResidualCapacity() == 1) {
            EnqueueJobMonitoringAlertUpdate();
        }
        return result;
    }

    void EnqueueJobMonitoringAlertUpdate()
    {
        BIND([&] {
            auto alert = TError();
            if (JobMonitoringIndexManager_.GetResidualCapacity() == 0) {
                alert = TError(
                    "Limit of monitored user jobs per controller agent reached, "
                    "some jobs may be not monitored")
                    << TErrorAttribute(
                        "limit_per_controller_agent",
                        Config_->UserJobMonitoring->MaxMonitoredUserJobsPerAgent);
            }
            MasterConnector_->SetControllerAgentAlert(
                EControllerAgentAlertType::UserJobMonitoringLimited,
                std::move(alert));
        })
        .Via(CancelableControlInvoker_)
        .Run();
    }

    DEFINE_SIGNAL(void(), SchedulerConnecting);
    DEFINE_SIGNAL(void(TIncarnationId), SchedulerConnected);
    DEFINE_SIGNAL(void(), SchedulerDisconnected);

private:
    TControllerAgentConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    const IThreadPoolPtr ControllerThreadPool_;
    const IThreadPoolPtr JobSpecBuildPool_;
    const IThreadPoolPtr StatisticsOffloadPool_;
    const TActionQueuePtr ExecNodesUpdateQueue_;
    const TActionQueuePtr SnapshotIOQueue_;
    const TThrottlerManagerPtr ChunkLocationThrottlerManager_;
    const IReconfigurableThroughputThrottlerPtr ReconfigurableJobSpecSliceThrottler_;
    const IThroughputThrottlerPtr JobSpecSliceThrottler_;
    const TAsyncSemaphorePtr CoreSemaphore_;
    const IEventLogWriterPtr EventLogWriter_;
    const TJobReporterPtr JobReporter_;
    const std::unique_ptr<TMasterConnector> MasterConnector_;
    const TJobTrackerPtr JobTracker_;
    const TJobProfilerPtr JobProfiler_;

    bool Connected_= false;
    bool ConnectScheduled_ = false;
    std::atomic<TInstant> ConnectionTime_ = TInstant::Zero();
    TIncarnationId IncarnationId_;
    TString SchedulerVersion_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableControlInvoker_;
    IInvokerPtr JobEventsInvoker_;

    TOperationIdToOperationMap IdToOperation_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ExecNodeDescriptorsLock_);
    TRefCountedExecNodeDescriptorMapPtr CachedExecNodeDescriptors_ = New<TRefCountedExecNodeDescriptorMap>();

    struct TFilteredExecNodeDescriptors
    {
        TRefCountedExecNodeDescriptorMapPtr All;
        TRefCountedExecNodeDescriptorMapPtr Online;

        TJobResources MaxAvailableResources;
    };

    const TIntrusivePtr<TSyncExpiringCache<TSchedulingTagFilter, TFilteredExecNodeDescriptors>> CachedExecNodeDescriptorsByTags_;
    int OnlineExecNodeCount_ = 0;

    TControllerAgentTrackerServiceProxy SchedulerProxy_;

    TInstant LastExecNodesUpdateTime_;
    TInstant LastOperationsSendTime_;
    TInstant LastOperationJobMetricsSendTime_;
    TInstant LastOperationAlertsSendTime_;
    TInstant LastSuspiciousJobsSendTime_;

    TAgentToSchedulerOperationEventOutboxPtr OperationEventsOutbox_;
    TAgentToSchedulerScheduleAllocationResponseOutboxPtr ScheduleAllocationResponsesOutbox_;
    TAgentToSchedulerRunningAllocationStatisticsOutboxPtr RunningAllocationStatisticsUpdatesOutbox_;

    std::unique_ptr<TMessageQueueInbox> AbortedAllocationEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> OperationEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> ScheduleAllocationRequestsInbox_;

    TIntrusivePtr<NYTree::ICachedYPathService> StaticOrchidService_;
    TZombieOperationOrchidsPtr ZombieOperationOrchids_;

    TPeriodicExecutorPtr HeartbeatExecutor_;
    TPeriodicExecutorPtr ScheduleAllocationHeartbeatExecutor_;

    TMemoryWatchdogPtr MemoryWatchdog_;

    TJobMonitoringIndexManager JobMonitoringIndexManager_;

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
            immediate ? TDuration::Zero() : Config_->SchedulerHandshakeFailureBackoff);
    }

    void DoConnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(ConnectScheduled_);
        ConnectScheduled_ = false;

        try {
            OnConnecting();
            SyncClusterDirectory();
            SyncMediumDirectory();
            SyncMasterCellDirectory();
            UpdateConfig();
            PerformHandshake();
            OnConnected();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error connecting to scheduler");

            SchedulerDisconnected_.Fire();
            DoCleanup();
            ScheduleConnect(false);
        }
    }

    void OnConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        YT_LOG_INFO("Connecting to scheduler");

        YT_VERIFY(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();
        CancelableControlInvoker_ = CancelableContext_->CreateInvoker(Bootstrap_->GetControlInvoker());

        {
            auto error = WaitFor(JobTracker_->Initialize());

            YT_LOG_FATAL_IF(
                !error.IsOK(),
                error,
                "Unexpected failure in job tracker initialization");
        }

        SwitchTo(CancelableControlInvoker_);

        SchedulerConnecting_.Fire();
    }

    void SyncClusterDirectory()
    {
        YT_LOG_INFO("Synchronizing cluster directory");

        WaitFor(Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetClusterDirectorySynchronizer()
            ->Sync(/*force*/ true))
            .ThrowOnError();

        YT_LOG_INFO("Cluster directory synchronized");
    }

    void SyncMediumDirectory()
    {
        YT_LOG_INFO("Requesting medium directory");

        WaitFor(Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMediumDirectorySynchronizer()
            ->NextSync(/*force*/ true))
            .ThrowOnError();

        YT_LOG_INFO("Medium directory received");
    }

    void SyncMasterCellDirectory()
    {
        YT_LOG_INFO("Syncing master cell directory");

        WaitForFast(Bootstrap_
            ->GetClient()
            ->GetNativeConnection()
            ->GetMasterCellDirectorySynchronizer()
            ->NextSync(/*force*/ true))
            .ThrowOnError();

        YT_LOG_INFO("Master cell directory synced");
    }

    void UpdateConfig()
    {
        YT_LOG_INFO("Updating config");

        WaitFor(MasterConnector_->UpdateConfig())
            .ThrowOnError();

        YT_LOG_INFO("Config updates");
    }

    void PerformHandshake()
    {
        YT_LOG_INFO("Sending handshake");

        auto req = SchedulerProxy_.Handshake();
        req->SetTimeout(Config_->SchedulerHandshakeRpcTimeout);
        req->set_agent_id(Bootstrap_->GetAgentId());
        ToProto(req->mutable_agent_addresses(), Bootstrap_->GetLocalAddresses());
        ToProto(req->mutable_tags(), MasterConnector_->GetTags());
        GenerateMutationId(req);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        if (auto maybeDelay = Config_->TestingOptions->DelayInHandshake) {
            YT_LOG_DEBUG("Sleeping before performing handshake (Delay: %v)", *maybeDelay);

            TDelayedExecutor::WaitForDuration(*maybeDelay);
        }

        YT_LOG_DEBUG("Handshake succeeded");

        IncarnationId_ = FromProto<TIncarnationId>(rsp->incarnation_id());
        SchedulerVersion_ = rsp->scheduler_version();
    }

    void OnConnected()
    {
        Connected_ = true;
        ConnectionTime_.store(TInstant::Now());

        YT_LOG_INFO(
            "Controller agent connected (IncarnationId: %v)",
            IncarnationId_);

        OperationEventsOutbox_ = New<TMessageQueueOutbox<TAgentToSchedulerOperationEvent>>(
            ControllerAgentLogger.WithTag(
                "Kind: AgentToSchedulerOperations, IncarnationId: %v",
                IncarnationId_),
            ControllerAgentProfiler.WithTag("queue", "operation_events"),
            CancelableControlInvoker_);
        ScheduleAllocationResponsesOutbox_ = New<TMessageQueueOutbox<TAgentToSchedulerScheduleAllocationResponse>>(
            ControllerAgentLogger.WithTag(
                "Kind: AgentToSchedulerScheduleAllocationResponses, IncarnationId: %v",
                IncarnationId_),
            ControllerAgentProfiler.WithTag("queue", "schedule_job_responses"),
            Bootstrap_->GetControlInvoker(),
            /*supportTracing*/ true);

        RunningAllocationStatisticsUpdatesOutbox_ = New<TMessageQueueOutbox<TAgentToSchedulerRunningAllocationStatistics>>(
            ControllerAgentLogger.WithTag(
                "Kind: AgentToSchedulerRunningAllocationStatistics, IncarnationId: %v",
                IncarnationId_),
            ControllerAgentProfiler.WithTag("queue", "running_allocation_statistics"),
            Bootstrap_->GetControlInvoker());

        AbortedAllocationEventsInbox_ = std::make_unique<TMessageQueueInbox>(
            ControllerAgentLogger.WithTag(
                "Kind: SchedulerToAgentAbortedAllocations, IncarnationId: %v",
                IncarnationId_),
            ControllerAgentProfiler.WithTag("queue", "job_events"),
            JobEventsInvoker_);
        OperationEventsInbox_ = std::make_unique<TMessageQueueInbox>(
            ControllerAgentLogger.WithTag(
                "Kind: SchedulerToAgentOperations, IncarnationId: %v",
                IncarnationId_),
            ControllerAgentProfiler.WithTag("queue", "operation_events"),
            CancelableControlInvoker_);
        ScheduleAllocationRequestsInbox_ = std::make_unique<TMessageQueueInbox>(
            ControllerAgentLogger.WithTag(
                "Kind: SchedulerToAgentScheduleAllocationRequests, IncarnationId: %v",
                IncarnationId_),
            ControllerAgentProfiler.WithTag("queue", "schedule_job_requests"),
            Bootstrap_->GetControlInvoker());

        MemoryWatchdog_ = New<TMemoryWatchdog>(
            Config_->MemoryWatchdog,
            Bootstrap_);

        HeartbeatExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TControllerAgent::TImpl::SendHeartbeat, MakeWeak(this)),
            Config_->SchedulerHeartbeatPeriod);
        HeartbeatExecutor_->Start();

        ScheduleAllocationHeartbeatExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TControllerAgent::TImpl::SendScheduleAllocationHeartbeat, MakeWeak(this)),
            Config_->ScheduleAllocationHeartbeatPeriod);
        ScheduleAllocationHeartbeatExecutor_->Start();

        ZombieOperationOrchids_->Clean();
        ZombieOperationOrchids_->StartPeriodicCleaning(CancelableControlInvoker_);

        // TODO(pogorelov): Do not call it directly, subscribe on signal when job tracker becomes stable.
        JobTracker_->OnSchedulerConnected(IncarnationId_);

        SchedulerConnected_.Fire(IncarnationId_);
    }

    void DoDisconnect(const TError& error) noexcept
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TForbidContextSwitchGuard contextSwitchGuard;

        if (Connected_) {
            YT_LOG_WARNING(error, "Disconnecting scheduler");

            SchedulerDisconnected_.Fire();

            YT_LOG_WARNING("Scheduler disconnected");
        }

        DoCleanup();

        ScheduleConnect(true);
    }

    void DoCleanup()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Connected_ = false;
        ConnectionTime_.store(TInstant::Zero());
        IncarnationId_ = {};

        for (const auto& [operationId, operation] : IdToOperation_) {
            auto controller = operation->GetController();
            controller->Cancel();
        }
        IdToOperation_.clear();

        if (CancelableContext_) {
            CancelableContext_->Cancel(TError("Scheduler disconnected"));
            CancelableContext_.Reset();
        }
        CancelableControlInvoker_.Reset();

        JobTracker_->Cleanup();

        CachedExecNodeDescriptorsByTags_->Clear();

        if (HeartbeatExecutor_) {
            YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());
            HeartbeatExecutor_.Reset();
        }

        MemoryWatchdog_.Reset();
        OperationEventsOutbox_.Reset();
        ScheduleAllocationResponsesOutbox_.Reset();
        RunningAllocationStatisticsUpdatesOutbox_.Reset();

        AbortedAllocationEventsInbox_.reset();
        OperationEventsInbox_.reset();
        ScheduleAllocationRequestsInbox_.reset();
    }

    struct TPreparedHeartbeatRequest
    {
        TControllerAgentTrackerServiceProxy::TReqHeartbeatPtr RpcRequest;
        bool ExecNodesRequested = false;
        bool OperationsSent = false;
        bool OperationJobMetricsSent = false;
        bool OperationAlertsSent = false;
        bool SuspiciousJobsSent = false;
    };

    TPreparedHeartbeatRequest PrepareHeartbeatRequest()
    {
        TPreparedHeartbeatRequest preparedRequest;

        auto request = preparedRequest.RpcRequest = SchedulerProxy_.Heartbeat();
        request->SetTimeout(Config_->SchedulerHeartbeatRpcTimeout);
        request->SetRequestHeavy(true);
        request->SetResponseHeavy(true);
        request->set_agent_id(Bootstrap_->GetAgentId());
        ToProto(request->mutable_incarnation_id(), IncarnationId_);
        GenerateMutationId(request);

        THashSet<TOperationId> flushJobMetricsOperationIds;
        THashSet<TOperationId> finishedOperationIds;

        OperationEventsOutbox_->BuildOutcoming(
            request->mutable_agent_to_scheduler_operation_events(),
            [&flushJobMetricsOperationIds, &finishedOperationIds] (auto* protoEvent, const auto& event) {
                protoEvent->set_event_type(static_cast<int>(event.EventType));
                ToProto(protoEvent->mutable_operation_id(), event.OperationId);
                protoEvent->set_controller_epoch(event.ControllerEpoch.Underlying());
                switch (event.EventType) {
                    case EAgentToSchedulerOperationEventType::Completed:
                        break;
                    case EAgentToSchedulerOperationEventType::Aborted:
                    case EAgentToSchedulerOperationEventType::Failed:
                    case EAgentToSchedulerOperationEventType::Suspended:
                        ToProto(protoEvent->mutable_error(), event.Error);
                        break;
                    case EAgentToSchedulerOperationEventType::BannedInTentativeTree:
                        ToProto(protoEvent->mutable_tentative_tree_id(), event.TentativeTreeId);
                        ToProto(protoEvent->mutable_tentative_tree_allocation_ids(), event.TentativeTreeAllocationIds);
                        break;
                    case EAgentToSchedulerOperationEventType::InitializationFinished:
                        ToProto(protoEvent->mutable_error(), event.Error);
                        if (event.InitializeResult) {
                            ToProto(protoEvent->mutable_initialize_result(), *event.InitializeResult);
                        }
                        break;
                    case EAgentToSchedulerOperationEventType::PreparationFinished:
                        ToProto(protoEvent->mutable_error(), event.Error);
                        if (event.PrepareResult) {
                            ToProto(protoEvent->mutable_prepare_result(), *event.PrepareResult);
                        }
                        break;
                    case EAgentToSchedulerOperationEventType::MaterializationFinished:
                        ToProto(protoEvent->mutable_error(), event.Error);
                        if (event.MaterializeResult) {
                            ToProto(protoEvent->mutable_materialize_result(), *event.MaterializeResult);
                        }
                        break;
                    case EAgentToSchedulerOperationEventType::RevivalFinished:
                        ToProto(protoEvent->mutable_error(), event.Error);
                        if (event.ReviveResult) {
                            ToProto(protoEvent->mutable_revive_result(), *event.ReviveResult);
                        }
                        break;
                    case EAgentToSchedulerOperationEventType::CommitFinished:
                        ToProto(protoEvent->mutable_error(), event.Error);
                        if (event.CommitResult) {
                            ToProto(protoEvent->mutable_commit_result(), *event.CommitResult);
                        }
                        break;
                    default:
                        YT_ABORT();
                }

                switch (event.EventType) {
                    case EAgentToSchedulerOperationEventType::Completed:
                    case EAgentToSchedulerOperationEventType::Aborted:
                    case EAgentToSchedulerOperationEventType::Failed:
                    case EAgentToSchedulerOperationEventType::Suspended:
                    case EAgentToSchedulerOperationEventType::BannedInTentativeTree:
                        flushJobMetricsOperationIds.insert(event.OperationId);
                    default:
                        break;
                }

                switch (event.EventType) {
                    case EAgentToSchedulerOperationEventType::Completed:
                    case EAgentToSchedulerOperationEventType::Aborted:
                    case EAgentToSchedulerOperationEventType::Failed:
                        finishedOperationIds.insert(event.OperationId);
                    default:
                        break;
                }
            });

        RunningAllocationStatisticsUpdatesOutbox_->BuildOutcoming(
            request->mutable_agent_to_scheduler_running_allocation_statistics_updates(),
            [] (auto* protoStatistics, const auto& statistics) {
                ToProto(protoStatistics, statistics);
            },
            Config_->MaxRunningJobStatisticsUpdateCountPerHeartbeat);

        auto error = WaitFor(BIND([&, request] {
                AbortedAllocationEventsInbox_->ReportStatus(request->mutable_scheduler_to_agent_aborted_allocation_events());
            })
            .AsyncVia(JobEventsInvoker_)
            .Run());

        YT_LOG_FATAL_IF(!error.IsOK(), error, "Failed to report job event inbox status");

        OperationEventsInbox_->ReportStatus(request->mutable_scheduler_to_agent_operation_events());
        ScheduleAllocationRequestsInbox_->ReportStatus(request->mutable_scheduler_to_agent_schedule_allocation_requests());

        auto now = TInstant::Now();
        preparedRequest.ExecNodesRequested = LastExecNodesUpdateTime_ + Config_->ExecNodesUpdatePeriod < now;
        preparedRequest.OperationsSent = LastOperationsSendTime_ + Config_->OperationsPushPeriod < now;
        preparedRequest.OperationJobMetricsSent = LastOperationJobMetricsSendTime_ + Config_->OperationJobMetricsPushPeriod < now;
        preparedRequest.OperationAlertsSent = LastOperationAlertsSendTime_ + Config_->OperationAlertsPushPeriod < now;
        preparedRequest.SuspiciousJobsSent = LastSuspiciousJobsSendTime_ + Config_->SuspiciousJobsPushPeriod < now;

        for (const auto& [operationId, operation] : GetOperations()) {
            bool flushJobMetrics = flushJobMetricsOperationIds.contains(operationId);
            bool sentAlerts = finishedOperationIds.contains(operationId);
            if (!preparedRequest.OperationsSent && !flushJobMetrics && !sentAlerts) {
                continue;
            }
            auto controller = operation->GetController();

            auto* protoOperation = request->add_operations();
            ToProto(protoOperation->mutable_operation_id(), operationId);

            // We must to sent job metrics for finished operations.
            if (preparedRequest.OperationJobMetricsSent || flushJobMetrics) {
                auto jobMetricsDelta = controller->PullJobMetricsDelta(/*force*/ flushJobMetrics);
                ToProto(protoOperation->mutable_job_metrics(), jobMetricsDelta);
            }

            if (preparedRequest.OperationAlertsSent || sentAlerts) {
                auto* protoAlerts = protoOperation->mutable_alerts();
                for (const auto& [alertType, alert] : controller->GetAlerts()) {
                    auto* protoAlert = protoAlerts->add_alerts();
                    protoAlert->set_type(static_cast<int>(alertType));
                    ToProto(protoAlert->mutable_error(), alert);
                }
            }

            if (preparedRequest.SuspiciousJobsSent) {
                protoOperation->set_suspicious_jobs(controller->GetSuspiciousJobsYson().ToString());
            }

            ToProto(
                protoOperation->mutable_composite_needed_resources(),
                controller->GetNeededResources());
            ToProto(
                protoOperation->mutable_min_needed_resources(),
                controller->GetMinNeededAllocationResources());
        }

        request->set_exec_nodes_requested(preparedRequest.ExecNodesRequested);

        if (auto controllerMemoryLimit = Config_->MemoryWatchdog->TotalControllerMemoryLimit) {
            request->set_controller_memory_limit(*controllerMemoryLimit);
            request->set_controller_memory_usage(
                tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes").value_or(0));
        }

        return preparedRequest;
    }

    void ConfirmHeartbeatRequest(const TPreparedHeartbeatRequest& preparedRequest)
    {
        auto now = TInstant::Now();
        if (preparedRequest.ExecNodesRequested) {
            LastExecNodesUpdateTime_ = now;
        }
        if (preparedRequest.OperationsSent) {
            LastOperationsSendTime_ = now;
        }
        if (preparedRequest.OperationJobMetricsSent) {
            LastOperationJobMetricsSendTime_ = now;
        }
        if (preparedRequest.OperationAlertsSent) {
            LastOperationAlertsSendTime_ = now;
        }
        if (preparedRequest.SuspiciousJobsSent) {
            LastSuspiciousJobsSendTime_ = now;
        }
    }

    bool HasExecNodes(const TIntrusivePtr<TTypedClientResponse<NScheduler::NProto::TRspHeartbeat>>& rsp) const
    {
        return !rsp->Attachments().empty() && !rsp->Attachments()[0].Empty();
    }

    NScheduler::NProto::TExecNodeDescriptorList
    GetExecNodeDescriptorList(const TIntrusivePtr<TTypedClientResponse<NScheduler::NProto::TRspHeartbeat>>& rsp) const
    {
        NScheduler::NProto::TExecNodeDescriptorList descriptorList;
        DeserializeProtoWithEnvelope(&descriptorList, rsp->Attachments()[0]);

        return descriptorList;
    }

    void UpdateExecNodeDescriptors(const NScheduler::NProto::TExecNodeDescriptorList& descriptorList)
    {
        int onlineExecNodeCount = 0;
        auto execNodeDescriptors = New<TRefCountedExecNodeDescriptorMap>();

        for (const auto& protoDescriptor : descriptorList.exec_nodes()) {
            auto descriptor = New<TExecNodeDescriptor>();
            FromProto(descriptor.Get(), protoDescriptor);
            if (descriptor->Online) {
                ++onlineExecNodeCount;
            }
            EmplaceOrCrash(
                *execNodeDescriptors,
                protoDescriptor.node_id(),
                std::move(descriptor));
        }

        JobTracker_->UpdateExecNodes(execNodeDescriptors);

        {
            auto guard = WriterGuard(ExecNodeDescriptorsLock_);
            std::swap(CachedExecNodeDescriptors_, execNodeDescriptors);
            OnlineExecNodeCount_ = onlineExecNodeCount;
        }
    }

    void SendHeartbeat()
    {
        auto preparedRequest = PrepareHeartbeatRequest();

        YT_LOG_DEBUG("Sending heartbeat (ExecNodesRequested: %v, OperationsSent: %v, OperationAlertsSent: %v, SuspiciousJobsSent: %v, "
            "OperationEventCount: %v, JobEventCount: %v)",
            preparedRequest.ExecNodesRequested,
            preparedRequest.OperationsSent,
            preparedRequest.OperationAlertsSent,
            preparedRequest.SuspiciousJobsSent,
            preparedRequest.RpcRequest->agent_to_scheduler_operation_events().items_size());

        auto rspOrError = WaitFor(preparedRequest.RpcRequest->Invoke());
        if (!rspOrError.IsOK()) {
            if (NRpc::IsRetriableError(rspOrError)) {
                YT_LOG_WARNING(rspOrError, "Error reporting heartbeat to scheduler");
                TDelayedExecutor::WaitForDuration(Config_->SchedulerHeartbeatFailureBackoff);
            } else {
                Disconnect(rspOrError);
            }
            return;
        }

        YT_LOG_DEBUG("Heartbeat succeeded");

        const auto& rsp = rspOrError.Value();

        OperationEventsOutbox_->HandleStatus(rsp->agent_to_scheduler_operation_events());
        RunningAllocationStatisticsUpdatesOutbox_->HandleStatus(rsp->agent_to_scheduler_running_allocation_statistics_updates());

        HandleAbortedAllocationEvents(rsp);
        HandleOperationEvents(rsp);

        if (HasExecNodes(rsp)) {
            GetExecNodesUpdateInvoker()->Invoke(BIND(&TImpl::UpdateExecNodeDescriptors, MakeStrong(this), GetExecNodeDescriptorList(rsp)));
        }

        for (const auto& protoOperationId : rsp->operation_ids_to_unregister()) {
            auto operationId = FromProto<TOperationId>(protoOperationId);
            auto operation = FindOperation(operationId);
            if (!operation) {
                YT_LOG_DEBUG("Requested to unregister an unknown operation; ignored (OperationId: %v)",
                    operationId);
                continue;
            }
            UnregisterOperation(operation->GetId());
        }

        JobReporter_->SetOperationsArchiveVersion(rsp->operations_archive_version());

        ConfirmHeartbeatRequest(preparedRequest);
    }

    void BuildOutcomingScheduleAllocationResponses(
        const TControllerAgentTrackerServiceProxy::TReqScheduleAllocationHeartbeatPtr&  req)
    {
        ScheduleAllocationResponsesOutbox_->BuildOutcoming(
            req->mutable_agent_to_scheduler_schedule_allocation_responses(),
            [] (auto* protoResponse, const auto& response) {
                const auto& scheduleJobResult = *response.Result;
                ToProto(protoResponse->mutable_allocation_id(), response.AllocationId);
                ToProto(protoResponse->mutable_operation_id(), response.OperationId);
                protoResponse->set_controller_epoch(scheduleJobResult.ControllerEpoch.Underlying());
                protoResponse->set_success(static_cast<bool>(scheduleJobResult.StartDescriptor));
                if (scheduleJobResult.StartDescriptor) {
                    const auto& startDescriptor = *scheduleJobResult.StartDescriptor;
                    YT_ASSERT(response.AllocationId == startDescriptor.Id);
                    ToProto(protoResponse->mutable_resource_limits(), startDescriptor.ResourceLimits);
                }
                protoResponse->set_duration(ToProto<i64>(scheduleJobResult.Duration));
                if (scheduleJobResult.NextDurationEstimate) {
                    protoResponse->set_next_duration_estimate(ToProto<i64>(*scheduleJobResult.NextDurationEstimate));
                }
                for (auto reason : TEnumTraits<EScheduleAllocationFailReason>::GetDomainValues()) {
                    if (scheduleJobResult.Failed[reason] > 0) {
                        auto* protoCounter = protoResponse->add_failed();
                        protoCounter->set_reason(static_cast<int>(reason));
                        protoCounter->set_value(scheduleJobResult.Failed[reason]);
                    }
                }
            });
    }

    void HandleScheduleAllocationRequests(
        const TControllerAgentTrackerServiceProxy::TRspScheduleAllocationHeartbeatPtr& rsp,
        TRequestId requestId,
        const TRefCountedExecNodeDescriptorMapPtr& execNodeDescriptors)
    {
        auto outbox = ScheduleAllocationResponsesOutbox_;

        auto replyWithFailure = [=] (TOperationId operationId, TAllocationId allocationId, EScheduleAllocationFailReason reason) {
            TAgentToSchedulerScheduleAllocationResponse response;
            response.AllocationId = allocationId;
            response.OperationId = operationId;
            response.Result = New<TControllerScheduleAllocationResult>();
            response.Result->RecordFail(reason);
            outbox->Enqueue(std::move(response));
        };

        // Some failures are handled in HandleScheduleAllocationRequests function, even though they are definitely related
        // to controller. We want to see their statistics in operation progress, so we have to account them in controller's
        // ScheduleAllocationStatistics.
        auto replyWithFailureAndRecordInController = [=] (
            TOperationId operationId,
            TAllocationId allocationId,
            EScheduleAllocationFailReason reason,
            IOperationControllerPtr controller)
        {
            replyWithFailure(operationId, allocationId, reason);
            controller->RecordScheduleAllocationFailure(reason);
        };

        ScheduleAllocationRequestsInbox_->HandleIncoming(
            rsp->mutable_scheduler_to_agent_schedule_allocation_requests(),
            [&] (auto* protoRequest) {
                auto allocationId = FromProto<TAllocationId>(protoRequest->allocation_id());
                auto operationId = FromProto<TOperationId>(protoRequest->operation_id());
                YT_LOG_DEBUG(
                    "Processing schedule allocation request (OperationId: %v, AllocationId: %v)",
                    operationId,
                    allocationId);

                auto operation = this->FindOperation(operationId);
                if (!operation) {
                    replyWithFailure(operationId, allocationId, EScheduleAllocationFailReason::UnknownOperation);
                    YT_LOG_DEBUG(
                        "Failed to schedule allocation due to unknown operation (OperationId: %v, AllocationId: %v)",
                        operationId,
                        allocationId);
                    return;
                }

                auto controller = operation->GetController();

                if (controller->IsThrottling()) {
                    YT_LOG_DEBUG(
                        "Schedule allocation request skipped since controller is throttling (OperationId: %v, AllocationId: %v)",
                        operationId,
                        allocationId);
                    replyWithFailureAndRecordInController(
                        operationId,
                        allocationId,
                        EScheduleAllocationFailReason::ControllerThrottling,
                        controller);
                    return;
                }

                auto scheduleAllocationInvoker = controller->GetCancelableInvoker(Config_->ScheduleAllocationControllerQueue);
                auto requestDequeueInstant = TInstant::Now();

                auto traceContext = TTraceContext::NewChildFromRpc(
                    protoRequest->tracing_ext(),
                    /*spanName*/ Format("ScheduleAllocation:%v", allocationId),
                    requestId,
                    /*forceTracing*/ false);

                GuardedInvoke(
                    scheduleAllocationInvoker,
                    BIND([=, rsp = rsp, this_ = MakeStrong(this)] {
                        TCurrentTraceContextGuard traceContextGuard(traceContext);

                        auto controllerInvocationInstant = TInstant::Now();

                        YT_LOG_DEBUG(
                            "Processing schedule allocation in controller invoker (OperationId: %v, AllocationId: %v, InvocationWaitDuration: %v)",
                            operationId,
                            allocationId,
                            controllerInvocationInstant - requestDequeueInstant);

                        if (controller->IsThrottling()) {
                            YT_LOG_DEBUG(
                                "Schedule allocation request skipped since controller is throttling (OperationId: %v, AllocationId: %v)",
                                operationId,
                                allocationId);
                            replyWithFailureAndRecordInController(
                                operationId,
                                allocationId,
                                EScheduleAllocationFailReason::ControllerThrottling,
                                controller);
                            return;
                        }

                        auto nodeId = NodeIdFromAllocationId(allocationId);
                        auto descriptorIt = execNodeDescriptors->find(nodeId);
                        if (descriptorIt == execNodeDescriptors->end()) {
                            YT_LOG_DEBUG(
                                "Failed to schedule allocation due to unknown node (OperationId: %v, AllocationId: %v, NodeId: %v)",
                                operationId,
                                allocationId,
                                nodeId);
                            replyWithFailureAndRecordInController(
                                operationId,
                                allocationId,
                                EScheduleAllocationFailReason::UnknownNode,
                                controller);
                            return;
                        }

                        const auto& execNodeDescriptor = *descriptorIt->second;
                        if (!execNodeDescriptor.Online) {
                            YT_LOG_DEBUG(
                                "Failed to schedule allocation due to node is offline (OperationId: %v, AllocationId: %v, NodeId: %v)",
                                operationId,
                                allocationId,
                                nodeId);
                            replyWithFailureAndRecordInController(
                                operationId,
                                allocationId,
                                EScheduleAllocationFailReason::NodeOffline,
                                controller);
                            return;
                        }

                        auto allocationLimits = FromProto<TJobResources>(protoRequest->allocation_resource_limits());
                        const auto& treeId = protoRequest->tree_id();

                        TAgentToSchedulerScheduleAllocationResponse response;
                        TSchedulingContext context(protoRequest, descriptorIt->second, protoRequest->spec());

                        response.OperationId = operationId;
                        response.AllocationId = allocationId;
                        response.Result = controller->ScheduleAllocation(
                            &context,
                            allocationLimits,
                            treeId);
                        auto scheduleAllocationFinishInstant = TInstant::Now();
                        YT_LOG_DEBUG(
                            "Schedule allocation finished (OperationId: %v, AllocationId: %v, ScheduleAllocationDuration: %v)",
                            operationId,
                            allocationId,
                            scheduleAllocationFinishInstant - controllerInvocationInstant);

                        if (!response.Result) {
                            response.Result = New<TControllerScheduleAllocationResult>();
                        }

                        outbox->Enqueue(std::move(response));
                        YT_LOG_DEBUG(
                            "Allocation schedule response enqueued (OperationId: %v, AllocationId: %v)",
                            operationId,
                            allocationId);
                    }),
                    BIND([=, this_ = MakeStrong(this)] {
                        YT_LOG_DEBUG(
                            "Failed to schedule job due to operation cancelation (OperationId: %v, AllocationId: %v)",
                            operationId,
                            allocationId);
                        replyWithFailure(
                            operationId,
                            allocationId,
                            EScheduleAllocationFailReason::UnknownOperation);
                    }));
            });
    }

    void SendScheduleAllocationHeartbeat()
    {
        auto req = SchedulerProxy_.ScheduleAllocationHeartbeat();

        req->SetTimeout(Config_->SchedulerHeartbeatRpcTimeout);
        req->SetRequestHeavy(true);
        req->SetResponseHeavy(true);
        req->set_agent_id(Bootstrap_->GetAgentId());
        ToProto(req->mutable_incarnation_id(), IncarnationId_);
        GenerateMutationId(req);

        BuildOutcomingScheduleAllocationResponses(req);

        ScheduleAllocationRequestsInbox_->ReportStatus(req->mutable_scheduler_to_agent_schedule_allocation_requests());

        YT_LOG_DEBUG(
            "Sending schedule jobs heartbeat (ScheduleAllocationResponseCount: %v)",
            req->agent_to_scheduler_schedule_allocation_responses().items_size());

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            if (NRpc::IsRetriableError(rspOrError)) {
                YT_LOG_WARNING(rspOrError, "Error reporting heartbeat to scheduler");
                TDelayedExecutor::WaitForDuration(Config_->SchedulerHeartbeatFailureBackoff);
            } else {
                Disconnect(rspOrError);
            }
            return;
        }

        YT_LOG_DEBUG("Schedule jobs heartbeat succeeded");

        const auto& rsp = rspOrError.Value();

        ScheduleAllocationResponsesOutbox_->HandleStatus(rsp->agent_to_scheduler_schedule_allocation_responses());

        HandleScheduleAllocationRequests(rsp, req->GetRequestId(), GetExecNodeDescriptors({}));
    }

    void HandleAbortedAllocationEvents(const TControllerAgentTrackerServiceProxy::TRspHeartbeatPtr& rsp)
    {
        auto abortedAllocationSummariesPerOperationIdOrError = WaitFor(BIND([this, rsp] {
                THashMap<TOperationId, std::vector<TAbortedAllocationSummary>> abortedAllocationSummariesPerOperationId;
                abortedAllocationSummariesPerOperationId.reserve(rsp->scheduler_to_agent_aborted_allocation_events().items_size());

                AbortedAllocationEventsInbox_->HandleIncoming<TAbortedAllocationSummary>(
                    rsp->mutable_scheduler_to_agent_aborted_allocation_events(),
                    [&] (TAbortedAllocationSummary&& abortedAllocationSummary) {
                        abortedAllocationSummariesPerOperationId[abortedAllocationSummary.OperationId].push_back(
                            std::move(abortedAllocationSummary));
                    });

                return abortedAllocationSummariesPerOperationId;
            })
            .AsyncVia(JobEventsInvoker_)
            .Run());

        YT_LOG_FATAL_UNLESS(
            abortedAllocationSummariesPerOperationIdOrError.IsOK(),
            abortedAllocationSummariesPerOperationIdOrError,
            "Failed to parse scheduler message");

        auto abortedAllocationSummariesPerOperationId = std::move(abortedAllocationSummariesPerOperationIdOrError.Value());

        for (auto& [operationId, abortedAllocationSummaries] : abortedAllocationSummariesPerOperationId) {
            auto operation = this->FindOperation(operationId);
            if (!operation) {
                YT_LOG_DEBUG(
                    "Skip allocation abort events since operation is not running (OperationId: %v, AbortedAllocationCount: %v)",
                    operationId,
                    std::size(abortedAllocationSummaries));
                continue;
            }

            const auto& jobTrackerOperationHandler = operation->GetHost()->GetJobTrackerOperationHandler();

            jobTrackerOperationHandler->OnAllocationsAborted(std::move(abortedAllocationSummaries));
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

                auto controller = operation->GetController();
                controller->GetCancelableInvoker(EOperationControllerQueue::Default)->Invoke(
                    BIND([controller, eventType] {
                        switch (eventType) {
                            case ESchedulerToAgentOperationEventType::UpdateMinNeededAllocationResources:
                                controller->UpdateMinNeededAllocationResources();
                                break;

                            default:
                                YT_ABORT();
                        }
                    }));
            });
    }

    // TODO(ignat): eliminate this copy/paste from scheduler.cpp somehow.
    TFilteredExecNodeDescriptors FilterExecNodes(const TSchedulingTagFilter& filter) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(ExecNodeDescriptorsLock_);

        TFilteredExecNodeDescriptors result;
        result.All = New<TRefCountedExecNodeDescriptorMap>();
        result.Online = New<TRefCountedExecNodeDescriptorMap>();

        TJobResources maxAvailableResources;
        for (const auto& [nodeId, descriptor] : *CachedExecNodeDescriptors_) {
            if (filter.CanSchedule(descriptor->Tags)) {
                YT_VERIFY(result.All->emplace(nodeId, descriptor).second);
                if (descriptor->Online) {
                    YT_VERIFY(result.Online->emplace(nodeId, descriptor).second);
                }
                maxAvailableResources = Max(maxAvailableResources, descriptor->ResourceLimits);
            }
        }

        result.MaxAvailableResources = maxAvailableResources;

        YT_LOG_DEBUG("Exec nodes filtered "
            "(Formula: %v, MatchingNodeCount: %v, MatchingOnlineNodeCount: %v, MaxAvailableResources: %v)",
            filter.GetBooleanFormula().GetFormula(),
            result.All->size(),
            result.Online->size(),
            result.MaxAvailableResources);

        return result;
    }

    void BuildStaticOrchid(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Building static orchid");

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("service").BeginMap()
                    // This information used by scheduler_uptime odin check and we want
                    // to receive all these fields by single request.
                    .Item("connected").Value(IsConnected())
                    .Item("last_connection_time").Value(ConnectionTime_)
                    .Item("controller_agent_version").Value(GetVersion())
                    .Item("scheduler_version").Value(SchedulerVersion_)
                    .Item("hostname").Value(GetDefaultAddress(Bootstrap_->GetLocalAddresses()))
                .EndMap()
                .DoIf(Connected_, [&] (TFluentMap fluent) {
                    fluent
                        .Item("incarnation_id").Value(IncarnationId_);
                })
                .Item("config").Value(Config_)
                .Item("config_revision").Value(MasterConnector_->GetConfigRevision())
                .Item("tagged_memory_statistics")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .DoList([&] (TFluentList fluent) {
                        // YSON representation of operations, their allocation tags and memory usages.
                        auto memoryUsageStatistic = BuildYsonStringFluently<NYson::EYsonType::ListFragment>();

                        memoryUsageStatistic.DoIf(
                            IsConnected(),
                            [&] (TFluentList fluent) {
                                fluent.DoFor(
                                    GetOperations(),
                                    [&] (TFluentList fluent, const auto& pair) {
                                        fluent.Item()
                                            .BeginMap()
                                                .Item("usage").Value(pair.second->GetController()->GetMemoryUsage())
                                                .Item("operation_id").Value(pair.first)
                                                // COMPAT(ni-stoiko): Alive flag of tagged_memory_statistics will be deprecated since 23.2.
                                                // All operations will be alive.
                                                .Item("alive").Value(true)
                                            .EndMap();
                                    });
                            });

                        fluent.GetConsumer()->OnRaw(memoryUsageStatistic.Finish());
                    })
                .Item("medium_directory").Value(
                    Bootstrap_
                        ->GetClient()
                        ->GetNativeConnection()
                        ->GetMediumDirectory()
                )
                .Item("snapshot_version").Value(ToUnderlying(GetCurrentSnapshotVersion()))
                .Item("snapshotted_operation_ids")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .DoListFor(IdToOperation_, [&] (TFluentList fluent, const std::pair<TOperationId, TOperationPtr>& it) {
                        if (it.second->GetController()->HasSnapshot()) {
                            fluent.Item().Value(ToString(it.first));
                        }
                    })
            .EndMap();

        YT_LOG_DEBUG("Static orchid built");
    }

    IYPathServicePtr GetDynamicOrchidService()
    {
        auto dynamicOrchidService = New<TCompositeMapService>();
        dynamicOrchidService->AddChild("operations", New<TOperationsService>(this));
        return dynamicOrchidService;
    }

    IYPathServicePtr GetJobTrackerOrchidService()
    {
        auto service = New<TCompositeMapService>();
        service->AddChild("job_tracker", JobTracker_->GetOrchidService());
        return service;
    }

    class TOperationsService
        : public TVirtualMapBase
    {
    public:
        explicit TOperationsService(const TControllerAgent::TImpl* controllerAgent)
            : TVirtualMapBase(nullptr /*owningNode*/)
            , ControllerAgent_(controllerAgent)
        { }

        i64 GetSize() const override
        {
            return ControllerAgent_->IdToOperation_.size();
        }

        std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            keys.reserve(limit);
            for (const auto& [operationId, operation] : ControllerAgent_->IdToOperation_) {
                if (static_cast<i64>(keys.size()) >= limit) {
                    break;
                }
                keys.emplace_back(ToString(operationId));
            }
            const auto& zombieOperationOrchids = ControllerAgent_->ZombieOperationOrchids_->GetOperationIdToOrchidMap();
            for (const auto& [operationId, orchid] : zombieOperationOrchids) {
                if (static_cast<i64>(keys.size()) >= limit) {
                    break;
                }
                keys.emplace_back(ToString(operationId));
            }
            return keys;
        }

        IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (!ControllerAgent_->IsConnected()) {
                return nullptr;
            }

            auto operationId = TOperationId(TGuid::FromString(key));
            if (auto operation = ControllerAgent_->FindOperation(operationId)) {
                return operation->GetController()->GetOrchid();
            }

            const auto& idToZombieOperationOrchid = ControllerAgent_->ZombieOperationOrchids_->GetOperationIdToOrchidMap();
            if (auto it = idToZombieOperationOrchid.find(operationId); it != idToZombieOperationOrchid.end()) {
                return it->second;
            }

            return nullptr;
        }

    private:
        const TControllerAgent::TImpl* const ControllerAgent_;
    };
};

////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(
    TControllerAgentConfigPtr config,
    INodePtr configNode,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), std::move(configNode), bootstrap))
{ }

TControllerAgent::~TControllerAgent() = default;

void TControllerAgent::Initialize()
{
    Impl_->Initialize();
}

IYPathServicePtr TControllerAgent::CreateOrchidService()
{
    return Impl_->CreateOrchidService();
}

const IInvokerPtr& TControllerAgent::GetControllerThreadPoolInvoker()
{
    return Impl_->GetControllerThreadPoolInvoker();
}

const IInvokerPtr& TControllerAgent::GetJobSpecBuildPoolInvoker()
{
    return Impl_->GetJobSpecBuildPoolInvoker();
}

const IInvokerPtr& TControllerAgent::GetStatisticsOffloadInvoker()
{
    return Impl_->GetStatisticsOffloadInvoker();
}

const IInvokerPtr& TControllerAgent::GetExecNodesUpdateInvoker()
{
    return Impl_->GetExecNodesUpdateInvoker();
}

const IInvokerPtr& TControllerAgent::GetSnapshotIOInvoker()
{
    return Impl_->GetSnapshotIOInvoker();
}

TMasterConnector* TControllerAgent::GetMasterConnector()
{
    return Impl_->GetMasterConnector();
}

TJobTracker* TControllerAgent::GetJobTracker() const
{
    return Impl_->GetJobTracker();
}

TJobProfiler* TControllerAgent::GetJobProfiler() const
{
    return Impl_->GetJobProfiler();
}

bool TControllerAgent::IsConnected() const
{
    return Impl_->IsConnected();
}

TIncarnationId TControllerAgent::GetIncarnationId() const
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

void TControllerAgent::ValidateIncarnation(TIncarnationId incarnationId) const
{
    Impl_->ValidateIncarnation(incarnationId);
}

void TControllerAgent::Disconnect(const TError& error)
{
    Impl_->Disconnect(error);
}

const TControllerAgentConfigPtr& TControllerAgent::GetConfig() const
{
    return Impl_->GetConfig();
}

void TControllerAgent::UpdateConfig(const TControllerAgentConfigPtr& config)
{
    Impl_->UpdateConfig(config);
}

const TThrottlerManagerPtr& TControllerAgent::GetChunkLocationThrottlerManager() const
{
    return Impl_->GetChunkLocationThrottlerManager();
}

const ICoreDumperPtr& TControllerAgent::GetCoreDumper() const
{
    return Impl_->GetCoreDumper();
}

const TAsyncSemaphorePtr& TControllerAgent::GetCoreSemaphore() const
{
    return Impl_->GetCoreSemaphore();
}

const IEventLogWriterPtr& TControllerAgent::GetEventLogWriter() const
{
    return Impl_->GetEventLogWriter();
}

const TJobReporterPtr& TControllerAgent::GetJobReporter() const
{
    return Impl_->GetJobReporter();
}

IInvokerPtr TControllerAgent::CreateCancelableInvoker(const IInvokerPtr& invoker)
{
    return Impl_->CreateCancelableInvoker(invoker);
}

TOperationPtr TControllerAgent::FindOperation(TOperationId operationId)
{
    return Impl_->FindOperation(operationId);
}

TOperationPtr TControllerAgent::GetOperation(TOperationId operationId)
{
    return Impl_->GetOperation(operationId);
}

TOperationPtr TControllerAgent::GetOperationOrThrow(TOperationId operationId)
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

TFuture<TOperationControllerUnregisterResult> TControllerAgent::DisposeAndUnregisterOperation(TOperationId operationId)
{
    return Impl_->DisposeAndUnregisterOperation(operationId);
}

TFuture<void> TControllerAgent::UpdateOperationRuntimeParameters(TOperationId operationId, TOperationRuntimeParametersUpdatePtr update)
{
    return Impl_->UpdateOperationRuntimeParameters(operationId, std::move(update));
}

TFuture<std::optional<TOperationControllerInitializeResult>> TControllerAgent::InitializeOperation(
    const TOperationPtr& operation,
    const std::optional<TControllerTransactionIds>& transactions)
{
    return Impl_->InitializeOperation(
        operation,
        transactions);
}

TFuture<std::optional<TOperationControllerPrepareResult>> TControllerAgent::PrepareOperation(const TOperationPtr& operation)
{
    return Impl_->PrepareOperation(operation);
}

TFuture<std::optional<TOperationControllerMaterializeResult>> TControllerAgent::MaterializeOperation(const TOperationPtr& operation)
{
    return Impl_->MaterializeOperation(operation);
}

TFuture<std::optional<TOperationControllerReviveResult>> TControllerAgent::ReviveOperation(const TOperationPtr& operation)
{
    return Impl_->ReviveOperation(operation);
}

TFuture<std::optional<TOperationControllerCommitResult>> TControllerAgent::CommitOperation(const TOperationPtr& operation)
{
    return Impl_->CommitOperation(operation);
}

TFuture<void> TControllerAgent::CompleteOperation(const TOperationPtr& operation)
{
    return Impl_->CompleteOperation(operation);
}

TFuture<void> TControllerAgent::TerminateOperation(const TOperationPtr& operation, EControllerState controllerFinalState)
{
    return Impl_->TerminateOperation(operation, controllerFinalState);
}

TFuture<std::vector<TErrorOr<TJobStartInfo>>> TControllerAgent::SettleJobs(
    const std::vector<TSettleJobRequest>& requests)
{
    return Impl_->SettleJobs(requests);
}

TFuture<TOperationInfo> TControllerAgent::BuildOperationInfo(TOperationId operationId)
{
    return Impl_->BuildOperationInfo(operationId);
}

int TControllerAgent::GetOnlineExecNodeCount() const
{
    return Impl_->GetOnlineExecNodeCount();
}

TRefCountedExecNodeDescriptorMapPtr TControllerAgent::GetExecNodeDescriptors(const TSchedulingTagFilter& filter, bool onlineOnly) const
{
    return Impl_->GetExecNodeDescriptors(filter, onlineOnly);
}

TJobResources TControllerAgent::GetMaxAvailableResources(const TSchedulingTagFilter& filter) const
{
    return Impl_->GetMaxAvailableResources(filter);
}

const IThroughputThrottlerPtr& TControllerAgent::GetJobSpecSliceThrottler() const
{
    return Impl_->GetJobSpecSliceThrottler();
}

void TControllerAgent::ValidateOperationAccess(
    const TString& user,
    TOperationId operationId,
    NYTree::EPermission permission)
{
    return Impl_->ValidateOperationAccess(user, operationId, permission);
}

std::optional<TJobMonitoringDescriptor> TControllerAgent::TryAcquireJobMonitoringDescriptor(TOperationId operationId)
{
    return Impl_->TryAcquireJobMonitoringDescriptor(operationId);
}

bool TControllerAgent::ReleaseJobMonitoringDescriptor(TOperationId operationId, TJobMonitoringDescriptor descriptor)
{
    return Impl_->ReleaseJobMonitoringDescriptor(operationId, descriptor);
}

DELEGATE_SIGNAL(TControllerAgent, void(), SchedulerConnecting, *Impl_);
DELEGATE_SIGNAL(TControllerAgent, void(TIncarnationId), SchedulerConnected, *Impl_);
DELEGATE_SIGNAL(TControllerAgent, void(), SchedulerDisconnected, *Impl_);

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
