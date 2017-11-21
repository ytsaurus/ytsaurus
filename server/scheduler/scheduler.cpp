#include "scheduler.h"
#include "private.h"
#include "event_log.h"
#include "fair_share_strategy.h"
#include "helpers.h"
#include "job_prober_service.h"
#include "master_connector.h"
#include "node_shard.h"
#include "scheduler_strategy.h"
#include "scheduling_tag.h"
#include "cache.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/operation_controller.h>
#include <yt/server/controller_agent/master_connector.h>
#include <yt/server/controller_agent/controller_agent.h>

#include <yt/server/exec_agent/public.h>

#include <yt/server/cell_scheduler/bootstrap.h>
#include <yt/server/cell_scheduler/config.h>

#include <yt/server/shell/config.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/ytlib/node_tracker_client/channel.h>
#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_buffered_table_writer.h>
#include <yt/ytlib/table_client/schemaless_writer.h>
#include <yt/ytlib/table_client/table_consumer.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/response_keeper.h>

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/size_literals.h>

#include <yt/core/profiling/timing.h>
#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/ytree/service_combiner.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/exception_helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NProfiling;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NRpc;
using namespace NApi;
using namespace NCellScheduler;
using namespace NObjectClient;
using namespace NHydra;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient;
using namespace NChunkClient;
using namespace NJobProberClient;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NSecurityClient;
using namespace NShell;

using NControllerAgent::TControllerTransactionsPtr;
using NControllerAgent::IOperationController;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;
using NNodeTrackerClient::TNodeDirectory;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

template <class K, class V>
THashMap<K, V> FilterLargestValues(const THashMap<K, V>& input, size_t threshold)
{
    threshold = std::min(threshold, input.size());
    std::vector<std::pair<K, V>> items(input.begin(), input.end());
    std::partial_sort(
        items.begin(),
        items.begin() + threshold,
        items.end(),
        [] (const std::pair<K, V>& lhs, const std::pair<K, V>& rhs) {
            return lhs.second > rhs.second;
        });
    return THashMap<K, V>(items.begin(), items.begin() + threshold);
}

////////////////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public TRefCounted
    , public NControllerAgent::IOperationHost
    , public ISchedulerStrategyHost
    , public INodeShardHost
    , public TEventLogHostBase
{
public:
    using TEventLogHostBase::LogEventFluently;

    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , InitialConfig_(Config_)
        , Bootstrap_(bootstrap)
        , SnapshotIOQueue_(New<TActionQueue>("SnapshotIO"))
        , ControllerThreadPool_(New<TThreadPool>(Config_->ControllerThreadCount, "Controller"))
        , StatisticsAnalyzerThreadPool_(New<TThreadPool>(Config_->StatisticsAnalyzerThreadCount, "Statistics"))
        , ReconfigurableJobSpecSliceThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->JobSpecSliceThrottler,
            NLogging::TLogger(),
            NProfiling::TProfiler(SchedulerProfiler.GetPathPrefix() + "/job_spec_slice_throttler")))
        , JobSpecSliceThrottler_(ReconfigurableJobSpecSliceThrottler_)
        , ChunkLocationThrottlerManager_(New<TThrottlerManager>(
            Config_->ChunkLocationThrottler,
            SchedulerLogger))
        , MasterConnector_(std::make_unique<TMasterConnector>(Config_, Bootstrap_))
        , CachedExecNodeDescriptorsByTags_(New<TExpiringCache<TSchedulingTagFilter, TExecNodeDescriptorListPtr>>(
            BIND(&TImpl::CalculateExecNodeDescriptors, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout,
            GetControlInvoker()))
        , CachedExecNodeMemoryDistributionByTags_(New<TExpiringCache<TSchedulingTagFilter, TMemoryDistribution>>(
            BIND(&TImpl::CalculateMemoryDistribution, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout,
            GetControlInvoker()))
        , TotalResourceLimitsProfiler_(Profiler.GetPathPrefix() + "/total_resource_limits")
        , MainNodesResourceLimitsProfiler_(Profiler.GetPathPrefix() + "/main_nodes_resource_limits")
        , TotalResourceUsageProfiler_(Profiler.GetPathPrefix() + "/total_resource_usage")
        , TotalCompletedJobTimeCounter_("/total_completed_job_time")
        , TotalFailedJobTimeCounter_("/total_failed_job_time")
        , TotalAbortedJobTimeCounter_("/total_aborted_job_time")
        , CoreSemaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentSafeCoreDumps))
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_THREAD_AFFINITY(GetControlInvoker(), ControlThread);

        auto primaryMasterCellTag = Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetPrimaryMasterCellTag();

        for (int index = 0; index < Config_->NodeShardCount; ++index) {
            NodeShards_.push_back(New<TNodeShard>(
                index,
                primaryMasterCellTag,
                Config_,
                this,
                Bootstrap_));
        }

        ServiceAddress_ = BuildServiceAddress(
            GetLocalHostName(),
            Bootstrap_->GetConfig()->RpcPort);

        for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
            JobStateToTag_[state] = TProfileManager::Get()->RegisterTag("state", FormatEnum(state));
        }

        for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
            JobTypeToTag_[type] = TProfileManager::Get()->RegisterTag("job_type", FormatEnum(type));
        }

        for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
            if (IsSentinelReason(reason)) {
                continue;
            }
            JobAbortReasonToTag_[reason] = TProfileManager::Get()->RegisterTag("abort_reason", FormatEnum(reason));
        }

        for (auto reason : TEnumTraits<EInterruptReason>::GetDomainValues()) {
            JobInterruptReasonToTag_[reason] = TProfileManager::Get()->RegisterTag("interrupt_reason", FormatEnum(reason));
        }
    }

    void Initialize()
    {
        InitStrategy();

        MasterConnector_->AddGlobalWatcherRequester(BIND(
            &TImpl::RequestPools,
            Unretained(this)));
        MasterConnector_->AddGlobalWatcherHandler(BIND(
            &TImpl::HandlePools,
            Unretained(this)));

        MasterConnector_->AddGlobalWatcher(
            BIND(&TImpl::RequestNodesAttributes, Unretained(this)),
            BIND(&TImpl::HandleNodesAttributes, Unretained(this)),
            Config_->NodesAttributesUpdatePeriod);

        MasterConnector_->AddGlobalWatcherRequester(BIND(
            &TImpl::RequestConfig,
            Unretained(this)));
        MasterConnector_->AddGlobalWatcherHandler(BIND(
            &TImpl::HandleConfig,
            Unretained(this)));

        MasterConnector_->SubscribeMasterConnected(BIND(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        MasterConnector_->SubscribeMasterDisconnected(BIND(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));

        MasterConnector_->Start();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            Config_->ProfilingUpdatePeriod);
        ProfilingExecutor_->Start();

        auto nameTable = New<TNameTable>();
        auto options = New<NTableClient::TTableWriterOptions>();
        options->EnableValidationOptions();

        EventLogWriter_ = CreateSchemalessBufferedTableWriter(
            Config_->EventLog,
            options,
            GetMasterClient(),
            nameTable,
            Config_->EventLog->Path);

        // Open is always synchronous for buffered writer.
        YCHECK(EventLogWriter_->Open().IsSet());

        EventLogValueConsumer_.reset(new TWritingValueConsumer(EventLogWriter_, New<TTypeConversionConfig>(), true /* flushImmediately */));
        EventLogTableConsumer_.reset(new TTableConsumer(EventLogValueConsumer_.get()));

        LogEventFluently(ELogEventType::SchedulerStarted)
            .Item("address").Value(ServiceAddress_);

        LoggingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity),
            BIND(&TImpl::OnLogging, MakeWeak(this)),
            Config_->ClusterInfoLoggingPeriod);
        LoggingExecutor_->Start();

        PendingEventLogRowsFlushExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity),
            BIND(&TImpl::OnPendingEventLogRowsFlush, MakeWeak(this)),
            Config_->PendingEventLogRowsFlushPeriod);
        PendingEventLogRowsFlushExecutor_->Start();

        UpdateExecNodeDescriptorsExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity),
            BIND(&TImpl::UpdateExecNodeDescriptors, MakeWeak(this)),
            Config_->UpdateExecNodeDescriptorsPeriod);
        UpdateExecNodeDescriptorsExecutor_->Start();
    }

    IYPathServicePtr GetOrchidService()
    {
        auto staticOrchidProducer = BIND(&TImpl::BuildStaticOrchid, MakeStrong(this));
        auto staticOrchidService = IYPathService::FromProducer(staticOrchidProducer)
            ->Via(GetControlInvoker(EControlQueue::Orchid))
            ->Cached(Config_->StaticOrchidCacheUpdatePeriod);

        auto dynamicOrchidService = GetDynamicOrchidService()
            ->Via(GetControlInvoker(EControlQueue::Orchid));

        return New<TServiceCombiner>(std::vector<IYPathServicePtr> {
            staticOrchidService,
            dynamicOrchidService
        });
    }

    std::vector<TOperationPtr> GetOperations()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TOperationPtr> operations;
        for (const auto& pair : IdToOperation_) {
            operations.push_back(pair.second);
        }
        return operations;
    }

    IInvokerPtr GetSnapshotIOInvoker()
    {
        return SnapshotIOQueue_->GetInvoker();
    }

    bool IsConnected()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IsConnected_ && MasterConnector_->IsConnected();
    }

    void ValidateConnected()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!IsConnected()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Master is not connected");
        }
    }

    void ValidateAcceptsHeartbeats()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!IsConnected()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Scheduler is not able to accept heartbeats");
        }
    }

    virtual TInstant GetConnectionTime() const override
    {
        return ConnectionTime_;
    }

    TOperationPtr FindOperation(const TOperationId& id) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = IdToOperation_.find(id);
        return it == IdToOperation_.end() ? nullptr : it->second;
    }

    TOperationPtr GetOperation(const TOperationId& id) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(id);
        YCHECK(operation);
        return operation;
    }

    TOperationPtr GetOperationOrThrow(const TOperationId& id) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(id);
        if (!operation) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::NoSuchOperation,
                "No such operation %v",
                id);
        }
        return operation;
    }

    virtual int GetExecNodeCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int execNodeCount = 0;
        for (auto& nodeShard : NodeShards_) {
            execNodeCount += nodeShard->GetExecNodeCount();
        }
        return execNodeCount;
    }

    int GetTotalNodeCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int totalNodeCount = 0;
        for (auto& nodeShard : NodeShards_) {
            totalNodeCount += nodeShard->GetTotalNodeCount();
        }
        return totalNodeCount;
    }

    virtual TExecNodeDescriptorListPtr GetExecNodeDescriptors(const TSchedulingTagFilter& filter) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (filter.IsEmpty()) {
            TReaderGuard guard(ExecNodeDescriptorsLock_);

            return CachedExecNodeDescriptors_;
        }

        return CachedExecNodeDescriptorsByTags_->Get(filter);
    }

    virtual TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (filter.IsEmpty()) {
            TReaderGuard guard(ExecNodeDescriptorsLock_);

            return CachedExecNodeMemoryDistribution_;
        }

        return CachedExecNodeMemoryDistributionByTags_->Get(filter);
    }

    virtual void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!alert.IsOK()) {
            LOG_WARNING(alert, "Setting scheduler alert (AlertType: %lv)", alertType);
        }

        MasterConnector_->SetSchedulerAlert(alertType, alert);
    }

    virtual const TCoreDumperPtr& GetCoreDumper() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetCoreDumper();
    }

    virtual const TAsyncSemaphorePtr& GetCoreSemaphore() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CoreSemaphore_;
    }

    void DoSetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);
        if (!operation) {
            return;
        }

        operation->Alerts()[alertType] = alert;
    }

    virtual TFuture<void> SetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoSetOperationAlert, MakeStrong(this), operationId, alertType, alert)
            .AsyncVia(GetControlInvoker())
            .Run();
    }

    virtual IJobHostPtr GetJobHost(const TJobId& jobId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return CreateJobHost(jobId, nodeShard);
    }

    virtual TFuture<void> ReleaseJobs(const std::vector<TJobId>& jobIds) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<std::vector<TJobId>> jobIdsToRemoveByShardId(NodeShards_.size());
        for (const auto& jobId : jobIds) {
            int shardId = GetNodeShardId(NodeIdFromJobId(jobId));
            jobIdsToRemoveByShardId[shardId].emplace_back(jobId);
        }

        std::vector<TFuture<void>> submitFutures;
        for (int shardId = 0; shardId < NodeShards_.size(); ++shardId) {
            if (jobIdsToRemoveByShardId[shardId].empty()) {
                continue;
            }
            auto submitFuture = BIND(&TNodeShard::ReleaseJobs, NodeShards_[shardId])
                .AsyncVia(NodeShards_[shardId]->GetInvoker())
                .Run(std::move(jobIdsToRemoveByShardId[shardId]));
            submitFutures.emplace_back(std::move(submitFuture));
        }

        return Combine(submitFutures);
    }

    virtual void SendJobMetricsToStrategy(const TOperationId& operationId, const TJobMetrics& jobMetricsDelta) override
    {
        GetStrategy()->ApplyJobMetricsDelta(operationId, jobMetricsDelta);
    }

    virtual void ValidatePoolPermission(
        const TYPath& path,
        const TString& user,
        EPermission permission) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Validating permission %Qv of user %Qv on pool %Qv",
            permission,
            user,
            path);

        const auto& client = GetMasterClient();
        auto result = WaitFor(client->CheckPermission(user, GetPoolsPath() + path, permission))
            .ValueOrThrow();
        if (result.Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthorizationError,
                "User %Qv has been denied access to pool %v",
                user,
                path.empty() ? RootPoolName : path)
                << result.ToError(user, permission);
        }

        LOG_DEBUG("Pool permission successfully validated");
    }

    void ValidateOperationPermission(
        const TString& user,
        const TOperationId& operationId,
        EPermission permission) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_DEBUG("Validating permission %Qv of user %Qv on operation %v",
            permission,
            user,
            ToString(operationId));

        auto path = GetOperationPath(operationId);

        const auto& client = GetMasterClient();
        auto asyncResult = client->CheckPermission(user, path, permission);
        auto resultOrError = WaitFor(asyncResult);
        if (!resultOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error checking permission for operation %v",
                operationId)
                << resultOrError;
        }

        const auto& result = resultOrError.Value();
        if (result.Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthorizationError,
                "User %Qv has been denied access to operation %v",
                user,
                operationId);
        }

        ValidateConnected();

        LOG_DEBUG("Operation permission successfully validated");
    }

    TFuture<TOperationPtr> StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const TMutationId& mutationId,
        IMapNodePtr spec,
        const TString& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (static_cast<int>(IdToOperation_.size()) >= Config_->MaxOperationCount) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::TooManyOperations,
                "Limit for the total number of concurrent operations %v has been reached",
                Config_->MaxOperationCount);
        }

        // Merge operation spec with template
        auto specTemplate = GetSpecTemplate(type, spec);
        if (specTemplate) {
            spec = UpdateNode(specTemplate, spec)->AsMap();
        }

        TOperationSpecBasePtr operationSpec;
        try {
            operationSpec = ConvertTo<TOperationSpecBasePtr>(spec);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
        }

        // Create operation object.
        auto operationId = MakeRandomId(
            EObjectType::Operation,
            GetMasterClient()->GetNativeConnection()->GetPrimaryMasterCellTag());
        auto operation = New<TOperation>(
            operationId,
            type,
            mutationId,
            transactionId,
            spec,
            user,
            operationSpec->Owners,
            TInstant::Now(),
            GetControlInvoker(EControlQueue::Operation));
        operation->SetState(EOperationState::Initializing);

        WaitFor(Strategy_->ValidateOperationStart(operation))
            .ThrowOnError();

        LOG_INFO("Starting operation (OperationType: %v, OperationId: %v, TransactionId: %v, User: %v)",
            type,
            operationId,
            transactionId,
            user);

        LOG_INFO("Total resource limits (OperationId: %v, ResourceLimits: %v)",
            operationId,
            FormatResources(GetTotalResourceLimits()));

        // Spawn a new fiber where all startup logic will work asynchronously.
        BIND(&TImpl::DoStartOperation, MakeStrong(this), operation)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();

        return operation->GetStarted();
    }

    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error, const TString& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationPermission(user, operation->GetId(), EPermission::Write);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            LOG_INFO(error, "Operation is already shuting down (OperationId: %v, State: %v)",
                operation->GetId(),
                operation->GetState());
            return operation->GetFinished();
        }

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND([=, this_ = MakeStrong(this)] {
                DoAbortOperation(operation->GetId(), error);
            }));

        return operation->GetFinished();
    }

    TFuture<void> SuspendOperation(TOperationPtr operation, const TString& user, bool abortRunningJobs)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationPermission(user, operation->GetId(), EPermission::Write);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Cannot suspend operation in %Qlv state",
                operation->GetState()));
        }

        DoSuspendOperation(operation->GetId(), TError("Suspend operation by user request"), abortRunningJobs, /* setAlert */ false);

        return MasterConnector_->FlushOperationNode(operation);
    }

    TFuture<void> ResumeOperation(TOperationPtr operation, const TString& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationPermission(user, operation->GetId(), EPermission::Write);

        if (!operation->GetSuspended()) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Operation is not suspended. Its state %Qlv",
                operation->GetState()));
        }

        std::vector<TFuture<void>> resumeFutures;
        for (auto& nodeShard : NodeShards_) {
            resumeFutures.push_back(BIND(&TNodeShard::ResumeOperationJobs, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run(operation->GetId()));
        }
        WaitFor(Combine(resumeFutures))
            .ThrowOnError();

        operation->SetSuspended(false);

        SetOperationAlert(
            operation->GetId(),
            EOperationAlertType::OperationSuspended,
            TError());

        LOG_INFO("Operation resumed (OperationId: %v)",
            operation->GetId());

        return MasterConnector_->FlushOperationNode(operation);
    }

    TFuture<void> CompleteOperation(TOperationPtr operation, const TError& error, const TString& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationPermission(user, operation->GetId(), EPermission::Write);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            LOG_INFO(error, "Operation is already shutting down (OperationId: %v, State: %v)",
                operation->GetId(),
                operation->GetState());
            return operation->GetFinished();
        }
        if (operation->GetState() != EOperationState::Running) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Operation is not running. Its state is %Qlv",
                operation->GetState()));
        }

        LOG_INFO(error, "Completing operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        auto controller = operation->GetController();
        YCHECK(controller);
        controller->Complete();

        return operation->GetFinished();
    }

    TFuture<TYsonString> Strace(const TJobId& jobId, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return BIND(&TNodeShard::StraceJob, nodeShard, jobId, user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    TFuture<void> DumpInputContext(const TJobId& jobId, const TYPath& path, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return BIND(&TNodeShard::DumpJobInputContext, nodeShard, jobId, path, user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    TFuture<TNodeDescriptor> GetJobNode(const TJobId& jobId, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return BIND(&TNodeShard::GetJobNode, nodeShard, jobId, user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    TFuture<void> SignalJob(const TJobId& jobId, const TString& signalName, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return BIND(&TNodeShard::SignalJob, nodeShard, jobId, signalName, user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    TFuture<void> AbandonJob(const TJobId& jobId, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return BIND(&TNodeShard::AbandonJob, nodeShard, jobId, user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    TFuture<TYsonString> PollJobShell(const TJobId& jobId, const TYsonString& parameters, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return BIND(&TNodeShard::PollJobShell, nodeShard, jobId, parameters, user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    TFuture<void> AbortJob(const TJobId& jobId, const TNullable<TDuration>& interruptTimeout, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        // A neat way to choose the proper overload.
        typedef void (TNodeShard::*CorrectSignature)(const TJobId&, const TNullable<TDuration>&, const TString&);
        return BIND(static_cast<CorrectSignature>(&TNodeShard::AbortJob), nodeShard, jobId, interruptTimeout, user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    void ProcessHeartbeat(const TCtxHeartbeatPtr& context)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto* request = &context->Request();
        auto nodeId = request->node_id();

        const auto& nodeShard = GetNodeShard(nodeId);
        nodeShard->GetInvoker()->Invoke(BIND(&TNodeShard::ProcessHeartbeat, nodeShard, context));
    }

    // ISchedulerStrategyHost implementation
    virtual TJobResources GetTotalResourceLimits() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto totalResourceLimits = ZeroJobResources();
        for (auto& nodeShard : NodeShards_) {
            totalResourceLimits += nodeShard->GetTotalResourceLimits();
        }
        return totalResourceLimits;
    }

    virtual TJobResources GetMainNodesResourceLimits() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return GetResourceLimits(Config_->MainNodesFilter);
    }

    TJobResources GetTotalResourceUsage()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto totalResourceUsage = ZeroJobResources();
        for (const auto& nodeShard : NodeShards_) {
            totalResourceUsage += nodeShard->GetTotalResourceUsage();
        }
        return totalResourceUsage;
    }

    virtual TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto resourceLimits = ZeroJobResources();
        for (auto& nodeShard : NodeShards_) {
            resourceLimits += nodeShard->GetResourceLimits(filter);
        }
        return resourceLimits;
    }

    int GetActiveJobCount()
    {
        int activeJobCount = 0;
        for (auto& nodeShard : NodeShards_) {
             activeJobCount += nodeShard->GetActiveJobCount();
        }
        return activeJobCount;
    }

    virtual void ActivateOperation(const TOperationId& operationId) override
    {
        auto operation = GetOperation(operationId);

        auto codicilGuard = operation->MakeCodicilGuard();

        operation->SetActivated(true);
        if (operation->GetPrepared()) {
            MaterializeOperation(operation);
        }
    }

    void MaterializeOperation(TOperationPtr operation)
    {
        auto controller = operation->GetController();
        if (controller->IsRevivedFromSnapshot()) {
            operation->SetState(EOperationState::RevivingJobs);
            RegisterJobsFromRevivedOperation(operation)
                .Subscribe(BIND([operation] (const TError& error) {
                    YCHECK(error.IsOK() && "Error while registering jobs from the revived operation");
                    if (operation->GetState() == EOperationState::RevivingJobs) {
                        operation->SetState(EOperationState::Running);
                    }
                }));
        } else {
            operation->SetState(EOperationState::Materializing);
            BIND(&IOperationController::Materialize, controller)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run()
                .Subscribe(BIND([operation] (const TError& error) {
                    if (error.IsOK()) {
                        if (operation->GetState() == EOperationState::Materializing) {
                            operation->SetState(EOperationState::Running);
                        }
                    }
                })
                .Via(operation->GetCancelableControlInvoker()));
        }
    }


    // IOperationHost implementation
    virtual NControllerAgent::TMasterConnector* GetControllerAgentMasterConnector() override
    {
        return Bootstrap_->GetControllerAgent()->GetMasterConnector();
    }

    virtual const TSchedulerConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    virtual const NApi::INativeClientPtr& GetMasterClient() const override
    {
        return Bootstrap_->GetMasterClient();
    }

    virtual const TNodeDirectoryPtr& GetNodeDirectory() override
    {
        return Bootstrap_->GetNodeDirectory();
    }

    virtual IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const
    {
        return Bootstrap_->GetControlInvoker(queue);
    }

    virtual IInvokerPtr CreateOperationControllerInvoker() override
    {
        return CreateSerializedInvoker(ControllerThreadPool_->GetInvoker());
    }

    virtual const TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const override
    {
        return ChunkLocationThrottlerManager_;
    }

    virtual IYsonConsumer* GetEventLogConsumer() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return EventLogTableConsumer_.get();
    }

    virtual void OnOperationCompleted(const TOperationId& operationId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoCompleteOperation, MakeStrong(this), operationId));
    }

    virtual void OnOperationFailed(const TOperationId& operationId, const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoFailOperation, MakeStrong(this), operationId, error));
    }

    virtual void OnOperationSuspended(const TOperationId& operationId, const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoSuspendOperation, MakeStrong(this), operationId, error, /* abortRunningJobs */ true, /* setAlert */ true));
    }

    virtual void OnOperationAborted(const TOperationId& operationId, const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND([=, this_ = MakeStrong(this)] {
                DoAbortOperation(operationId, error);
            }));
    }

    virtual std::unique_ptr<IValueConsumer> CreateLogConsumer() override
    {
        return std::unique_ptr<IValueConsumer>(new TEventLogValueConsumer(this));
    }

    // INodeShardHost implementation
    virtual int GetNodeShardId(TNodeId nodeId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return nodeId % NodeShards_.size();
    }

    virtual const ISchedulerStrategyPtr& GetStrategy() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Strategy_;
    }

    const IInvokerPtr& GetStatisticsAnalyzerInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return StatisticsAnalyzerThreadPool_->GetInvoker();
    }

    virtual const IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return JobSpecSliceThrottler_;
    }

    TFuture<void> AttachJobContext(
        const NYTree::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&NControllerAgent::TControllerAgent::AttachJobContext, Bootstrap_->GetControllerAgent())
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run(path, chunkId, operationId, jobId);
    }

    TJobProberServiceProxy CreateJobProberProxy(const TString& address) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& channelFactory = GetMasterClient()->GetChannelFactory();
        auto channel = channelFactory->CreateChannel(address);

        TJobProberServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config_->JobProberRpcTimeout);
        return proxy;
    }

private:
    TSchedulerConfigPtr Config_;
    const TSchedulerConfigPtr InitialConfig_;
    TBootstrap* const Bootstrap_;

    const TActionQueuePtr SnapshotIOQueue_;
    const TThreadPoolPtr ControllerThreadPool_;
    const TThreadPoolPtr StatisticsAnalyzerThreadPool_;

    const IReconfigurableThroughputThrottlerPtr ReconfigurableJobSpecSliceThrottler_;
    const IThroughputThrottlerPtr JobSpecSliceThrottler_;

    const TThrottlerManagerPtr ChunkLocationThrottlerManager_;

    const std::unique_ptr<TMasterConnector> MasterConnector_;
    std::atomic<bool> IsConnected_ = {false};

    ISchedulerStrategyPtr Strategy_;

    TInstant ConnectionTime_;

    typedef THashMap<TOperationId, TOperationPtr> TOperationIdMap;
    TOperationIdMap IdToOperation_;

    TReaderWriterSpinLock ExecNodeDescriptorsLock_;
    TExecNodeDescriptorListPtr CachedExecNodeDescriptors_ = New<TExecNodeDescriptorList>();
    TIntrusivePtr<TExpiringCache<TSchedulingTagFilter, TExecNodeDescriptorListPtr>> CachedExecNodeDescriptorsByTags_;

    TMemoryDistribution CachedExecNodeMemoryDistribution_;
    TIntrusivePtr<TExpiringCache<TSchedulingTagFilter, TMemoryDistribution>> CachedExecNodeMemoryDistributionByTags_;


    TProfiler TotalResourceLimitsProfiler_;
    TProfiler MainNodesResourceLimitsProfiler_;
    TProfiler TotalResourceUsageProfiler_;

    TSimpleCounter TotalCompletedJobTimeCounter_;
    TSimpleCounter TotalFailedJobTimeCounter_;
    TSimpleCounter TotalAbortedJobTimeCounter_;

    TEnumIndexedVector<TTagId, EJobState> JobStateToTag_;
    TEnumIndexedVector<TTagId, EJobType> JobTypeToTag_;
    TEnumIndexedVector<TTagId, EAbortReason> JobAbortReasonToTag_;
    TEnumIndexedVector<TTagId, EInterruptReason> JobInterruptReasonToTag_;

    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr LoggingExecutor_;
    TPeriodicExecutorPtr PendingEventLogRowsFlushExecutor_;
    TPeriodicExecutorPtr UpdateExecNodeDescriptorsExecutor_;

    const TAsyncSemaphorePtr CoreSemaphore_;

    TString ServiceAddress_;

    std::vector<TNodeShardPtr> NodeShards_;

    class TEventLogValueConsumer
        : public IValueConsumer
    {
    public:
        explicit TEventLogValueConsumer(TScheduler::TImpl* host)
            : Host_(host)
        { }

        virtual const TNameTablePtr& GetNameTable() const override
        {
            return Host_->EventLogWriter_->GetNameTable();
        }

        virtual bool GetAllowUnknownColumns() const override
        {
            return true;
        }

        virtual void OnBeginRow() override
        { }

        virtual void OnValue(const TUnversionedValue& value) override
        {
            Builder_.AddValue(value);
        }

        virtual void OnEndRow() override
        {
            Host_->PendingEventLogRows_.Enqueue(Builder_.FinishRow());
        }

    private:
        TScheduler::TImpl* const Host_;
        TUnversionedOwningRowBuilder Builder_;
    };

    ISchemalessWriterPtr EventLogWriter_;
    std::unique_ptr<IValueConsumer> EventLogValueConsumer_;
    std::unique_ptr<IYsonConsumer> EventLogTableConsumer_;
    TMultipleProducerSingleConsumerLockFreeStack<TUnversionedOwningRow> PendingEventLogRows_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    const TNodeShardPtr& GetNodeShard(TNodeId nodeId) const
    {
        return NodeShards_[GetNodeShardId(nodeId)];
    }

    const TNodeShardPtr& GetNodeShardByJobId(TJobId jobId) const
    {
        auto nodeId = NodeIdFromJobId(jobId);
        return GetNodeShard(nodeId);
    }


    void ReleaseStderrChunk(const TOperationPtr& operation, const TChunkId& chunkId)
    {
        auto cellTag = CellTagFromId(chunkId);
        auto channel = GetMasterClient()->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Leader, cellTag);
        TChunkServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();
        auto req = batchReq->add_unstage_chunk_tree_subrequests();
        ToProto(req->mutable_chunk_tree_id(), chunkId);
        req->set_recursive(false);

        // Fire-and-forget.
        // The subscriber is only needed to log the outcome.
        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnStderrChunkReleased, MakeStrong(this)));
    }

    void OnStderrChunkReleased(const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        // NB: We only look at the topmost error and ignore subresponses.
        if (!batchRspOrError.IsOK()) {
            LOG_WARNING(batchRspOrError, "Error releasing stderr chunk");
        }
    }

    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TJobCounter> shardJobCounter(NodeShards_.size());
        std::vector<TAbortedJobCounter> shardAbortedJobCounter(NodeShards_.size());
        std::vector<TCompletedJobCounter> shardCompletedJobCounter(NodeShards_.size());

        for (int i = 0; i < NodeShards_.size(); ++i) {
            auto& nodeShard = NodeShards_[i];
            shardJobCounter[i] = nodeShard->GetJobCounter();
            shardAbortedJobCounter[i] = nodeShard->GetAbortedJobCounter();
            shardCompletedJobCounter[i] = nodeShard->GetCompletedJobCounter();
        }

        for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
            for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
                TTagIdList commonTags = {JobStateToTag_[state], JobTypeToTag_[type]};
                if (state == EJobState::Aborted) {
                    for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
                        if (IsSentinelReason(reason)) {
                            continue;
                        }
                        auto tags = commonTags;
                        tags.push_back(JobAbortReasonToTag_[reason]);
                        int counter = 0;
                        for (int i = 0; i < NodeShards_.size(); ++i) {
                            counter += shardAbortedJobCounter[i][reason][state][type];
                        }
                        Profiler.Enqueue("/job_count", counter, EMetricType::Counter, tags);
                    }
                } else if (state == EJobState::Completed) {
                    for (auto reason : TEnumTraits<EInterruptReason>::GetDomainValues()) {
                        auto tags = commonTags;
                        tags.push_back(JobInterruptReasonToTag_[reason]);
                        int counter = 0;
                        for (int i = 0; i < NodeShards_.size(); ++i) {
                            counter += shardCompletedJobCounter[i][reason][state][type];
                        }
                        Profiler.Enqueue("/job_count", counter, EMetricType::Counter, tags);
                    }
                } else {
                    int counter = 0;
                    for (int i = 0; i < NodeShards_.size(); ++i) {
                        counter += shardJobCounter[i][state][type];
                    }
                    Profiler.Enqueue("/job_count", counter, EMetricType::Counter, commonTags);
                }
            }
        }

        Profiler.Enqueue("/active_job_count", GetActiveJobCount(), EMetricType::Gauge);

        Profiler.Enqueue("/exec_node_count", GetExecNodeCount(), EMetricType::Gauge);
        Profiler.Enqueue("/total_node_count", GetTotalNodeCount(), EMetricType::Gauge);

        ProfileResources(TotalResourceLimitsProfiler_, GetTotalResourceLimits());
        ProfileResources(MainNodesResourceLimitsProfiler_, GetMainNodesResourceLimits());
        ProfileResources(TotalResourceUsageProfiler_, GetTotalResourceUsage());

        {
            TJobTimeStatisticsDelta jobTimeStatisticsDelta;
            for (auto& nodeShard : NodeShards_) {
                jobTimeStatisticsDelta += nodeShard->GetJobTimeStatisticsDelta();
            }
            Profiler.Increment(TotalCompletedJobTimeCounter_, jobTimeStatisticsDelta.CompletedJobTimeDelta);
            Profiler.Increment(TotalFailedJobTimeCounter_, jobTimeStatisticsDelta.FailedJobTimeDelta);
            Profiler.Increment(TotalAbortedJobTimeCounter_, jobTimeStatisticsDelta.AbortedJobTimeDelta);
        }
    }

    void OnLogging()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsConnected()) {
            LogEventFluently(ELogEventType::ClusterInfo)
                .Item("exec_node_count").Value(GetExecNodeCount())
                .Item("total_node_count").Value(GetTotalNodeCount())
                .Item("resource_limits").Value(GetTotalResourceLimits())
                .Item("main_nodes_resource_limits").Value(GetMainNodesResourceLimits())
                .Item("resource_usage").Value(GetTotalResourceUsage());
        }
    }


    void OnPendingEventLogRowsFlush()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsConnected()) {
            auto owningRows = PendingEventLogRows_.DequeueAll();
            std::vector<TUnversionedRow> rows(owningRows.begin(), owningRows.end());
            EventLogWriter_->Write(rows);
        }
    }

    void OnMasterConnected(const TMasterHandshakeResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto registerFuture = BIND(&TImpl::RegisterRevivingOperations, MakeStrong(this), result.OperationReports)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
        WaitFor(registerFuture)
            .ThrowOnError();

        {
            std::vector<TFuture<void>> nodeShardFutures;
            for (auto& nodeShard : NodeShards_) {
                nodeShardFutures.push_back(BIND(&TNodeShard::OnMasterConnected, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run());
            }
            WaitFor(Combine(nodeShardFutures))
                .ThrowOnError();
        }

        auto responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Start();

        CachedExecNodeDescriptorsByTags_->Start();
        CachedExecNodeMemoryDistributionByTags_->Start();

        IsConnected_.store(true);

        LogEventFluently(ELogEventType::MasterConnected)
            .Item("address").Value(ServiceAddress_);

        ConnectionTime_ = TInstant::Now();

        Strategy_->StartPeriodicActivity();

        auto processFuture = BIND(&TImpl::ProcessOperationReports, MakeStrong(this), result.OperationReports)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
        WaitFor(processFuture)
            .ThrowOnError();
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TForbidContextSwitchGuard contextSwitchGuard;

        LOG_INFO("Starting scheduler state cleanup");

        IsConnected_.store(false);

        auto responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Stop();

        CachedExecNodeDescriptorsByTags_->Stop();
        CachedExecNodeMemoryDistributionByTags_->Stop();

        LogEventFluently(ELogEventType::MasterDisconnected)
            .Item("address").Value(ServiceAddress_);

        auto error = TError("Master disconnected");

        if (Config_->TestingOptions->MasterDisconnectDelay) {
            Sleep(*Config_->TestingOptions->MasterDisconnectDelay);
        }

        {
            std::vector<TFuture<void>> abortFutures;
            for (auto& nodeShard : NodeShards_) {
                abortFutures.push_back(BIND(&TNodeShard::AbortAllJobs, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run(error));
            }
            Combine(abortFutures)
                .Get();
        }

        auto operations = IdToOperation_;
        for (const auto& pair : operations) {
            auto operation = pair.second;
            LOG_INFO("Forgetting operation (OperationId: %v)", operation->GetId());
            if (!operation->IsFinishedState()) {
                operation->Cancel();
                operation->GetController()->Forget();
                SetOperationFinalState(
                    operation,
                    EOperationState::Aborted,
                    error);
            }
            FinishOperation(operation);
        }
        YCHECK(IdToOperation_.empty());

        {
            std::vector<TFuture<void>> nodeShardFutures;
            for (auto& nodeShard : NodeShards_) {
                nodeShardFutures.push_back(BIND(&TNodeShard::OnMasterDisconnected, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run());
            }
            Combine(nodeShardFutures)
                .Get();
        }

        Strategy_->ResetState();

        LOG_INFO("Finished scheduler state cleanup");
    }

    void LogOperationsFairShare() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& pair : IdToOperation_) {
            const auto& operationId = pair.first;
            const auto& operation = pair.second;
            if (operation->GetState() == EOperationState::Running) {
                LOG_DEBUG("%v (OperationId: %v)",
                    Strategy_->GetOperationLoggingProgress(operationId),
                    operationId);
            }
        }
    }

    void LogOperationFinished(TOperationPtr operation, ELogEventType logEventType, TError error)
    {
        LogEventFluently(logEventType)
            .Do(BIND(&TImpl::BuildOperationInfoForEventLog, MakeStrong(this), operation))
            .Item("start_time").Value(operation->GetStartTime())
            .Item("finish_time").Value(operation->GetFinishTime())
            .Item("controller_time_statistics").Value(operation->ControllerTimeStatistics())
            .Item("error").Value(error);
    }

    void OnUserTransactionAborted(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(operation);

        DoAbortOperation(
            operation,
            TError("User transaction %v of operation has expired or was aborted",
                operation->GetUserTransactionId()));
    }

    void OnUserTransactionAborted(const TOperationId& operationId) override
    {
        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND([=, this_ = MakeStrong(this)] {
                auto operation = FindOperation(operationId);
                if (!operation || operation->IsFinishedState()) {
                    return;
                }
                OnUserTransactionAborted(operation);
            }));
    }

    void RequestPools(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        static const auto poolConfigTemplate = New<TPoolConfig>();
        static const auto poolConfigKeys = poolConfigTemplate->GetRegisteredKeys();

        LOG_INFO("Updating pools");
        auto req = TYPathProxy::Get(GetPoolsPath());
        ToProto(req->mutable_attributes()->mutable_keys(), poolConfigKeys);
        batchReq->AddRequest(req, "get_pools");
    }

    void HandlePools(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pools");
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error getting pools configuration");
            return;
        }

        const auto& rsp = rspOrError.Value();
        INodePtr poolsNode;
        try {
            poolsNode = ConvertToNode(TYsonString(rsp->value()));
        } catch (const std::exception& ex) {
            auto error = TError("Error parsing pools configuration")
                << ex;
            SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            return;
        }

        Strategy_->UpdatePools(poolsNode);
    }

    void RequestNodesAttributes(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating nodes information");

        auto req = TYPathProxy::List("//sys/nodes");
        std::vector<TString> attributeKeys{
            "id",
            "tags",
            "state",
            "io_weights"
        };
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, "get_nodes");
    }

    void HandleNodesAttributes(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>("get_nodes");
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error updating nodes information");
            return;
        }

        try {
            const auto& rsp = rspOrError.Value();
            auto nodesList = ConvertToNode(TYsonString(rsp->value()))->AsList();
            std::vector<std::vector<std::pair<TString, INodePtr>>> nodesForShard(NodeShards_.size());
            std::vector<TFuture<void>> shardFutures;
            for (const auto& child : nodesList->GetChildren()) {
                auto address = child->GetValue<TString>();
                auto objectId = child->Attributes().Get<TObjectId>("id");
                auto nodeId = NodeIdFromObjectId(objectId);
                auto nodeShardId = GetNodeShardId(nodeId);
                nodesForShard[nodeShardId].emplace_back(address, child);
            }

            for (int i = 0 ; i < NodeShards_.size(); ++i) {
                auto& nodeShard = NodeShards_[i];
                shardFutures.push_back(
                    BIND(&TNodeShard::HandleNodesAttributes, nodeShard)
                        .AsyncVia(nodeShard->GetInvoker())
                        .Run(std::move(nodesForShard[i])));
            }
            WaitFor(Combine(shardFutures))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error updating nodes information");
        }

        LOG_INFO("Nodes information updated");
    }

    void RequestOperationRuntimeParams(
        TOperationPtr operation,
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        static auto runtimeParamsTemplate = New<TOperationRuntimeParams>();
        auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), runtimeParamsTemplate->GetRegisteredKeys());
        batchReq->AddRequest(req, "get_runtime_params");
    }

    void HandleOperationRuntimeParams(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_runtime_params");
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error updating operation runtime parameters");
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto attributesNode = ConvertToNode(TYsonString(rsp->value()));

        Strategy_->UpdateOperationRuntimeParams(operation, attributesNode);
    }

    void RequestConfig(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating scheduler configuration");

        auto req = TYPathProxy::Get("//sys/scheduler/config");
        batchReq->AddRequest(req, "get_config");
    }

    void HandleConfig(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_config");
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No config in Cypress, just ignore.
            SetSchedulerAlert(ESchedulerAlertType::UpdateConfig, TError());
            return;
        }
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error getting scheduler configuration");
            return;
        }

        auto newConfig = CloneYsonSerializable(InitialConfig_);
        try {
            const auto& rsp = rspOrError.Value();
            auto configFromCypress = ConvertToNode(TYsonString(rsp->value()));
            try {
                newConfig->Load(configFromCypress, /* validate */ true, /* setDefaults */ false);
            } catch (const std::exception& ex) {
                auto error = TError("Error updating cell scheduler configuration")
                    << ex;
                SetSchedulerAlert(ESchedulerAlertType::UpdateConfig, error);
                return;
            }
        } catch (const std::exception& ex) {
            auto error = TError("Error parsing updated scheduler configuration")
                << ex;
            SetSchedulerAlert(ESchedulerAlertType::UpdateConfig, error);
            return;
        }

        SetSchedulerAlert(ESchedulerAlertType::UpdateConfig, TError());

        auto oldConfigNode = ConvertToNode(Config_);
        auto newConfigNode = ConvertToNode(newConfig);

        if (!AreNodesEqual(oldConfigNode, newConfigNode)) {
            LOG_INFO("Scheduler configuration updated");

            Config_ = newConfig;

            for (const auto& operation : GetOperations()) {
                auto controller = operation->GetController();
                BIND(&IOperationController::UpdateConfig, controller, Config_)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
            }

            for (auto& nodeShard : NodeShards_) {
                BIND(&TNodeShard::UpdateConfig, nodeShard, Config_)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run();
            }

            Strategy_->UpdateConfig(Config_);
            MasterConnector_->UpdateConfig(Config_);

            ChunkLocationThrottlerManager_->Reconfigure(Config_->ChunkLocationThrottler);
            ReconfigurableJobSpecSliceThrottler_->Reconfigure(Config_->JobSpecSliceThrottler);

            LoggingExecutor_->SetPeriod(Config_->ClusterInfoLoggingPeriod);
            PendingEventLogRowsFlushExecutor_->SetPeriod(Config_->PendingEventLogRowsFlushPeriod);
            UpdateExecNodeDescriptorsExecutor_->SetPeriod(Config_->UpdateExecNodeDescriptorsPeriod);
            ProfilingExecutor_->SetPeriod(Config_->ProfilingUpdatePeriod);
        }
    }

    void UpdateExecNodeDescriptors()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TFuture<TExecNodeDescriptorListPtr>> shardDescriptorsFutures;
        for (auto& nodeShard : NodeShards_) {
            shardDescriptorsFutures.push_back(BIND(&TNodeShard::GetExecNodeDescriptors, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run());
        }

        auto shardDescriptors = WaitFor(Combine(shardDescriptorsFutures))
            .ValueOrThrow();

        auto result = New<TExecNodeDescriptorList>();
        for (const auto& descriptors : shardDescriptors) {
            result->Descriptors.insert(
                result->Descriptors.end(),
                descriptors->Descriptors.begin(),
                descriptors->Descriptors.end());
        }

        {
            TWriterGuard guard(ExecNodeDescriptorsLock_);

            std::swap(CachedExecNodeDescriptors_, result);
        }

        auto execNodeMemoryDistribution = CalculateMemoryDistribution(EmptySchedulingTagFilter);
        {
            TWriterGuard guard(ExecNodeDescriptorsLock_);

            CachedExecNodeMemoryDistribution_ = execNodeMemoryDistribution;
        }
    }

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

    TMemoryDistribution CalculateMemoryDistribution(const TSchedulingTagFilter& filter) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TMemoryDistribution result;

        {
            TReaderGuard guard(ExecNodeDescriptorsLock_);

            for (const auto& descriptor : CachedExecNodeDescriptors_->Descriptors) {
                if (filter.CanSchedule(descriptor.Tags)) {
                    ++result[RoundUp(descriptor.ResourceLimits.GetMemory(), 1_GB)];
                }
            }
        }

        return FilterLargestValues(result, Config_->MemoryDistributionDifferentNodeTypesThreshold);
    }

    void DoStartOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() != EOperationState::Initializing) {
            throw TFiberCanceledException();
        }

        bool registered = false;
        try {
            auto controller = CreateControllerForOperation(this, operation.Get());
            operation->SetController(controller);

            Strategy_->ValidateOperationCanBeRegistered(operation);

            RegisterOperation(operation);
            registered = true;

            controller->Initialize();
            auto initializeResult = controller->GetInitializeResult();

            WaitFor(MasterConnector_->CreateOperationNode(operation, initializeResult))
                .ThrowOnError();

            if (operation->GetState() != EOperationState::Initializing) {
                throw TFiberCanceledException();
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to initialize")
                << ex;
            if (registered) {
                OnOperationFailed(operation->GetId(), wrappedError);
            } else {
                operation->SetStarted(wrappedError);
            }
            THROW_ERROR(wrappedError);
        }

        LogEventFluently(ELogEventType::OperationStarted)
            .Do(BIND(&TImpl::BuildOperationInfoForEventLog, MakeStrong(this), operation));

        // NB: Once we've registered the operation in Cypress we're free to complete
        // StartOperation request. Preparation will happen in a separate fiber in a non-blocking
        // fashion.
        auto controller = operation->GetController();
        BIND(&TImpl::DoPrepareOperation, MakeStrong(this), operation)
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run();

        operation->SetStarted(TError());
    }

    void DoPrepareOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() != EOperationState::Initializing) {
            throw TFiberCanceledException();
        }

        const auto& operationId = operation->GetId();

        try {
            // Run async preparation.
            LOG_INFO("Preparing operation (OperationId: %v)",
                operationId);

            operation->SetState(EOperationState::Preparing);

            auto controller = operation->GetController();
            auto asyncResult = BIND(&IOperationController::Prepare, controller)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run();

            TWallTimer timer;
            auto result = WaitFor(asyncResult);
            auto prepareDuration = timer.GetElapsedTime();
            operation->UpdateControllerTimeStatistics("/prepare", prepareDuration);

            THROW_ERROR_EXCEPTION_IF_FAILED(result);

            if (operation->GetState() != EOperationState::Preparing) {
                throw TFiberCanceledException();
            }
            operation->SetState(EOperationState::Pending);
            operation->SetPrepared(true);
            if (operation->GetActivated()) {
                MaterializeOperation(operation);
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to prepare")
                << ex;
            OnOperationFailed(operation->GetId(), wrappedError);
            return;
        }

        LOG_INFO("Operation has been prepared (OperationId: %v)",
            operationId);

        LogEventFluently(ELogEventType::OperationPrepared)
            .Item("operation_id").Value(operationId);

        // From this moment on the controller is fully responsible for the
        // operation's fate. It will eventually call #OnOperationCompleted or
        // #OnOperationFailed to inform the scheduler about the outcome.
    }

    void RegisterRevivingOperation(const TOperationPtr& operation)
    {
        auto codicilGuard = operation->MakeCodicilGuard();

        const auto& operationId = operation->GetId();

        LOG_INFO("Reviving operation (OperationId: %v)",
            operationId);

        if (operation->GetMutationId()) {
            TRspStartOperation response;
            ToProto(response.mutable_operation_id(), operationId);
            auto responseMessage = CreateResponseMessage(response);
            auto responseKeeper = Bootstrap_->GetResponseKeeper();
            responseKeeper->EndRequest(operation->GetMutationId(), responseMessage);
        }

        // NB: The operation is being revived, hence it already
        // has a valid node associated with it.
        // If the revival fails, we still need to update the node
        // and unregister the operation from Master Connector.

        try {
            auto controller = CreateControllerForOperation(this, operation.Get());
            operation->SetController(controller);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);
            auto wrappedError = TError("Operation has failed to revive") << ex;
            SetOperationFinalState(operation, EOperationState::Failed, wrappedError);
            MasterConnector_->FlushOperationNode(operation);
            return;
        }

        RegisterOperation(operation);
    }

    TFuture<void> ReviveOperation(const TOperationPtr& operation, const TControllerTransactionsPtr& controllerTransactions)
    {
        auto controller = operation->GetController();
        return BIND(&TImpl::DoReviveOperation, MakeStrong(this), operation, controllerTransactions)
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run();
    }

    void DoReviveOperation(TOperationPtr operation, TControllerTransactionsPtr controllerTransactions)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() != EOperationState::Reviving) {
            throw TFiberCanceledException();
        }

        try {
            auto controller = operation->GetController();

            controller->InitializeReviving(controllerTransactions);

            if (operation->GetState() != EOperationState::Reviving) {
                throw TFiberCanceledException();
            }

            {
                auto error = WaitFor(MasterConnector_->ResetRevivingOperationNode(operation));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            {
                auto asyncResult = BIND(&IOperationController::Revive, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
                auto error = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            if (operation->GetState() != EOperationState::Reviving) {
                throw TFiberCanceledException();
            }

            operation->SetState(EOperationState::Pending);
            operation->SetPrepared(true);
            if (operation->GetActivated()) {
                MaterializeOperation(operation);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Operation has failed to revive (OperationId: %v)",
                operation->GetId());
            auto wrappedError = TError("Operation has failed to revive") << ex;
            OnOperationFailed(operation->GetId(), wrappedError);
            return;
        }

        LOG_INFO("Operation has been revived and is now running (OperationId: %v)",
            operation->GetId());
    }

    TFuture<void> RegisterJobsFromRevivedOperation(const TOperationPtr& operation)
    {
        const auto& controller = operation->GetController();
        auto jobs = controller->BuildJobsFromJoblets();
        LOG_INFO("Registering running jobs from the revived operation (OperationId: %v, JobCount: %v)",
            operation->GetId(),
            jobs.size());

        // First, register jobs in the strategy. Do this syncrhonously as we are in the scheduler control thread.
        GetStrategy()->RegisterJobs(operation->GetId(), jobs);

        // Second, register jobs on the corresponding node shards.
        std::vector<std::vector<TJobPtr>> jobsByShardId(NodeShards_.size());
        for (auto& job : jobs) {
            auto shardId = GetNodeShardId(NodeIdFromJobId(job->GetId()));
            jobsByShardId[shardId].emplace_back(std::move(job));
        }
        std::vector<TFuture<void>> registrationFutures;
        for (int shardId = 0; shardId < NodeShards_.size(); ++shardId) {
            if (jobsByShardId[shardId].empty()) {
                continue;
            }
            auto registerFuture = BIND(&TNodeShard::RegisterRevivedJobs, NodeShards_[shardId])
                .AsyncVia(NodeShards_[shardId]->GetInvoker())
                .Run(std::move(jobsByShardId[shardId]));
            registrationFutures.emplace_back(std::move(registerFuture));
        }
        return Combine(registrationFutures);
    }

    void RegisterOperation(TOperationPtr operation)
    {
        VERIFY_INVOKER_AFFINITY(MasterConnector_->GetCancelableControlInvoker());

        YCHECK(IdToOperation_.insert(std::make_pair(operation->GetId(), operation)).second);
        for (auto& nodeShard : NodeShards_) {
            BIND(&TNodeShard::RegisterOperation, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run(operation->GetId(), operation->GetController());
        }

        Strategy_->RegisterOperation(operation);

        MasterConnector_->AddOperationWatcherRequester(
            operation,
            BIND(&TImpl::RequestOperationRuntimeParams, Unretained(this), operation));
        MasterConnector_->AddOperationWatcherHandler(
            operation,
            BIND(&TImpl::HandleOperationRuntimeParams, Unretained(this), operation));

        Bootstrap_->GetControllerAgent()->RegisterOperation(operation->GetId(), operation->GetController());

        LOG_DEBUG("Operation registered (OperationId: %v)",
            operation->GetId());
    }

    void AbortOperationJobs(TOperationPtr operation, const TError& error, bool terminated)
    {
        std::vector<TFuture<void>> abortFutures;
        for (auto& nodeShard : NodeShards_) {
            abortFutures.push_back(BIND(&TNodeShard::AbortOperationJobs, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run(operation->GetId(), error, terminated));
        }
        WaitFor(Combine(abortFutures))
            .ThrowOnError();
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        YCHECK(IdToOperation_.erase(operation->GetId()) == 1);
        for (auto& nodeShard : NodeShards_) {
            BIND(&TNodeShard::UnregisterOperation, nodeShard, operation->GetId())
                .AsyncVia(nodeShard->GetInvoker())
                .Run();
        }

        Strategy_->UnregisterOperation(operation);

        Bootstrap_->GetControllerAgent()->UnregisterOperation(operation->GetId());

        LOG_DEBUG("Operation unregistered (OperationId: %v)",
            operation->GetId());
    }

    void BuildOperationInfoForEventLog(TOperationPtr operation, IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .Item("spec").Value(operation->GetSpec())
            .Item("authenticated_user").Value(operation->GetAuthenticatedUser());
        Strategy_->BuildOperationInfoForEventLog(operation, consumer);
    }

    void SetOperationFinalState(TOperationPtr operation, EOperationState state, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!operation->GetStarted().IsSet()) {
            operation->SetStarted(error);
        }
        operation->SetState(state);
        operation->SetFinishTime(TInstant::Now());
        ToProto(operation->Result().mutable_error(), error);
    }

    void FinishOperation(TOperationPtr operation)
    {
        if (!operation->GetFinished().IsSet()) {
            operation->SetFinished();
            operation->SetController(nullptr);
            UnregisterOperation(operation);
        }
    }

    void InitStrategy()
    {
        std::vector<IInvokerPtr> feasibleInvokers;
        for (auto controlQueue : TEnumTraits<EControlQueue>::GetDomainValues()) {
            feasibleInvokers.push_back(Bootstrap_->GetControlInvoker(controlQueue));
        }
        Strategy_ = CreateFairShareStrategy(Config_, this, feasibleInvokers);
    }

    INodePtr GetSpecTemplate(EOperationType type, IMapNodePtr spec)
    {
        switch (type) {
            case EOperationType::Map:
                return Config_->MapOperationOptions->SpecTemplate;
            case EOperationType::Merge: {
                auto mergeSpec = NControllerAgent::ParseOperationSpec<TMergeOperationSpec>(spec);
                switch (mergeSpec->Mode) {
                    case EMergeMode::Unordered:
                        return Config_->UnorderedMergeOperationOptions->SpecTemplate;
                    case EMergeMode::Ordered:
                        return Config_->OrderedMergeOperationOptions->SpecTemplate;
                    case EMergeMode::Sorted:
                        return Config_->SortedMergeOperationOptions->SpecTemplate;
                    default:
                        Y_UNREACHABLE();
                }
            }
            case EOperationType::Erase:
                return Config_->EraseOperationOptions->SpecTemplate;
            case EOperationType::Sort:
                return Config_->SortOperationOptions->SpecTemplate;
            case EOperationType::Reduce:
                return Config_->ReduceOperationOptions->SpecTemplate;
            case EOperationType::JoinReduce:
                return Config_->JoinReduceOperationOptions->SpecTemplate;
            case EOperationType::MapReduce:
                return Config_->MapReduceOperationOptions->SpecTemplate;
            case EOperationType::RemoteCopy:
                return Config_->RemoteCopyOperationOptions->SpecTemplate;
            default:
                Y_UNREACHABLE();
        }
    }

    void DoCompleteOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);

        if (!operation || operation->IsFinishedState() || operation->IsFinishingState()) {
            // Operation is probably being aborted.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO("Completing operation (OperationId: %v)",
            operationId);

        operation->SetState(EOperationState::Completing);

        // The operation may still have running jobs (e.g. those started speculatively).
        AbortOperationJobs(operation, TError("Operation completed"), /* terminated */ true);

        try {
            // First flush: ensure that all stderrs are attached and the
            // state is changed to Completing.
            {
                auto asyncResult = MasterConnector_->FlushOperationNode(operation);
                // Result is ignored since failure cause scheduler disconnection.
                WaitFor(asyncResult);
                if (operation->GetState() != EOperationState::Completing) {
                    throw TFiberCanceledException();
                }
            }

            {
                auto controller = operation->GetController();
                auto asyncResult = BIND(&IOperationController::Commit, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
                WaitFor(asyncResult)
                    .ThrowOnError();
                if (controller->IsForgotten()) {
                    // Master disconnected happend while committing controller.
                    return;
                }

                if (operation->GetState() != EOperationState::Completing) {
                    throw TFiberCanceledException();
                }

                if (Config_->TestingOptions->FinishOperationTransitionDelay) {
                    Sleep(*Config_->TestingOptions->FinishOperationTransitionDelay);
                    if (controller->IsForgotten()) {
                        // Master disconnected happend while committing controller.
                        return;
                    }
                }
            }

            YCHECK(operation->GetState() == EOperationState::Completing);
            SetOperationFinalState(operation, EOperationState::Completed, TError());

            // Second flush: ensure that state is changed to Completed.
            {
                auto asyncResult = MasterConnector_->FlushOperationNode(operation);
                WaitFor(asyncResult)
                    .ThrowOnError();
                YCHECK(operation->GetState() == EOperationState::Completed);
            }

            ReleaseCompletedJobs(operation);

            FinishOperation(operation);
        } catch (const std::exception& ex) {
            OnOperationFailed(operation->GetId(), ex);
            return;
        }

        LOG_INFO("Operation completed (OperationId: %v)",
             operationId);

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }

    void DoFailOperation(const TOperationId operationId, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);

        // NB: finishing state is ok, do not skip operation fail in this case.
        if (!operation || operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO(error, "Operation failed (OperationId: %v)",
             operation->GetId());

        TerminateOperation(
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            ELogEventType::OperationFailed,
            error);
    }

    void DoAbortOperation(const TOperationPtr& operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO(error, "Aboring operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        TerminateOperation(
            operation,
            EOperationState::Aborting,
            EOperationState::Aborted,
            ELogEventType::OperationAborted,
            error);
    }

    void DoAbortOperation(const TOperationId operationId, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);

        // NB: finishing state is ok, do not skip operation fail in this case.
        if (!operation || operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        DoAbortOperation(operation, error);
    }

    void DoSuspendOperation(const TOperationId operationId, const TError& error, bool abortRunningJobs, bool setAlert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);

        // NB: finishing state is ok, do not skip operation fail in this case.
        if (!operation || operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        operation->SetSuspended(true);

        if (abortRunningJobs) {
            AbortOperationJobs(operation, error, /* terminated */ false);
        }

        if (setAlert) {
            SetOperationAlert(
                operation->GetId(),
                EOperationAlertType::OperationSuspended,
                error);
        }

        LOG_INFO(error, "Operation suspended (OperationId: %v)",
            operation->GetId());
    }

    void TerminateOperation(
        TOperationPtr operation,
        EOperationState intermediateState,
        EOperationState finalState,
        ELogEventType logEventType,
        const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto state = operation->GetState();
        if (IsOperationFinished(state) ||
            state == EOperationState::Failing ||
            state == EOperationState::Aborting)
        {
            // Safe to call multiple times, just ignore it.
            return;
        }

        operation->SetState(intermediateState);

        AbortOperationJobs(
            operation,
            TError("Operation terminated")
                << TErrorAttribute("state", state)
                << error,
            /* terminated */ true);

        // First flush: ensure that all stderrs are attached and the
        // state is changed to its intermediate value.
        {
            // Result is ignored since failure cause scheduler disconnection.
            WaitFor(MasterConnector_->FlushOperationNode(operation));
            if (operation->GetState() != intermediateState)
                return;
        }


        if (Config_->TestingOptions->FinishOperationTransitionDelay) {
            auto controller = operation->GetController();
            Sleep(*Config_->TestingOptions->FinishOperationTransitionDelay);
            if (controller->IsForgotten()) {
                // Master disconnect happened while committing controller.
                return;
            }
        }

        {
            operation->Cancel();
            auto controller = operation->GetController();
            if (controller) {
                try {
                    controller->Abort();
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Failed to abort controller (OperationId: %v)", operation->GetId());
                    MasterConnector_->Disconnect();
                    return;
                }
            }
        }

        SetOperationFinalState(operation, finalState, error);

        // Second flush: ensure that the state is changed to its final value.
        {
            // Result is ignored since failure cause scheduler disconnection.
            WaitFor(MasterConnector_->FlushOperationNode(operation));
            if (operation->GetState() != finalState)
                return;
        }

        ReleaseCompletedJobs(operation);

        LogOperationFinished(operation, logEventType, error);

        FinishOperation(operation);
    }

    void CompleteCompletingOperation(const TOperationReport& report)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& operation = report.Operation;

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO("Completing operation (OperationId: %v)",
             operation->GetId());

        if (report.ShouldCommitOutputTransaction) {
            WaitFor(report.ControllerTransactions->Output->Commit())
                .ThrowOnError();
        }

        SetOperationFinalState(operation, EOperationState::Completed, TError());

        // Result is ignored since failure cause scheduler disconnection.
        WaitFor(MasterConnector_->FlushOperationNode(operation));

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }

    void AbortAbortingOperation(TOperationPtr operation, TControllerTransactionsPtr controllerTransactions)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO("Aborting operation (OperationId: %v)",
             operation->GetId());

        auto abortTransaction = [&] (ITransactionPtr transaction) {
            if (transaction) {
                // Fire-and-forget.
                transaction->Abort();
            }
        };

        abortTransaction(controllerTransactions->Async);
        abortTransaction(controllerTransactions->Input);
        abortTransaction(controllerTransactions->Output);

        SetOperationFinalState(operation, EOperationState::Aborted, TError());

        // Result is ignored since failure cause scheduler disconnection.
        WaitFor(MasterConnector_->FlushOperationNode(operation));

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }

    void ProcessOperationReports(const std::vector<TOperationReport>& operationReports)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Prepare reviving process on node shards.
        std::vector<TFuture<void>> prepareFutures;
        for (auto& shard : NodeShards_) {
            auto prepareFuture = BIND(&TNodeShard::PrepareReviving, shard)
                .AsyncVia(shard->GetInvoker())
                .Run();
            prepareFutures.emplace_back(std::move(prepareFuture));
        }
        WaitFor(Combine(prepareFutures))
            .ThrowOnError();

        std::vector<TFuture<void>> reviveFutures;
        for (const auto& operationReport : operationReports) {
            const auto& operation = operationReport.Operation;

            if (operationReport.IsCommitted) {
                CompleteCompletingOperation(operationReport);
                continue;
            }

            if (operationReport.IsAborting) {
                AbortAbortingOperation(operation, operationReport.ControllerTransactions);
                continue;
            }

            if (operationReport.UserTransactionAborted) {
                OnUserTransactionAborted(operation);
                continue;
            }

            reviveFutures.emplace_back(ReviveOperation(operation, operationReport.ControllerTransactions));
        }
        WaitFor(Combine(reviveFutures))
            .ThrowOnError();

        // Start reviving process on node shards.
        std::vector<TFuture<void>> startFutures;
        for (auto& shard : NodeShards_) {
            auto startFuture = BIND(&TNodeShard::StartReviving, shard)
                .AsyncVia(shard->GetInvoker())
                .Run();
            startFutures.emplace_back(std::move(startFuture));
        }
        WaitFor(Combine(startFutures))
            .ThrowOnError();
    }

    void RegisterRevivingOperations(const std::vector<TOperationReport>& operationReports)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& operationReport : operationReports) {
            const auto& operation = operationReport.Operation;

            operation->SetState(EOperationState::Reviving);

            if (operationReport.IsCommitted ||
                operationReport.IsAborting ||
                operationReport.UserTransactionAborted)
            {
                continue;
            }

            RegisterRevivingOperation(operation);
        }
    }

    void BuildStaticOrchid(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("connected").Value(MasterConnector_->IsConnected())
                .Item("cell").BeginMap()
                    .Item("resource_limits").Value(GetTotalResourceLimits())
                    .Item("main_nodes_resource_limits").Value(GetMainNodesResourceLimits())
                    .Item("resource_usage").Value(GetTotalResourceUsage())
                    .Item("exec_node_count").Value(GetExecNodeCount())
                    .Item("total_node_count").Value(GetTotalNodeCount())
                    .Item("nodes_memory_distribution").Value(GetExecNodeMemoryDistribution(TSchedulingTagFilter()))
                .EndMap()
                .Item("suspicious_jobs").BeginMap()
                    .Do([=] (IYsonConsumer* consumer) {
                        std::vector<TFuture<TYsonString>> asyncResults;
                        for (const auto& pair : IdToOperation_) {
                            const auto& operation = pair.second;
                            auto controller = operation->GetController();
                            if (controller) {
                                asyncResults.push_back(BIND(&IOperationController::BuildSuspiciousJobsYson, controller)
                                    .AsyncVia(controller->GetInvoker())
                                    .Run());
                            }
                        }
                        auto results = WaitFor(Combine(asyncResults))
                            .ValueOrThrow();

                        for (const auto& ysonString : results) {
                            consumer->OnRaw(ysonString);
                        }
                    })
                .EndMap()
                .Item("nodes").BeginMap()
                    .Do([=] (IYsonConsumer* consumer) {
                        for (auto nodeShard : NodeShards_) {
                            auto asyncResult = WaitFor(
                                BIND(&TNodeShard::BuildNodesYson, nodeShard, consumer)
                                    .AsyncVia(nodeShard->GetInvoker())
                                    .Run());
                            asyncResult.ThrowOnError();
                        }
                    })
                .EndMap()
                .Item("config").Value(Config_)
                .DoIf(Strategy_.operator bool(), BIND(&ISchedulerStrategy::BuildOrchid, Strategy_))
            .EndMap();
    }

    void BuildOperationYson(TOperationPtr operation, IYsonConsumer* consumer) const
    {
        auto codicilGuard = operation->MakeCodicilGuard();

        auto controller = operation->GetController();

        bool hasControllerProgress = operation->HasControllerProgress();
        bool hasControllerJobSplitterInfo = operation->HasControllerJobSplitterInfo();
        BuildYsonFluently(consumer)
            .BeginMap()
                // Include the complete list of attributes.
                .Do(BIND(&NScheduler::BuildInitializingOperationAttributes, operation))
                .Item("progress").BeginMap()
                    .DoIf(hasControllerProgress, BIND([=] (IYsonConsumer* consumer) {
                        auto asyncResult = WaitFor(
                            // TODO(ignat): maybe use cached version here?
                            BIND(&IOperationController::BuildProgress, controller)
                                .AsyncVia(controller->GetInvoker())
                                .Run(consumer));
                        asyncResult.ThrowOnError();
                    }))
                    .Do(BIND(&ISchedulerStrategy::BuildOperationProgress, Strategy_, operation->GetId()))
                .EndMap()
                .Item("brief_progress").BeginMap()
                    .DoIf(hasControllerProgress, BIND([=] (IYsonConsumer* consumer) {
                        auto asyncResult = WaitFor(
                            BIND(&IOperationController::BuildBriefProgress, controller)
                                .AsyncVia(controller->GetInvoker())
                                .Run(consumer));
                        asyncResult.ThrowOnError();
                    }))
                    .Do(BIND(&ISchedulerStrategy::BuildBriefOperationProgress, Strategy_, operation->GetId()))
                .EndMap()
                .Item("running_jobs").BeginAttributes()
                    .Item("opaque").Value("true")
                .EndAttributes()
                .BeginMap()
                    .Do([=] (IYsonConsumer* consumer) {
                        auto future = BIND(&IOperationController::BuildJobsYson, controller)
                            .AsyncVia(controller->GetCancelableInvoker())
                            .Run();
                        auto jobsYson = WaitFor(future)
                            .ValueOrThrow();
                        consumer->OnRaw(jobsYson);
                    })
                .EndMap()
                .Item("job_splitter").BeginAttributes()
                    .Item("opaque").Value("true")
                .EndAttributes()
                .BeginMap()
                    .DoIf(hasControllerJobSplitterInfo, BIND([=] (IYsonConsumer* consumer) {
                        auto asyncResult = WaitFor(
                            BIND(&IOperationController::BuildJobSplitterInfo, controller)
                                .AsyncVia(controller->GetInvoker())
                                .Run(consumer));
                        asyncResult.ThrowOnError();
                    }))
                .EndMap()
                .Do([=] (IYsonConsumer* consumer) {
                    auto asyncResult = WaitFor(
                        BIND(&IOperationController::BuildMemoryDigestStatistics, controller)
                            .AsyncVia(controller->GetInvoker())
                            .Run(consumer));
                    asyncResult.ThrowOnError();
                })
            .EndMap();
    }

    void ReleaseCompletedJobs(const TOperationPtr& operation)
    {
        if (const auto& controller = operation->GetController()) {
            int numberOfJobsToRelease = controller->GetRecentlyCompletedJobCount();
            if (numberOfJobsToRelease > 0) {
                auto error = WaitFor(controller->ReleaseJobs(numberOfJobsToRelease));
                YCHECK(error.IsOK() && "ReleaseJobs failed");
            }
        }
    }

    IYPathServicePtr GetDynamicOrchidService()
    {
        auto dynamicOrchidService = New<TCompositeMapService>();
        dynamicOrchidService->AddChild("operations", New<TOperationsService>(this));
        dynamicOrchidService->AddChild("jobs", New<TJobsService>(this));
        return dynamicOrchidService;
    }

    class TOperationsService
        : public TVirtualMapBase
    {
    public:
        explicit TOperationsService(const TScheduler::TImpl* scheduler)
            : TVirtualMapBase(nullptr /* owningNode */)
            , Scheduler_(scheduler)
        { }

        virtual i64 GetSize() const override
        {
            return Scheduler_->IdToOperation_.size();
        }

        virtual std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            keys.reserve(limit);
            for (const auto& pair : Scheduler_->IdToOperation_) {
                if (static_cast<i64>(keys.size()) >= limit) {
                    break;
                }
                keys.emplace_back(ToString(pair.first));
            }
            return keys;
        }

        virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
        {
            TOperationId operationId = TOperationId::FromString(key);
            auto operation = Scheduler_->FindOperation(operationId);
            if (!operation) {
                return nullptr;
            }

            return IYPathService::FromProducer(
                BIND(&TScheduler::TImpl::BuildOperationYson, MakeStrong(Scheduler_), operation));
        }

    private:
        const TScheduler::TImpl* const Scheduler_;
    };

    class TJobsService
        : public TVirtualMapBase
    {
    public:
        explicit TJobsService(const TScheduler::TImpl* scheduler)
            : TVirtualMapBase(nullptr /* owningNode */)
            , Scheduler_(scheduler)
        { }

        virtual void GetSelf(
            TReqGet* request,
            TRspGet* response,
            const TCtxGetPtr& context) override
        {
            ThrowMethodNotSupported(context->GetMethod());
        }

        virtual void ListSelf(
            TReqList* request,
            TRspList* response,
            const TCtxListPtr& context) override
        {
            ThrowMethodNotSupported(context->GetMethod());
        }

        virtual i64 GetSize() const override
        {
            Y_UNREACHABLE();
        }

        virtual std::vector<TString> GetKeys(i64 limit) const override
        {
            Y_UNREACHABLE();
        }

        virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
        {
            TJobId jobId = TJobId::FromString(key);
            auto buildJobYsonCallback = BIND(&TJobsService::BuildControllerJobYson, MakeStrong(this), jobId);
            auto jobYPathService = IYPathService::FromProducer(buildJobYsonCallback)
                ->Via(Scheduler_->GetControlInvoker(EControlQueue::Orchid));
            return jobYPathService;
        }

    private:
        void BuildControllerJobYson(const TJobId& jobId, IYsonConsumer* consumer) const
        {
            const auto& nodeShard = Scheduler_->GetNodeShardByJobId(jobId);

            auto getOperationIdCallback = BIND(&TNodeShard::GetOperationIdByJobId, nodeShard, jobId)
                .AsyncVia(nodeShard->GetInvoker())
                .Run();
            auto operationId = WaitFor(getOperationIdCallback)
                .ValueOrThrow();

            if (!operationId) {
                return;
            }

            auto operation = Scheduler_->GetOperation(operationId);
            auto controller = operation->GetController();
            if (!controller) {
                return;
            }

            auto jobYsonCallback = BIND(&IOperationController::BuildJobYson, controller, jobId, /* outputStatistics */ true)
                .AsyncVia(controller->GetInvoker())
                .Run();

            auto jobYsonString = WaitFor(jobYsonCallback)
                .ValueOrThrow();

            consumer->OnRaw(jobYsonString);
        }

        const TScheduler::TImpl* Scheduler_;
    };
};

////////////////////////////////////////////////////////////////////////////////

TScheduler::TScheduler(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TScheduler::~TScheduler() = default;

void TScheduler::Initialize()
{
    Impl_->Initialize();
}

ISchedulerStrategyPtr TScheduler::GetStrategy()
{
    return Impl_->GetStrategy();
}

IYPathServicePtr TScheduler::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

std::vector<TOperationPtr> TScheduler::GetOperations()
{
    return Impl_->GetOperations();
}

IInvokerPtr TScheduler::GetSnapshotIOInvoker()
{
    return Impl_->GetSnapshotIOInvoker();
}

bool TScheduler::IsConnected()
{
    return Impl_->IsConnected();
}

void TScheduler::ValidateConnected()
{
    Impl_->ValidateConnected();
}

void TScheduler::ValidateAcceptsHeartbeats()
{
    Impl_->ValidateAcceptsHeartbeats();
}

TOperationPtr TScheduler::FindOperation(const TOperationId& id) const
{
    return Impl_->FindOperation(id);
}

TOperationPtr TScheduler::GetOperationOrThrow(const TOperationId& id) const
{
    return Impl_->GetOperationOrThrow(id);
}

TFuture<TOperationPtr> TScheduler::StartOperation(
    EOperationType type,
    const TTransactionId& transactionId,
    const TMutationId& mutationId,
    IMapNodePtr spec,
    const TString& user)
{
    return Impl_->StartOperation(
        type,
        transactionId,
        mutationId,
        spec,
        user);
}

TFuture<void> TScheduler::AbortOperation(
    TOperationPtr operation,
    const TError& error,
    const TString& user)
{
    return Impl_->AbortOperation(operation, error, user);
}

TFuture<void> TScheduler::SuspendOperation(
    TOperationPtr operation,
    const TString& user,
    bool abortRunningJobs)
{
    return Impl_->SuspendOperation(operation, user, abortRunningJobs);
}

TFuture<void> TScheduler::ResumeOperation(
    TOperationPtr operation,
    const TString& user)
{
    return Impl_->ResumeOperation(operation, user);
}

TFuture<void> TScheduler::CompleteOperation(
    TOperationPtr operation,
    const TError& error,
    const TString& user)
{
    return Impl_->CompleteOperation(operation, error, user);
}

TFuture<void> TScheduler::DumpInputContext(const TJobId& jobId, const NYPath::TYPath& path, const TString& user)
{
    return Impl_->DumpInputContext(jobId, path, user);
}

TFuture<TNodeDescriptor> TScheduler::GetJobNode(const TJobId& jobId, const TString& user)
{
    return Impl_->GetJobNode(jobId, user);
}


TFuture<TYsonString> TScheduler::Strace(const TJobId& jobId, const TString& user)
{
    return Impl_->Strace(jobId, user);
}

TFuture<void> TScheduler::SignalJob(const TJobId& jobId, const TString& signalName, const TString& user)
{
    return Impl_->SignalJob(jobId, signalName, user);
}

TFuture<void> TScheduler::AbandonJob(const TJobId& jobId, const TString& user)
{
    return Impl_->AbandonJob(jobId, user);
}

TFuture<TYsonString> TScheduler::PollJobShell(const TJobId& jobId, const TYsonString& parameters, const TString& user)
{
    return Impl_->PollJobShell(jobId, parameters, user);
}

TFuture<void> TScheduler::AbortJob(const TJobId& jobId, const TNullable<TDuration>& interruptTimeout, const TString& user)
{
    return Impl_->AbortJob(jobId, interruptTimeout, user);
}

void TScheduler::ProcessHeartbeat(TCtxHeartbeatPtr context)
{
    Impl_->ProcessHeartbeat(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
