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
#include <yt/ytlib/scheduler/controller_agent_service_proxy.h>

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

#include <yt/ytlib/job_tracker_client/job_tracker_service.pb.h>

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

#include <yt/core/net/local_address.h>

#include <yt/core/profiling/timing.h>
#include <yt/core/profiling/profile_manager.h>

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
using namespace NNet;
using namespace NApi;
using namespace NCellScheduler;
using namespace NObjectClient;
using namespace NHydra;
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
using namespace NEventLog;

using NControllerAgent::TControllerTransactionsPtr;
using NControllerAgent::IOperationControllerSchedulerHost;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;
using NNodeTrackerClient::TNodeDirectory;

using NScheduler::NProto::TRspStartOperation;

using std::placeholders::_1;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

template <class K, class V>
yhash<K, V> FilterLargestValues(const yhash<K, V>& input, size_t threshold)
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
    return yhash<K, V>(items.begin(), items.begin() + threshold);
}

////////////////////////////////////////////////////////////////////////////////

struct TPoolTreeKeysHolder
{
    TPoolTreeKeysHolder()
    {
        auto treeConfigTemplate = New<TFairShareStrategyTreeConfig>();
        auto treeConfigKeys = treeConfigTemplate->GetRegisteredKeys();

        auto poolConfigTemplate = New<TPoolConfig>();
        auto poolConfigKeys = poolConfigTemplate->GetRegisteredKeys();

        Keys.reserve(treeConfigKeys.size() + poolConfigKeys.size() + 1);
        Keys.insert(Keys.end(), treeConfigKeys.begin(), treeConfigKeys.end());
        Keys.insert(Keys.end(), poolConfigKeys.begin(), poolConfigKeys.end());
        Keys.insert(Keys.end(), DefaultTreeAttributeName);
    }

    std::vector<TString> Keys;
};

////////////////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public TRefCounted
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
        , ReconfigurableJobSpecSliceThrottler_(CreateReconfigurableThroughputThrottler(
            Config_->JobSpecSliceThrottler,
            NLogging::TLogger(),
            NProfiling::TProfiler(SchedulerProfiler.GetPathPrefix() + "/job_spec_slice_throttler")))
        , JobSpecSliceThrottler_(ReconfigurableJobSpecSliceThrottler_)
        , MasterConnector_(std::make_unique<TMasterConnector>(Config_, Bootstrap_))
        , CachedExecNodeMemoryDistributionByTags_(New<TExpiringCache<TSchedulingTagFilter, TMemoryDistribution>>(
            BIND(&TImpl::CalculateMemoryDistribution, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout,
            GetControlInvoker()))
        , TotalResourceLimitsProfiler_(Profiler.GetPathPrefix() + "/total_resource_limits")
        , TotalResourceUsageProfiler_(Profiler.GetPathPrefix() + "/total_resource_usage")
        , TotalCompletedJobTimeCounter_("/total_completed_job_time")
        , TotalFailedJobTimeCounter_("/total_failed_job_time")
        , TotalAbortedJobTimeCounter_("/total_aborted_job_time")
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_THREAD_AFFINITY(GetControlInvoker(), ControlThread);

        auto primaryMasterCellTag = GetMasterClient()
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

        {
            std::vector<IInvokerPtr> feasibleInvokers;
            for (auto controlQueue : TEnumTraits<EControlQueue>::GetDomainValues()) {
                feasibleInvokers.push_back(Bootstrap_->GetControlInvoker(controlQueue));
            }
            Strategy_ = CreateFairShareStrategy(Config_, this, feasibleInvokers);
        }
    }

    void Initialize()
    {
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

        MasterConnector_->AddGlobalWatcherRequester(BIND(
            &TImpl::RequestOperationArchiveVersion,
            Unretained(this)));
        MasterConnector_->AddGlobalWatcherHandler(BIND(
            &TImpl::HandleOperationArchiveVersion,
            Unretained(this)));

        MasterConnector_->SubscribeMasterConnecting(BIND(
            &TImpl::OnMasterConnecting,
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

        EventLogWriter_ = New<TEventLogWriter>(
            Config_->EventLog,
            GetMasterClient(),
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity));
        EventLogWriterConsumer_ = EventLogWriter_->CreateConsumer();

        LogEventFluently(ELogEventType::SchedulerStarted)
            .Item("address").Value(ServiceAddress_);

        LoggingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity),
            BIND(&TImpl::OnLogging, MakeWeak(this)),
            Config_->ClusterInfoLoggingPeriod);
        LoggingExecutor_->Start();

        UpdateExecNodeDescriptorsExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(EControlQueue::PeriodicActivity),
            BIND(&TImpl::UpdateExecNodeDescriptors, MakeWeak(this)),
            Config_->UpdateExecNodeDescriptorsPeriod);
        UpdateExecNodeDescriptorsExecutor_->Start();
    }

    const NApi::INativeClientPtr& GetMasterClient() const
    {
        return Bootstrap_->GetMasterClient();
    }

    IYPathServicePtr GetOrchidService()
    {
        auto staticOrchidProducer = BIND(&TImpl::BuildStaticOrchid, MakeStrong(this));
        auto staticOrchidService = IYPathService::FromProducer(staticOrchidProducer)
            ->Via(GetControlInvoker(EControlQueue::Orchid))
            ->Cached(Config_->StaticOrchidCacheUpdatePeriod);

        auto dynamicOrchidService = GetDynamicOrchidService()
            ->Via(GetControlInvoker(EControlQueue::Orchid));

        return New<TServiceCombiner>(
            std::vector<IYPathServicePtr>{staticOrchidService, dynamicOrchidService},
            Config_->OrchidKeysUpdatePeriod);
    }

    bool IsConnected()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return MasterConnector_->GetState() == EMasterConnectorState::Connected;
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

    void Disconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        MasterConnector_->Disconnect();
    }

    virtual TInstant GetConnectionTime() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return MasterConnector_->GetConnectionTime();
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

    virtual TFuture<void> SetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoSetOperationAlert, MakeStrong(this), operationId, alertType, alert.Sanitize())
            .AsyncVia(GetControlInvoker())
            .Run();
    }

    virtual void ValidatePoolPermission(
        const TYPath& path,
        const TString& user,
        EPermission permission) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Validating pool permission (Permission: %v, User: %v, Pool: %v)",
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

        LOG_DEBUG("Validating operation permission (Permission: %v, User: %v, OperationId: %v)",
            permission,
            user,
            operationId);

        auto path = GetNewOperationPath(operationId);

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
        IMapNodePtr specNode,
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
        auto specTemplate = GetSpecTemplate(type, specNode);
        if (specTemplate) {
            specNode = PatchNode(specTemplate, specNode)->AsMap();
        }

        TOperationSpecBasePtr spec;
        try {
            spec = ConvertTo<TOperationSpecBasePtr>(specNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operation spec")
                << ex;
        }

        auto secureVault = std::move(spec->SecureVault);
        specNode->RemoveChild("secure_vault");

        auto operationId = MakeRandomId(
            EObjectType::Operation,
            GetMasterClient()->GetNativeConnection()->GetPrimaryMasterCellTag());

        auto operation = New<TOperation>(
            operationId,
            type,
            mutationId,
            transactionId,
            specNode,
            secureVault,
            BuildOperationRuntimeParams(spec),
            user,
            spec->Owners,
            TInstant::Now(),
            GetControlInvoker(EControlQueue::Operation),
            spec->TestingOperationOptions->CypressStorageMode);
        operation->SetState(EOperationState::Initializing);
        operation->SetSchedulerIncarnation(SchedulerIncarnation_);

        WaitFor(Strategy_->ValidateOperationStart(operation.Get()))
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

    TFuture<void> AbortOperation(
        const TOperationPtr& operation,
        const TError& error,
        const TString& user)
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

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND([=, this_ = MakeStrong(this)] {
                DoAbortOperation(operation->GetId(), error);
            }));

        return operation->GetFinished();
    }

    TFuture<void> SuspendOperation(
        const TOperationPtr& operation,
        const TString& user,
        bool abortRunningJobs)
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

        DoSuspendOperation(
            operation->GetId(),
            TError("Suspend operation by user request"),
            abortRunningJobs,
            /* setAlert */ false);

        return MasterConnector_->FlushOperationNode(operation);
    }

    TFuture<void> ResumeOperation(
        const TOperationPtr& operation,
        const TString& user)
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
        for (const auto& nodeShard : NodeShards_) {
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

    TFuture<void> CompleteOperation(
        const TOperationPtr& operation,
        const TError& error,
        const TString& user)
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

    TFuture<void> AbortJob(const TJobId& jobId, TNullable<TDuration> interruptTimeout, const TString& user)
    {
        const auto& nodeShard = GetNodeShardByJobId(jobId);
        return
            BIND(
                static_cast<void (TNodeShard::*)(const TJobId&, TNullable<TDuration>, const TString&)>(&TNodeShard::AbortJob),
                nodeShard,
                jobId,
                interruptTimeout,
                user)
            .AsyncVia(nodeShard->GetInvoker())
            .Run();
    }

    
    void ProcessNodeHeartbeat(const TCtxNodeHeartbeatPtr& context)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto* request = &context->Request();
        auto nodeId = request->node_id();

        const auto& nodeShard = GetNodeShard(nodeId);
        nodeShard->GetInvoker()->Invoke(BIND(&TNodeShard::ProcessHeartbeat, nodeShard, context));
    }

    void ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto* request = &context->Request();
        auto* response = &context->Response();

        auto agentIncarnationId = FromProto<NControllerAgent::TIncarnationId>(request->agent_incarnation_id());

        context->SetRequestInfo("AgentIncarnationId: %v", agentIncarnationId);

        if (agentIncarnationId != AgentIncarnationId_) {
            THROW_ERROR_EXCEPTION("Wrong agent incarnation id: expected %v, got %v",
                AgentIncarnationId_,
                agentIncarnationId);
        }

        for (const auto& jobMetricsProto : request->job_metrics()) {
            auto jobMetrics = FromProto<TOperationJobMetrics>(jobMetricsProto);
            GetStrategy()->ApplyJobMetricsDelta(jobMetrics);
        }

        ProcessNodeShardRequests(
            request->jobs_to_interrupt(),
            [] (const auto& nodeShard, const auto& subrequest) {
                auto jobId = FromProto<TJobId>(subrequest.job_id());
                auto reason = EInterruptReason(subrequest.reason());
                nodeShard->InterruptJob(jobId, reason);
            });

        ProcessNodeShardRequests(
            request->jobs_to_abort(),
            [] (const auto& nodeShard, const auto& subrequest) {
                auto jobId = FromProto<TJobId>(subrequest.job_id());
                auto error = FromProto<TError>(subrequest.error());
                nodeShard->AbortJob(jobId, error);
            });

        ProcessNodeShardRequests(
            request->jobs_to_fail(),
            [] (const auto& nodeShard, const auto& subrequest) {
                auto jobId = FromProto<TJobId>(subrequest.job_id());
                nodeShard->FailJob(jobId);
            });

        for (const auto& jobsToRelease : request->jobs_to_release()) {
            auto operationId = FromProto<TOperationId>(jobsToRelease.operation_id());
            auto jobIds = FromProto<std::vector<TJobId>>(jobsToRelease.job_ids());
            OnJobsRelease(
                operationId,
                jobIds,
                jobsToRelease.controller_scheduler_incarnation());
        }

        for (const auto& protoOperationId : request->completed_operation_ids()) {
            auto operationId = FromProto<TOperationId>(protoOperationId);
            OnOperationCompleted(operationId);
        }

        for (const auto& protoOperationSuspension : request->suspended_operations()) {
            auto operationId = FromProto<TOperationId>(protoOperationSuspension.operation_id());
            auto error = FromProto<TError>(protoOperationSuspension.error());
            OnOperationSuspended(operationId, error);
        }

        for (const auto& protoOperationAbort : request->aborted_operations()) {
            auto operationId = FromProto<TOperationId>(protoOperationAbort.operation_id());
            auto error = FromProto<TError>(protoOperationAbort.error());
            OnOperationAborted(operationId, error);
        }

        for (const auto& protoOperationFailure : request->failed_operations()) {
            auto operationId = FromProto<TOperationId>(protoOperationFailure.operation_id());
            auto error = FromProto<TError>(protoOperationFailure.error());
            OnOperationFailed(operationId, error);
        }

        for (const auto& protoOperationAlerts : request->operation_alerts()) {
            auto operationId = FromProto<TOperationId>(protoOperationAlerts.operation_id());
            TOperationAlertMap alerts;
            for (const auto& protoAlert : protoOperationAlerts.alerts()) {
                auto alertType = EOperationAlertType(protoAlert.type());
                auto alert = FromProto<TError>(protoAlert.error());
                YCHECK(alerts.emplace(alertType, std::move(alert)).second);
            }
            DoSetOperationAlerts(operationId, alerts);
        }

        if (request->exec_nodes_requested()) {
            TExecNodeDescriptorListPtr execNodes;
            {
                TReaderGuard guard(ExecNodeDescriptorsLock_);
                execNodes = CachedExecNodeDescriptors_;
            }

            for (const auto& execNode : execNodes->Descriptors) {
                ToProto(response->mutable_exec_nodes()->add_exec_nodes(), execNode);
            }
        }

        SuspiciousJobsYson_ = TYsonString(request->suspicious_jobs(), EYsonType::MapFragment);
    }

    // ISchedulerStrategyHost implementation
    virtual TJobResources GetTotalResourceLimits() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto totalResourceLimits = ZeroJobResources();
        for (const auto& nodeShard : NodeShards_) {
            totalResourceLimits += nodeShard->GetTotalResourceLimits();
        }
        return totalResourceLimits;
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
        for (const auto& nodeShard : NodeShards_) {
            resourceLimits += nodeShard->GetResourceLimits(filter);
        }

        {
            auto value = std::make_pair(GetCpuInstant(), resourceLimits);
            auto it = CachedResourceLimitsByTags_.find(filter);
            if (it == CachedResourceLimitsByTags_.end()) {
                CachedResourceLimitsByTags_.emplace(filter, std::move(value));
            } else {
                it->second = std::move(value);
            }
        }

        return resourceLimits;
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

    virtual void AbortOperation(const TOperationId& operationId, const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoAbortOperation(operationId, error);
    }

    void MaterializeOperation(const TOperationPtr& operation)
    {
        if (operation->GetState() != EOperationState::Pending) {
            // Operation can be in finishing state already.
            return;
        }

        auto controller = operation->GetController();
        if (controller->IsRevivedFromSnapshot()) {
            operation->SetState(EOperationState::RevivingJobs);
            RegisterJobsFromRevivedOperation(operation)
                .Subscribe(
                    BIND([operation] (const TError& error) {
                        if (!error.IsOK()) {
                            return;
                        }
                        if (operation->GetState() == EOperationState::RevivingJobs) {
                            operation->SetState(EOperationState::Running);
                        }
                    })
                    .Via(operation->GetCancelableControlInvoker()));
        } else {
            operation->SetState(EOperationState::Materializing);
            BIND(&IOperationControllerSchedulerHost::Materialize, controller)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run()
                .Subscribe(
                    BIND([operation] (const TError& error) {
                        if (!error.IsOK()) {
                            return;
                        }
                        if (operation->GetState() == EOperationState::Materializing) {
                            operation->SetState(EOperationState::Running);
                        }
                    })
                    .Via(operation->GetCancelableControlInvoker()));
        }
    }

    virtual std::vector<TNodeId> GetExecNodeIds(const TSchedulingTagFilter& filter) const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TNodeId> result;
        for (const auto& pair : NodeIdToTags_) {
            if (filter.CanSchedule(pair.second)) {
                result.push_back(pair.first);
            }
        }

        return result;
    }

    virtual IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const
    {
        return Bootstrap_->GetControlInvoker(queue);
    }

    virtual IYsonConsumer* GetEventLogConsumer() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return EventLogWriterConsumer_.get();
    }

    // INodeShardHost implementation
    virtual int GetNodeShardId(TNodeId nodeId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return nodeId % static_cast<int>(NodeShards_.size());
    }

    virtual TFuture<void> RegisterOrUpdateNode(TNodeId nodeId, const yhash_set<TString>& tags) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoRegisterOrUpdateNodeTags, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run(nodeId, tags);
    }

    virtual void UnregisterNode(TNodeId nodeId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        BIND([this, this_ = MakeStrong(this), nodeId] { YCHECK(NodeIdToTags_.erase(nodeId) == 1); })
            .AsyncVia(GetControlInvoker())
            .Run();
    }

    virtual const ISchedulerStrategyPtr& GetStrategy() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Strategy_;
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
            // XXX(babenko): wrong affinity
            .AsyncVia(Bootstrap_->GetControllerAgent()->GetCancelableInvoker())
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

    virtual int GetOperationArchiveVersion() const override
    {
        return OperationArchiveVersion_.load(std::memory_order_relaxed);
    }

private:
    TSchedulerConfigPtr Config_;
    const TSchedulerConfigPtr InitialConfig_;
    TBootstrap* const Bootstrap_;

    const IReconfigurableThroughputThrottlerPtr ReconfigurableJobSpecSliceThrottler_;
    const IThroughputThrottlerPtr JobSpecSliceThrottler_;

    const std::unique_ptr<TMasterConnector> MasterConnector_;

    //! Ordinal number of this scheduler incarnation. It is used
    //! to discard late callbacks that are submitted by still
    //! running controllers.
    //! This field is incremented on each OnMasterConnected and
    //! should be accessed only from scheduler control thread.
    int SchedulerIncarnation_ = -1;

    ISchedulerStrategyPtr Strategy_;

    yhash<TOperationId, TOperationPtr> IdToOperation_;

    TReaderWriterSpinLock ExecNodeDescriptorsLock_;
    TExecNodeDescriptorListPtr CachedExecNodeDescriptors_ = New<TExecNodeDescriptorList>();
    bool ShouldSendExecNodeDescriptorToControllerAgent_ = false;

    TMemoryDistribution CachedExecNodeMemoryDistribution_;
    TIntrusivePtr<TExpiringCache<TSchedulingTagFilter, TMemoryDistribution>> CachedExecNodeMemoryDistributionByTags_;


    TProfiler TotalResourceLimitsProfiler_;
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
    TPeriodicExecutorPtr UpdateExecNodeDescriptorsExecutor_;

    TString ServiceAddress_;

    std::vector<TNodeShardPtr> NodeShards_;

    yhash<TNodeId, yhash_set<TString>> NodeIdToTags_;

    yhash<TSchedulingTagFilter, std::pair<TCpuInstant, TJobResources>> CachedResourceLimitsByTags_;

    TEventLogWriterPtr EventLogWriter_;
    std::unique_ptr<IYsonConsumer> EventLogWriterConsumer_;

    std::atomic<int> OperationArchiveVersion_ = {-1};

    TYsonString SuspiciousJobsYson_ = TYsonString("", EYsonType::MapFragment);
    
    // TODO(babenko): multiple incarnations
    NControllerAgent::TIncarnationId AgentIncarnationId_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    template <class T, class F>
    void ProcessNodeShardRequests(const google::protobuf::RepeatedPtrField<T>& subrequests, F handler)
    {
        std::vector<std::vector<const T*>> groupedSubrequests(NodeShards_.size());
        for (const auto& subrequest : subrequests) {
            auto jobId = FromProto<TJobId>(subrequest.job_id());
            auto shardId = GetNodeShardId(NodeIdFromJobId(jobId));
            groupedSubrequests[shardId].push_back(&subrequest);
        }

        for (size_t shardId = 0; shardId < NodeShards_.size(); ++shardId) {
            const auto& nodeShard = NodeShards_[shardId];
            nodeShard->GetInvoker()->Invoke(BIND(
                [=, subrequests = std::move(groupedSubrequests[shardId])] {
                    for (const auto* subrequest : subrequests) {
                        handler(nodeShard, *subrequest);
                    }
                }));
        }
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

        if (operation->Alerts()[alertType] == alert) {
            return;
        }

        operation->MutableAlerts()[alertType] = alert;
    }

    void DoSetOperationAlerts(
        const TOperationId& operationId,
        const TOperationAlertMap& operationAlerts)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);
        if (!operation) {
            return;
        }

        for (const auto& pair : operationAlerts) {
            const auto& alertType = pair.first;
            const auto& alert = pair.second;
            if (operation->Alerts()[alertType] != alert) {
                operation->MutableAlerts()[alertType] = alert;
            }
        }
    }


    void DoRegisterOrUpdateNodeTags(TNodeId nodeId, const yhash_set<TString>& tags)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Strategy_->ValidateNodeTags(tags);
        NodeIdToTags_[nodeId] = tags;
    }


    void OnJobsRelease(
        const TOperationId& operationId,
        std::vector<TJobId> jobIds,
        int controllerSchedulerIncarnation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        MasterConnector_->GetCancelableControlInvoker()->Invoke(BIND(
            &TImpl::DoReleaseJobs,
            MakeStrong(this),
            operationId,
            Passed(std::move(jobIds)),
            controllerSchedulerIncarnation));
    }

    void DoReleaseJobs(
        const TOperationId& operationId,
        const std::vector<TJobId>& jobIds,
        int controllerSchedulerIncarnation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (SchedulerIncarnation_ != controllerSchedulerIncarnation) {
            LOG_WARNING("Not releasing jobs because of the wrong scheduler incarnation "
                "(SchedulerIncarnation: %v, ControllerSchedulerIncarnation: %v)",
                SchedulerIncarnation_,
                controllerSchedulerIncarnation);
            return;
        }

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

        auto error = WaitFor(Combine(submitFutures));
        if (!error.IsOK()) {
            DoFailOperation(operationId, error);
        }
    }


    const TNodeShardPtr& GetNodeShard(TNodeId nodeId) const
    {
        return NodeShards_[GetNodeShardId(nodeId)];
    }

    const TNodeShardPtr& GetNodeShardByJobId(const TJobId& jobId) const
    {
        auto nodeId = NodeIdFromJobId(jobId);
        return GetNodeShard(nodeId);
    }


    int GetExecNodeCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int execNodeCount = 0;
        for (const auto& nodeShard : NodeShards_) {
            execNodeCount += nodeShard->GetExecNodeCount();
        }
        return execNodeCount;
    }

    int GetTotalNodeCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int totalNodeCount = 0;
        for (const auto& nodeShard : NodeShards_) {
            totalNodeCount += nodeShard->GetTotalNodeCount();
        }
        return totalNodeCount;
    }

    int GetActiveJobCount()
    {
        int activeJobCount = 0;
        for (const auto& nodeShard : NodeShards_) {
            activeJobCount += nodeShard->GetActiveJobCount();
        }
        return activeJobCount;
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
        ProfileResources(TotalResourceUsageProfiler_, GetTotalResourceUsage());

        {
            TJobTimeStatisticsDelta jobTimeStatisticsDelta;
            for (const auto& nodeShard : NodeShards_) {
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
                .Item("resource_usage").Value(GetTotalResourceUsage());
        }
    }


    void OnMasterConnecting(const TMasterHandshakeResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ++SchedulerIncarnation_;
        LOG_INFO("Preparing new incarnation of scheduler (SchedulerIncarnation: %v)",
            SchedulerIncarnation_);

        ValidateConfig();

        // NB: Must start the keeper before registering operations.
        const auto& responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Start();

        ProcessHandshakeOperations(result.Operations);

        {
            LOG_INFO("Connecting node shards");

            std::vector<TFuture<void>> asyncResults;
            for (const auto& nodeShard : NodeShards_) {
                asyncResults.push_back(BIND(&TNodeShard::OnMasterConnected, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run());
            }

            auto error = WaitFor(Combine(asyncResults));
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Error connecting node shards")
                    << error;
            }
        }
    }

    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // XXX(babenko)
        AgentIncarnationId_ = NControllerAgent::TIncarnationId::Create();
        Bootstrap_->GetControllerAgent()->GetMasterConnector()->OnMasterConnected(AgentIncarnationId_);
        for (const auto& pair : IdToOperation_) {
            const auto& operation = pair.second;
            Bootstrap_->GetControllerAgent()->GetMasterConnector()->StartOperationNodeUpdates(operation->GetId(), operation->GetStorageMode());
        }

        CachedExecNodeMemoryDistributionByTags_->Start();

        Strategy_->StartPeriodicActivity();

        LogEventFluently(ELogEventType::MasterConnected)
            .Item("address").Value(ServiceAddress_);

        // Initiate background revival.
        MasterConnector_
            ->GetCancelableControlInvoker()
            ->Invoke(BIND([this, this_ = MakeStrong(this)] {
                try {
                    ReviveOperations();
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Error reviving operations");
                    Disconnect();
                }
            }));
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Cleaning up scheduler state (SchedulerIncarnation: %v)",
            SchedulerIncarnation_);

        // XXX(babenko)
        TForbidContextSwitchGuard contextSwitchGuard;

        // XXX(babenko)
        Bootstrap_->GetControllerAgent()->GetMasterConnector()->OnMasterDisconnected();

        const auto& responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Stop();

        CachedExecNodeMemoryDistributionByTags_->Stop();

        LogEventFluently(ELogEventType::MasterDisconnected)
            .Item("address").Value(ServiceAddress_);

        auto error = TError("Master disconnected");

        if (Config_->TestingOptions->MasterDisconnectDelay) {
            Sleep(*Config_->TestingOptions->MasterDisconnectDelay);
        }

        {
            LOG_INFO("Aborting jobs at node shards");

            std::vector<TFuture<void>> asyncResults;
            for (const auto& nodeShard : NodeShards_) {
                asyncResults.push_back(BIND(&TNodeShard::AbortAllJobs, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run(error));
            }

            // XXX(babenko)
            Combine(asyncResults)
                .Get();
        }

        {
            LOG_INFO("Forgetting operations");

            auto idToOperation = IdToOperation_;
            for (const auto& pair : idToOperation) {
                const auto& operation = pair.second;
                LOG_INFO("Forgetting operation (OperationId: %v)", operation->GetId());
                if (!operation->IsFinishedState()) {
                    operation->Cancel();
                    operation->SetForgotten(true);
                    SetOperationFinalState(
                        operation,
                        EOperationState::Aborted,
                        error);
                }
                FinishOperation(operation);
            }

            YCHECK(IdToOperation_.empty());
        }

        {
            LOG_INFO("Disconnecting node shards");

            std::vector<TFuture<void>> asyncResults;
            for (const auto& nodeShard : NodeShards_) {
                asyncResults.push_back(BIND(&TNodeShard::OnMasterDisconnected, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run());
            }

            // XXX(babenko)
            Combine(asyncResults)
                .Get();
        }

        Strategy_->ResetState();

        LOG_INFO("Finished scheduler state cleanup");
    }

    void LogOperationFinished(const TOperationPtr& operation, ELogEventType logEventType, TError error)
    {
        LogEventFluently(logEventType)
            .Do(BIND(&TImpl::BuildOperationInfoForEventLog, MakeStrong(this), operation))
            .Item("start_time").Value(operation->GetStartTime())
            .Item("finish_time").Value(operation->GetFinishTime())
            .Item("controller_time_statistics").Value(operation->ControllerTimeStatistics())
            .Item("error").Value(error);
    }


    void ValidateOperationState(const TOperationPtr& operation, EOperationState expectedState)
    {
        if (operation->GetState() != expectedState) {
            LOG_INFO("Operation has unexpected state (OperationId: %v, State: %v, ExpectedState: %v)",
                operation->GetId(),
                operation->GetState(),
                expectedState);
            throw TFiberCanceledException();
        }
    }


    void RequestPools(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        static const TPoolTreeKeysHolder PoolTreeKeysHolder;

        LOG_INFO("Updating pools");
        auto req = TYPathProxy::Get(GetPoolsPath());
        ToProto(req->mutable_attributes()->mutable_keys(), PoolTreeKeysHolder.Keys);
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
        const TOperationPtr& operation,
        const TObjectServiceProxy::TReqExecuteBatchPtr& batchReq)
    {
        static auto runtimeParamsTemplate = New<TOperationRuntimeParams>();

        {
            auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()) + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), runtimeParamsTemplate->GetRegisteredKeys());
            batchReq->AddRequest(req, "get_runtime_params");
        }

        {
            auto req = TYPathProxy::Get(GetNewOperationPath(operation->GetId()) + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), runtimeParamsTemplate->GetRegisteredKeys());
            batchReq->AddRequest(req, "get_runtime_params_new");
        }
    }

    void HandleOperationRuntimeParams(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_runtime_params");
        auto rspOrErrorNew = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_runtime_params_new");

        auto storageMode = operation->GetStorageMode();

        auto* rspOrErrorPtr = storageMode == EOperationCypressStorageMode::HashBuckets
            ? &rspOrErrorNew
            : &rspOrError;

        if (!rspOrErrorPtr->IsOK()) {
            LOG_WARNING(*rspOrErrorPtr, "Error updating operation runtime parameters (OperationId: %v)",
                operation->GetId());
        }

        const auto& rsp = rspOrErrorPtr->Value();
        auto runtimeParamsNode = ConvertToNode(TYsonString(rsp->value()));

        try {
            auto newRuntimeParams = CloneYsonSerializable(operation->GetRuntimeParams());
            if (ReconfigureYsonSerializable(newRuntimeParams, runtimeParamsNode)) {
                if (operation->GetOwners() != newRuntimeParams->Owners) {
                    operation->SetOwners(newRuntimeParams->Owners);
                }
                operation->SetRuntimeParams(newRuntimeParams);
                Strategy_->UpdateOperationRuntimeParams(operation.Get(), newRuntimeParams);
                LOG_INFO("Operation runtime parameters updated (OperationId: %v)",
                    operation->GetId());
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing operation runtime parameters (OperationId: %v)",
                operation->GetId());
        }
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
                auto error = TError("Error updating scheduler configuration")
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
            ValidateConfig();

            for (const auto& nodeShard : NodeShards_) {
                BIND(&TNodeShard::UpdateConfig, nodeShard, Config_)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run();
            }

            Strategy_->UpdateConfig(Config_);
            MasterConnector_->UpdateConfig(Config_);

            // TODO(ignat): Make separate logic for updating config in controller agent (after separating configs).
            Bootstrap_->GetControllerAgent()->UpdateConfig(Config_);

            ReconfigurableJobSpecSliceThrottler_->Reconfigure(Config_->JobSpecSliceThrottler);

            LoggingExecutor_->SetPeriod(Config_->ClusterInfoLoggingPeriod);
            UpdateExecNodeDescriptorsExecutor_->SetPeriod(Config_->UpdateExecNodeDescriptorsPeriod);
            ProfilingExecutor_->SetPeriod(Config_->ProfilingUpdatePeriod);

            EventLogWriter_->UpdateConfig(Config_->EventLog);
        }
    }


    void RequestOperationArchiveVersion(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating operation archive version");
        auto req = TYPathProxy::Get(GetOperationsArchiveVersionPath());
        batchReq->AddRequest(req, "get_operation_archive_version");
    }

    void HandleOperationArchiveVersion(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operation_archive_version");
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error getting operation archive version");
            return;
        }

        try {
            OperationArchiveVersion_.store(
                ConvertTo<int>(TYsonString(rspOrError.Value()->value())),
                std::memory_order_relaxed);
            SetSchedulerAlert(ESchedulerAlertType::UpdateArchiveVersion, TError());
        } catch (const std::exception& ex) {
            auto error = TError("Error parsing operation archive version")
                << ex;
            SetSchedulerAlert(ESchedulerAlertType::UpdateArchiveVersion, error);
        }
    }


    void UpdateExecNodeDescriptors()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TFuture<TExecNodeDescriptorListPtr>> shardDescriptorsFutures;
        for (const auto& nodeShard : NodeShards_) {
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
            ShouldSendExecNodeDescriptorToControllerAgent_ = true;
        }

        auto execNodeMemoryDistribution = CalculateMemoryDistribution(EmptySchedulingTagFilter);
        {
            TWriterGuard guard(ExecNodeDescriptorsLock_);
            CachedExecNodeMemoryDistribution_ = execNodeMemoryDistribution;
        }
    }

    virtual TExecNodeDescriptorListPtr CalculateExecNodeDescriptors(const TSchedulingTagFilter& filter) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TExecNodeDescriptorListPtr descriptors;
        {
            TReaderGuard guard(ExecNodeDescriptorsLock_);
            descriptors = CachedExecNodeDescriptors_;
        }

        if (filter.IsEmpty()) {
            return descriptors;
        }

        auto result = New<TExecNodeDescriptorList>();
        for (const auto& descriptor : descriptors->Descriptors) {
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

    void DoStartOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationState(operation, EOperationState::Initializing);

        bool registered = false;
        try {
            auto controller = CreateControllerForOperation(Bootstrap_->GetControllerAgent(), operation.Get());
            operation->SetController(controller);

            Strategy_->ValidateOperationCanBeRegistered(operation.Get());

            RegisterOperation(operation);
            // XXX(babenko): Ignore result? (we cannot throw error here)
            Bootstrap_->GetControllerAgent()->RegisterController(operation->GetId(), controller);

            registered = true;

            {
                auto asyncResult = BIND(&IOperationControllerSchedulerHost::Initialize, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
                auto error = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            operation->ControllerAttributes().InitializationAttributes = controller->GetInitializationAttributes();

            WaitFor(MasterConnector_->CreateOperationNode(operation))
                .ThrowOnError();

            MasterConnector_->StartOperationNodeUpdates(operation);
            // XXX(babenko)
            Bootstrap_->GetControllerAgent()->GetMasterConnector()->StartOperationNodeUpdates(operation->GetId(), operation->GetStorageMode());

            ValidateOperationState(operation, EOperationState::Initializing);
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
            .Do(std::bind(&TImpl::BuildOperationInfoForEventLog, MakeStrong(this), operation, _1))
            .Do(std::bind(&ISchedulerStrategy::BuildOperationInfoForEventLog, Strategy_, operation.Get(), _1));

        // NB: Once we've registered the operation in Cypress we're free to complete
        // StartOperation request. Preparation will happen in a separate fiber in a non-blocking
        // fashion.
        auto controller = operation->GetController();
        BIND(&TImpl::DoPrepareOperation, MakeStrong(this), operation)
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run();

        operation->SetStarted(TError());
    }

    void DoPrepareOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationState(operation, EOperationState::Initializing);

        const auto& operationId = operation->GetId();

        LOG_INFO("Preparing operation (OperationId: %v)",
            operationId);

        operation->SetState(EOperationState::Preparing);

        try {
            // Run async preparation.
            auto controller = operation->GetController();
            auto asyncResult = BIND(&IOperationControllerSchedulerHost::Prepare, controller)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run();

            TWallTimer timer;
            auto result = WaitFor(asyncResult);
            auto prepareDuration = timer.GetElapsedTime();
            operation->UpdateControllerTimeStatistics("/prepare", prepareDuration);

            THROW_ERROR_EXCEPTION_IF_FAILED(result);

            operation->ControllerAttributes().Attributes = controller->GetAttributes();

            ValidateOperationState(operation, EOperationState::Preparing);

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
            .Item("operation_id").Value(operationId)
            .Item("unrecognized_spec").Value(operation->ControllerAttributes().InitializationAttributes->UnrecognizedSpec);

        // From this moment on the controller is fully responsible for the
        // operation's fate.
    }

    void RegisterRevivingOperation(const TOperationPtr& operation)
    {
        auto codicilGuard = operation->MakeCodicilGuard();

        const auto& operationId = operation->GetId();

        LOG_INFO("Registering operation for revival (OperationId: %v)",
            operationId);

        operation->SetSchedulerIncarnation(SchedulerIncarnation_);

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

        IOperationControllerPtr controller;
        try {
            controller = CreateControllerForOperation(Bootstrap_->GetControllerAgent(), operation.Get());
            operation->SetController(controller);

            Strategy_->ValidateOperationCanBeRegistered(operation.Get());
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);

            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            SetOperationFinalState(operation, EOperationState::Failed, wrappedError);

            // No need to wait for the outcome.
            Y_UNUSED(MasterConnector_->FlushOperationNode(operation));
            return;
        }

        // NB: Should not throw!
        RegisterOperation(operation);
        // XXX(babenko)
        // Ignore result? (we cannot throw error here)
        Bootstrap_->GetControllerAgent()->RegisterController(operation->GetId(), controller);
    }

    void DoReviveOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationState(operation, EOperationState::Reviving);

        auto revivalDescriptor = *operation->RevivalDescriptor();
        operation->RevivalDescriptor().Reset();

        auto operationId = operation->GetId();
        LOG_INFO("Reviving operation (OperationId: %v, SchedulerIncarnation: %v)",
            operationId,
            SchedulerIncarnation_);

        try {
            auto controller = operation->GetController();

            {
                auto asyncResult = BIND(&IOperationControllerSchedulerHost::InitializeReviving, controller, revivalDescriptor.ControllerTransactions)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
                auto error = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(error);

                operation->ControllerAttributes().InitializationAttributes = controller->GetInitializationAttributes();
            }

            ValidateOperationState(operation, EOperationState::Reviving);

            {
                auto error = WaitFor(MasterConnector_->ResetRevivingOperationNode(operation));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            ValidateOperationState(operation, EOperationState::Reviving);

            {
                auto asyncResult = BIND(&IOperationControllerSchedulerHost::Revive, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
                auto error = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(error);

                operation->ControllerAttributes().Attributes = controller->GetAttributes();
            }

            ValidateOperationState(operation, EOperationState::Reviving);

            LOG_INFO("Operation has been revived (OperationId: %v)",
                operationId);

            operation->SetState(EOperationState::Pending);
            operation->SetPrepared(true);

            if (operation->GetActivated()) {
                MaterializeOperation(operation);
            }
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);
            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            OnOperationFailed(operationId, wrappedError);
        }
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
        for (const auto& job : jobs) {
            auto shardId = GetNodeShardId(NodeIdFromJobId(job->GetId()));
            jobsByShardId[shardId].emplace_back(std::move(job));
        }
        
        std::vector<TFuture<void>> asyncResults;
        for (int shardId = 0; shardId < NodeShards_.size(); ++shardId) {
            if (jobsByShardId[shardId].empty()) {
                continue;
            }
            auto asyncResult = BIND(&TNodeShard::RegisterRevivedJobs, NodeShards_[shardId])
                .AsyncVia(NodeShards_[shardId]->GetInvoker())
                .Run(operation->GetId(), std::move(jobsByShardId[shardId]));
            asyncResults.emplace_back(std::move(asyncResult));
        }
        return Combine(asyncResults);
    }

    void RegisterOperation(const TOperationPtr& operation)
    {
        YCHECK(IdToOperation_.insert(std::make_pair(operation->GetId(), operation)).second);
        for (const auto& nodeShard : NodeShards_) {
            BIND(&TNodeShard::RegisterOperation, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run(operation->GetId(), operation->GetController());
        }

        Strategy_->RegisterOperation(operation.Get());

        MasterConnector_->AddOperationWatcherRequester(
            operation,
            BIND(&TImpl::RequestOperationRuntimeParams, Unretained(this), operation));
        MasterConnector_->AddOperationWatcherHandler(
            operation,
            BIND(&TImpl::HandleOperationRuntimeParams, Unretained(this), operation));

        LOG_DEBUG("Operation registered (OperationId: %v)",
            operation->GetId());
    }

    void AbortOperationJobs(const TOperationPtr& operation, const TError& error, bool terminated)
    {
        std::vector<TFuture<void>> abortFutures;
        for (const auto& nodeShard : NodeShards_) {
            abortFutures.push_back(BIND(&TNodeShard::AbortOperationJobs, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run(operation->GetId(), error, terminated));
        }
        WaitFor(Combine(abortFutures))
            .ThrowOnError();
    }

    void UnregisterOperation(const TOperationPtr& operation)
    {
        YCHECK(IdToOperation_.erase(operation->GetId()) == 1);
        for (const auto& nodeShard : NodeShards_) {
            BIND(&TNodeShard::UnregisterOperation, nodeShard, operation->GetId())
                .AsyncVia(nodeShard->GetInvoker())
                .Run();
        }

        Strategy_->UnregisterOperation(operation.Get());

        Bootstrap_->GetControllerAgent()->UnregisterController(operation->GetId());

        LOG_DEBUG("Operation unregistered (OperationId: %v)",
            operation->GetId());
    }

    void BuildOperationInfoForEventLog(const TOperationPtr& operation, TFluentMap fluent)
    {
        fluent
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .Item("spec").Value(operation->GetSpec())
            .Item("authenticated_user").Value(operation->GetAuthenticatedUser());
    }

    void SetOperationFinalState(const TOperationPtr& operation, EOperationState state, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!operation->GetStarted().IsSet()) {
            operation->SetStarted(error);
        }
        operation->SetState(state);
        operation->SetFinishTime(TInstant::Now());
        ToProto(operation->MutableResult().mutable_error(), error);
    }

    void FinishOperation(const TOperationPtr& operation)
    {
        if (!operation->GetFinished().IsSet()) {
            operation->SetFinished();
            operation->SetController(nullptr);
            UnregisterOperation(operation);
        }
    }

    INodePtr GetSpecTemplate(EOperationType type, const IMapNodePtr& spec)
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


    void OnOperationCompleted(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        MasterConnector_->GetCancelableControlInvoker()->Invoke(BIND(
            &TImpl::DoCompleteOperation,
            MakeStrong(this),
            operationId));
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
                // Result is ignored since failure causes scheduler disconnection.
                Y_UNUSED(WaitFor(asyncResult));
                ValidateOperationState(operation, EOperationState::Completing);
            }

            {
                auto controller = operation->GetController();
                auto asyncResult = BIND(&IOperationControllerSchedulerHost::Commit, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
                WaitFor(asyncResult)
                    .ThrowOnError();
                if (operation->GetForgotten()) {
                    // Master disconnected happend while committing controller.
                    return;
                }

                ValidateOperationState(operation, EOperationState::Completing);

                if (Config_->TestingOptions->FinishOperationTransitionDelay) {
                    Sleep(*Config_->TestingOptions->FinishOperationTransitionDelay);
                    if (operation->GetForgotten()) {
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

            // Notify controller that it is going to be disposed.
            if (const auto& controller = operation->GetController()) {
                controller->GetInvoker()->Invoke(BIND(&IOperationControllerSchedulerHost::OnBeforeDisposal, controller));
            }

            FinishOperation(operation);
        } catch (const std::exception& ex) {
            OnOperationFailed(operation->GetId(), ex);
            return;
        }

        LOG_INFO("Operation completed (OperationId: %v)",
             operationId);

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }


    void OnOperationFailed(const TOperationId& operationId, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoFailOperation, MakeStrong(this), operationId, error));
    }

    void DoFailOperation(const TOperationId& operationId, const TError& error)
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


    void OnOperationAborted(const TOperationId& operationId, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        MasterConnector_->GetCancelableControlInvoker()->Invoke(BIND(
            static_cast<void(TImpl::*)(const TOperationId&, const TError& error)>(&TImpl::DoAbortOperation),
            MakeStrong(this),
            operationId,
            error));
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

    void DoAbortOperation(const TOperationId& operationId, const TError& error)
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


    void OnOperationSuspended(const TOperationId& operationId, const TError& error)
    {
        MasterConnector_->GetCancelableControlInvoker()->Invoke(BIND(
            &TImpl::DoSuspendOperation,
            MakeStrong(this),
            operationId,
            error,
            /* abortRunningJobs */ true,
            /* setAlert */ true));
    }

    void DoSuspendOperation(
        const TOperationId& operationId,
        const TError& error,
        bool abortRunningJobs,
        bool setAlert)
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
            operationId);
    }


    void TerminateOperation(
        const TOperationPtr& operation,
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
            // Result is ignored since failure causes scheduler disconnection.
            Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));
            if (operation->GetState() != intermediateState) {
                return;
            }
        }


        if (Config_->TestingOptions->FinishOperationTransitionDelay) {
            Sleep(*Config_->TestingOptions->FinishOperationTransitionDelay);
            if (operation->GetForgotten()) {
                // Master disconnect happened while committing controller.
                return;
            }
        }

        operation->Cancel();

        if (auto controller = operation->GetController()) {
            try {
                controller->Abort();
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Failed to abort controller (OperationId: %v)", operation->GetId());
                MasterConnector_->Disconnect();
                return;
            }
        }

        SetOperationFinalState(operation, finalState, error);

        // Second flush: ensure that the state is changed to its final value.
        {
            // Result is ignored since failure causes scheduler disconnection.
            Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));
            if (operation->GetState() != finalState) {
                return;
            }
        }

        // Notify controller that it is going to be disposed.
        if (auto controller = operation->GetController()) {
            auto error = WaitFor(
                BIND(&IOperationControllerSchedulerHost::OnBeforeDisposal, controller)
                    .AsyncVia(controller->GetInvoker())
                    .Run());
            YCHECK(error.IsOK() && "OnBeforeDisposal failed");
        }

        LogOperationFinished(operation, logEventType, error);

        FinishOperation(operation);
    }


    void CompleteOperationWithoutRevival(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO("Completing operation without revival (OperationId: %v)",
             operation->GetId());

        const auto& revivalDescriptor = *operation->RevivalDescriptor();
        if (revivalDescriptor.ShouldCommitOutputTransaction) {
            WaitFor(revivalDescriptor.ControllerTransactions->Output->Commit())
                .ThrowOnError();
        }

        SetOperationFinalState(operation, EOperationState::Completed, TError());

        // Result is ignored since failure causes scheduler disconnection.
        Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }

    void AbortOperationWithoutRevival(const TOperationPtr& operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO(error, "Aborting operation without revival (OperationId: %v)",
             operation->GetId());

        auto abortTransaction = [&] (ITransactionPtr transaction) {
            if (transaction) {
                // Fire-and-forget.
                transaction->Abort();
            }
        };

        const auto& controllerTransactions = operation->RevivalDescriptor()->ControllerTransactions;
        abortTransaction(controllerTransactions->Async);
        abortTransaction(controllerTransactions->Input);
        abortTransaction(controllerTransactions->Output);

        SetOperationFinalState(operation, EOperationState::Aborted, error);

        // Result is ignored since failure causes scheduler disconnection.
        Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));

        LogOperationFinished(operation, ELogEventType::OperationAborted, error);
    }

    void ReviveOperations()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        {
            LOG_INFO("Reviving operations");

            std::vector<TFuture<void>> asyncResults;
            auto idToOperation = IdToOperation_;
            for (const auto& pair : idToOperation) {
                const auto& operation = pair.second;
                auto asyncResult = BIND(&TImpl::DoReviveOperation, MakeStrong(this), operation)
                    .AsyncVia(operation->GetCancelableControlInvoker())
                    .Run();
                asyncResults.emplace_back(std::move(asyncResult));
            }

            // We need to all revivals to complete (either successfully or not) to proceed any further;
            // hence we use CombineAll rather than Combine.
            auto error = WaitFor(CombineAll(asyncResults));
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to revive operations")
                    << error;
            }
        }

        {
            LOG_INFO("Reviving node shards");

            std::vector<TFuture<void>> asyncResults;
            for (const auto& nodeShard : NodeShards_) {
                auto startFuture = BIND(&TNodeShard::StartReviving, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run();
                asyncResults.emplace_back(std::move(startFuture));
            }

            auto error = WaitFor(Combine(asyncResults));
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to start revival at node shards")
                    << error;
            }
        }
    }

    void ProcessHandshakeOperations(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Checking operations for revival");
        for (const auto& operation : operations) {
            YCHECK(operation->RevivalDescriptor());
            const auto& revivalDescriptor = *operation->RevivalDescriptor();

            MasterConnector_->StartOperationNodeUpdates(operation);
            operation->SetState(EOperationState::Reviving);

            if (revivalDescriptor.OperationCommitted) {
                // TODO(babenko): parallelize
                CompleteOperationWithoutRevival(operation);
            } else if (revivalDescriptor.OperationAborting) {
                // TODO(babenko): parallelize
                // TODO(babenko): error is lost
                AbortOperationWithoutRevival(
                    operation,
                    TError("Operation aborted since it was found in \"aborting\" state during scheduler revival"));
            } else if (revivalDescriptor.UserTransactionAborted) {
                // TODO(babenko): parallelize
                AbortOperationWithoutRevival(
                    operation,
                    GetUserTransactionAbortedError(operation->GetUserTransactionId()));
            } else {
                RegisterRevivingOperation(operation);
            }
        }
    }


    void RemoveExpiredResourceLimitsTags()
    {
        std::vector<TSchedulingTagFilter> toRemove;
        for (const auto& pair : CachedResourceLimitsByTags_) {
            const auto& filter = pair.first;
            const auto& record = pair.second;
            if (record.first + DurationToCpuDuration(Config_->SchedulingTagFilterExpireTimeout) < GetCpuInstant()) {
                toRemove.push_back(filter);
            }
        }

        for (const auto& filter : toRemove) {
            YCHECK(CachedResourceLimitsByTags_.erase(filter) == 1);
        }
    }

    void BuildStaticOrchid(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        RemoveExpiredResourceLimitsTags();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("connected").Value(IsConnected())
                .Item("cell").BeginMap()
                    .Item("resource_limits").Value(GetTotalResourceLimits())
                    .Item("resource_usage").Value(GetTotalResourceUsage())
                    .Item("exec_node_count").Value(GetExecNodeCount())
                    .Item("total_node_count").Value(GetTotalNodeCount())
                    .Item("nodes_memory_distribution").Value(GetExecNodeMemoryDistribution(TSchedulingTagFilter()))
                    .Item("resource_limits_by_tags")
                        .DoMapFor(CachedResourceLimitsByTags_, [] (TFluentMap fluent, const auto& pair) {
                            const auto& filter = pair.first;
                            const auto& record = pair.second;
                            if (!filter.IsEmpty()) {
                                fluent.Item(filter.GetBooleanFormula().GetFormula()).Value(record.second);
                            }
                        })
                .EndMap()
                .Item("suspicious_jobs").BeginMap()
                    .Items(SuspiciousJobsYson_)
                .EndMap()
                .Item("nodes").BeginMap()
                    .Do([=] (TFluentMap fluent) {
                        for (const auto& nodeShard : NodeShards_) {
                            auto asyncResult = WaitFor(
                                BIND(&TNodeShard::BuildNodesYson, nodeShard, fluent)
                                    .AsyncVia(nodeShard->GetInvoker())
                                    .Run());
                            asyncResult.ThrowOnError();
                        }
                    })
                .EndMap()
                .Item("config").Value(Config_)
                .Do(std::bind(&ISchedulerStrategy::BuildOrchid, Strategy_, _1))
            .EndMap();
    }

    void BuildOperationYson(
        const TOperationPtr& operation,
        const TControllerAgentServiceProxy::TErrorOrRspGetOperationInfoPtr& rspOrError,
        IYsonConsumer* consumer) const
    {
        static const auto emptyMapFragment = TYsonString(TString(), EYsonType::MapFragment);

        auto codicilGuard = operation->MakeCodicilGuard();

        auto toYsonString = [] (const TProtoStringType& protoString) {
            return protoString.empty() ? emptyMapFragment : TYsonString(protoString, EYsonType::MapFragment);
        };

        bool isOK = rspOrError.IsOK();
        if (!isOK) {
            LOG_DEBUG(rspOrError, "Failed to get operation info from controller");
        }
        
        auto controllerProgress = isOK ? toYsonString(rspOrError.Value()->progress()) : emptyMapFragment;
        auto controllerBriefProgress = isOK ? toYsonString(rspOrError.Value()->brief_progress()) : emptyMapFragment;
        auto controllerRunningJobs = isOK ? toYsonString(rspOrError.Value()->running_jobs()) : emptyMapFragment;
        auto controllerJobSplitterInfo = isOK ? toYsonString(rspOrError.Value()->job_splitter()) : emptyMapFragment;
        auto controllerMemoryDigest = isOK ? toYsonString(rspOrError.Value()->memory_digest()) : emptyMapFragment;

        BuildYsonFluently(consumer)
            .BeginMap()
                .Do(BIND(&NScheduler::BuildFullOperationAttributes, operation))
                .Item("progress").BeginMap()
                    .Do(BIND(&ISchedulerStrategy::BuildOperationProgress, Strategy_, operation->GetId()))
                    .Items(controllerProgress)
                .EndMap()
                .Item("brief_progress").BeginMap()
                    .Do(BIND(&ISchedulerStrategy::BuildBriefOperationProgress, Strategy_, operation->GetId()))
                    .Items(controllerBriefProgress)
                .EndMap()
                .Item("running_jobs")
                    .BeginAttributes()
                        .Item("opaque").Value("true")
                    .EndAttributes()
                    .BeginMap()
                        .Items(controllerRunningJobs)
                    .EndMap()
                .Item("job_splitter")
                    .BeginAttributes()
                        .Item("opaque").Value("true")
                    .EndAttributes()
                    .BeginMap()
                        .Items(controllerJobSplitterInfo)
                    .EndMap()
                .Items(controllerMemoryDigest)
            .EndMap();
    }

    IYPathServicePtr GetDynamicOrchidService()
    {
        auto dynamicOrchidService = New<TCompositeMapService>();
        dynamicOrchidService->AddChild("operations", New<TOperationsService>(this));
        dynamicOrchidService->AddChild("jobs", New<TJobsService>(this));
        return dynamicOrchidService;
    }

    void ValidateConfig()
    {
        // First reset the alert.
        SetSchedulerAlert(ESchedulerAlertType::UnrecognizedConfigOptions, TError());

        if (!Config_->EnableUnrecognizedAlert) {
            return;
        }

        auto unrecognized = Config_->GetUnrecognizedRecursively();
        if (unrecognized && unrecognized->GetChildCount() > 0) {
            LOG_WARNING("Scheduler config contains unrecognized options (Unrecognized: %v)",
                ConvertToYsonString(unrecognized, EYsonFormat::Text));
            SetSchedulerAlert(
                ESchedulerAlertType::UnrecognizedConfigOptions,
                TError("Scheduler config contains unrecognized options")
                    << TErrorAttribute("unrecognized", unrecognized));
        }
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
            auto operationId = TOperationId::FromString(key);
            auto operation = Scheduler_->FindOperation(operationId);
            if (!operation) {
                return nullptr;
            }

            TControllerAgentServiceProxy proxy(Scheduler_->Bootstrap_->GetLocalRpcChannel());
            proxy.SetDefaultTimeout(Scheduler_->Config_->ControllerAgentOperationRpcTimeout);
            auto request = proxy.GetOperationInfo();
            ToProto(request->mutable_operation_id(), operationId);
            auto rspOrError = WaitFor(request->Invoke());

            // Operation can be unregistered, since request to controller agent is asynchronous.
            if (!Scheduler_->FindOperation(operationId)) {
                return nullptr;
            }

            return IYPathService::FromProducer(
                BIND(&TScheduler::TImpl::BuildOperationYson, MakeStrong(Scheduler_), operation, rspOrError));
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
            auto jobId = TJobId::FromString(key);
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

            TControllerAgentServiceProxy proxy(Scheduler_->Bootstrap_->GetLocalRpcChannel());
            proxy.SetDefaultTimeout(Scheduler_->Config_->ControllerAgentOperationRpcTimeout);
            auto request = proxy.GetJobInfo();
            ToProto(request->mutable_operation_id(), operationId);
            ToProto(request->mutable_job_id(), jobId);
            auto response = WaitFor(request->Invoke())
                .ValueOrThrow();

            consumer->OnRaw(TYsonString(response->info()));
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

void TScheduler::Disconnect()
{
    Impl_->Disconnect();
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

TFuture<void> TScheduler::AbortJob(const TJobId& jobId, TNullable<TDuration> interruptTimeout, const TString& user)
{
    return Impl_->AbortJob(jobId, interruptTimeout, user);
}

void TScheduler::ProcessNodeHeartbeat(const TCtxNodeHeartbeatPtr& context)
{
    Impl_->ProcessNodeHeartbeat(context);
}

void TScheduler::ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
{
    Impl_->ProcessAgentHeartbeat(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
