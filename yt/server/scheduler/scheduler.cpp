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
#include "controller_agent_tracker.h"
#include "controller_agent.h"
#include "operation_controller.h"
#include "bootstrap.h"
#include "config.h"
#include "operations_cleaner.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/controller_agent_service_proxy.h>

#include <yt/server/exec_agent/public.h>

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
#include <yt/core/misc/sync_expiring_cache.h>

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

using NControllerAgent::IOperationControllerSchedulerHost;
using NControllerAgent::TOperationControllerHost;
using NControllerAgent::TControllerAgentServiceProxy;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;
using NNodeTrackerClient::TNodeDirectory;

using NScheduler::NProto::TRspStartOperation;

using std::placeholders::_1;

using NYT::FromProto;
using NYT::ToProto;

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
        , MasterConnector_(std::make_unique<TMasterConnector>(Config_, Bootstrap_))
        , CachedExecNodeMemoryDistributionByTags_(New<TSyncExpiringCache<TSchedulingTagFilter, TMemoryDistribution>>(
            BIND(&TImpl::CalculateMemoryDistribution, MakeStrong(this)),
            Config_->SchedulingTagFilterExpireTimeout,
            GetControlInvoker(EControlQueue::PeriodicActivity)))
        , TotalResourceLimitsProfiler_(Profiler.GetPathPrefix() + "/total_resource_limits")
        , TotalResourceUsageProfiler_(Profiler.GetPathPrefix() + "/total_resource_usage")
        , TotalCompletedJobTimeCounter_("/total_completed_job_time")
        , TotalFailedJobTimeCounter_("/total_failed_job_time")
        , TotalAbortedJobTimeCounter_("/total_aborted_job_time")
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_THREAD_AFFINITY(GetControlInvoker(EControlQueue::Default), ControlThread);

        for (int index = 0; index < Config_->NodeShardCount; ++index) {
            NodeShards_.push_back(New<TNodeShard>(
                index,
                Config_,
                this,
                Bootstrap_));
            CancelableNodeShardInvokers_.push_back(GetNullInvoker());
        }

        OperationsCleaner_ = New<TOperationsCleaner>(Config_->OperationsCleaner, Bootstrap_);

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
            &TImpl::RequestPoolTrees,
            Unretained(this)));
        MasterConnector_->AddGlobalWatcherHandler(BIND(
            &TImpl::HandlePoolTrees,
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
        MasterConnector_->SubscribeMasterHandshake(BIND(
            &TImpl::OnMasterHandshake,
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
            Config_->ExecNodeDescriptorsUpdatePeriod);
        UpdateExecNodeDescriptorsExecutor_->Start();
    }

    const NApi::INativeClientPtr& GetMasterClient() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetMasterClient();
    }

    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

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

    TRefCountedExecNodeDescriptorMapPtr GetCachedExecNodeDescriptors()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeDescriptorsLock_);
        return CachedExecNodeDescriptors_;
    }

    const TSchedulerConfigPtr& GetConfig() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_;
    }

    const std::vector<TNodeShardPtr>& GetNodeShards() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NodeShards_;
    }

    const IInvokerPtr& GetCancelableNodeShardInvoker(int shardId) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CancelableNodeShardInvokers_[shardId];
    }

    bool IsConnected() const
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

    TMasterConnector* GetMasterConnector() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return MasterConnector_.get();
    }


    void Disconnect(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        MasterConnector_->Disconnect(error);
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
        const TError& alert,
        TNullable<TDuration> timeout = Null) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoSetOperationAlert, MakeStrong(this), operationId, alertType, alert, timeout)
            .AsyncVia(GetControlInvoker(EControlQueue::Operation))
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
        auto result = WaitFor(client->CheckPermission(user, GetPoolTreesPath() + path, permission))
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

        const auto& client = GetMasterClient();

        std::vector<NYTree::TYPath> paths = {
            GetOperationPath(operationId),
            GetNewOperationPath(operationId)
        };

        for (const auto& path : paths) {
            auto asyncResult = client->CheckPermission(user, path, permission);
            auto resultOrError = WaitFor(asyncResult);
            if (!resultOrError.IsOK()) {
                if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    continue;
                }

                THROW_ERROR_EXCEPTION("Error checking permission for operation %v",
                    operationId)
                    << resultOrError;
            }

            const auto& result = resultOrError.Value();
            if (result.Action == ESecurityAction::Allow) {
                ValidateConnected();
                LOG_DEBUG("Operation permission successfully validated (Permission: %v, User: %v, OperationId: %v)",
                    permission,
                    user,
                    operationId);
                return;
            }
        }

        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AuthorizationError,
            "User %Qv has been denied access to operation %v",
            user,
            operationId);
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

        if (Config_->SpecTemplate) {
            specNode = PatchNode(Config_->SpecTemplate, specNode)->AsMap();
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

        auto runtimeParams = New<TOperationRuntimeParameters>();
        runtimeParams->Owners = spec->Owners;
        // NOTE: At this point not all runtime params are filled since there are options that
        // are unknown until operation is registered in strategy (e.g. trees in which operation will run).
        // These unknown runtime params will be filled inside strategy.

        auto operation = New<TOperation>(
            operationId,
            type,
            mutationId,
            transactionId,
            specNode,
            secureVault,
            runtimeParams,
            user,
            TInstant::Now(),
            spec->EnableCompatibleStorageMode,
            MasterConnector_->GetCancelableControlInvoker(EControlQueue::Operation));
        operation->SetStateAndEnqueueEvent(EOperationState::Starting);

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO("Starting operation (OperationType: %v, OperationId: %v, TransactionId: %v, User: %v)",
            type,
            operationId,
            transactionId,
            user);

        LOG_INFO("Total resource limits (OperationId: %v, ResourceLimits: %v)",
            operationId,
            FormatResources(GetTotalResourceLimits()));

        try {
            WaitFor(Strategy_->ValidateOperationStart(operation.Get()))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to start")
                << ex;
            operation->SetStarted(wrappedError);
            THROW_ERROR(wrappedError);
        }

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoStartOperation, MakeStrong(this), operation));

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

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoAbortOperation, MakeStrong(this), operation, error));

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
            operation,
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
                "Operation is in %Qlv state",
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
        operation->ResetAlert(EOperationAlertType::OperationSuspended);

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
                "Operation is in %Qlv state",
                operation->GetState()));
        }

        LOG_INFO(error, "Completing operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        const auto& controller = operation->GetController();
        auto completeError = WaitFor(controller->Complete());
        if (!completeError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to complete operation %v", operation->GetId())
                << completeError;
        }

        return operation->GetFinished();
    }

    void OnOperationCompleted(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoCompleteOperation, MakeStrong(this), operation));
    }

    void OnOperationAborted(const TOperationPtr& operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoAbortOperation, MakeStrong(this), operation, error));
    }

    void OnOperationFailed(const TOperationPtr& operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoFailOperation, MakeStrong(this), operation, error));
    }

    void OnOperationSuspended(const TOperationPtr& operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->GetCancelableControlInvoker()->Invoke(BIND(
            &TImpl::DoSuspendOperation,
            MakeStrong(this),
            operation,
            error,
        /* abortRunningJobs */ true,
        /* setAlert */ true));
    }

    void OnOperationAgentUnregistered(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& controller = operation->GetController();
        controller->RevokeAgent();

        Strategy_->DisableOperation(operation.Get());

        operation->Restart();
        operation->SetStateAndEnqueueEvent(EOperationState::Orphaned);

        for (const auto& nodeShard : NodeShards_) {
            nodeShard->GetInvoker()->Invoke(BIND(
                &TNodeShard::StartOperationRevival,
                nodeShard,
                operation->GetId()));
        }

        AddOperationToTransientQueue(operation);
    }

    void OnOperationBannedInTentativeTree(const TOperationPtr& operation, const TString& treeId)
    {
        GetControlInvoker(EControlQueue::Operation)->Invoke(
            BIND(&ISchedulerStrategy::UnregisterOperationFromTree, GetStrategy(), operation->GetId(), treeId));
    }

    void DoUpdateOperationParameters(
        TOperationPtr operation,
        const TOperationRuntimeParametersPtr& runtimeParams)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        // Not applying runtime params until they are persisted in Cypress.
        auto resultOrError = MasterConnector_->FlushOperationRuntimeParameters(operation, runtimeParams);
        WaitFor(resultOrError)
            .ThrowOnError();

        if (runtimeParams->Owners && operation->GetOwners() != *runtimeParams->Owners) {
            operation->SetOwners(*runtimeParams->Owners);
        }

        operation->SetRuntimeParameters(runtimeParams);
        Strategy_->UpdateOperationRuntimeParameters(operation.Get());

        // Updating ACL and other attributes.
        WaitFor(MasterConnector_->FlushOperationNode(operation))
            .ThrowOnError();

        LogEventFluently(ELogEventType::RuntimeParametersInfo)
            .Item("runtime_params").Value(runtimeParams);

        LOG_INFO("Operation runtime parameters updated (OperationId: %v)",
            operation->GetId());
    }

    TFuture<void> UpdateOperationParameters(TOperationPtr operation, const TString& user, INodePtr parameters)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidateOperationPermission(user, operation->GetId(), EPermission::Write);

        auto newRuntimeParams = UpdateYsonSerializable(
            operation->GetRuntimeParameters(),
            parameters);

        auto updateFuture = BIND(&TImpl::DoUpdateOperationParameters, MakeStrong(this))
            .AsyncVia(operation->GetCancelableControlInvoker())
            .Run(operation, newRuntimeParams);

        return updateFuture;
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
                &TNodeShard::AbortJobByUserRequest,
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

        // We extract operation states here as they may be accessed only from

        const auto& nodeShard = GetNodeShard(nodeId);
        nodeShard->GetInvoker()->Invoke(BIND(&TNodeShard::ProcessHeartbeat, nodeShard, context));
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

        auto operation = GetOperation(operationId);

        DoAbortOperation(operation, error);
    }

    void MaterializeOperation(const TOperationPtr& operation)
    {
        if (operation->GetState() != EOperationState::Pending) {
            // Operation can be in finishing state already.
            return;
        }

        LOG_INFO("Materializing operation (OperationId: %v, RevivedFromSnapshot: %v)",
            operation->GetId(),
            operation->GetRevivedFromSnapshot());

        TFuture<void> asyncResult;
        if (operation->GetRevivedFromSnapshot()) {
            operation->SetStateAndEnqueueEvent(EOperationState::RevivingJobs);
            asyncResult = RegisterJobsFromRevivedOperation(operation);
        } else {
            operation->SetStateAndEnqueueEvent(EOperationState::Materializing);
            asyncResult = Combine(std::vector<TFuture<void>>({
                operation->GetController()->Materialize(),
                ResetOperationRevival(operation)
            }));
        }

        auto expectedState = operation->GetState();
        asyncResult.Subscribe(
            BIND([=, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    return;
                }
                if (operation->GetState() != expectedState) {
                    return;
                }
                operation->SetStateAndEnqueueEvent(EOperationState::Running);
                Strategy_->EnableOperation(operation.Get());
            })
            .Via(operation->GetCancelableControlInvoker()));
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

    virtual IInvokerPtr GetControlInvoker(EControlQueue queue) const
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

    virtual TFuture<void> RegisterOrUpdateNode(
        TNodeId nodeId,
        const TString& nodeAddress,
        const THashSet<TString>& tags) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoRegisterOrUpdateNode, MakeStrong(this))
            .AsyncVia(GetControlInvoker(EControlQueue::NodeTracker))
            .Run(nodeId, nodeAddress, tags);
    }

    virtual void UnregisterNode(TNodeId nodeId, const TString& nodeAddress) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        GetControlInvoker(EControlQueue::NodeTracker)->Invoke(
            BIND([this, this_ = MakeStrong(this), nodeId, nodeAddress] {
                // NOTE: If node is unregistered from node shard before it becomes online
                // then its id can be missing in the map.
                auto it = NodeIdToTags_.find(nodeId);
                if (it == NodeIdToTags_.end()) {
                    LOG_WARNING("Node is not registered at scheduler (Address: %v)", nodeAddress);
                } else {
                    NodeIdToTags_.erase(it);
                    LOG_INFO("Node unregistered from scheduler (Address: %v)", nodeAddress);
                }
            }));
    }

    void DoRegisterOrUpdateNode(
        TNodeId nodeId,
        const TString& nodeAddress,
        const THashSet<TString>& tags)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Strategy_->ValidateNodeTags(tags);

        auto it = NodeIdToTags_.find(nodeId);
        if (it == NodeIdToTags_.end()) {
            YCHECK(NodeIdToTags_.emplace(nodeId, tags).second);
            LOG_INFO("Node is registered at scheduler (Address: %v, Tags: %v)",
                nodeAddress,
                tags);
        } else {
            it->second = tags;
            LOG_INFO("Node tags were updated at scheduler (Address: %v, NewTags: %v)",
                nodeAddress,
                tags);
        }
    }

    virtual const ISchedulerStrategyPtr& GetStrategy() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Strategy_;
    }

    const TOperationsCleanerPtr& GetOperationsCleaner() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return OperationsCleaner_;
    }

    TFuture<void> AttachJobContext(
        const NYTree::TYPath& path,
        const TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId,
        const TString& user) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TImpl::DoAttachJobContext, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker(EControlQueue::UserRequest))
            .Run(path, chunkId, operationId, jobId, user);
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
        VERIFY_THREAD_AFFINITY_ANY();

        return OperationArchiveVersion_.load();
    }

private:
    TSchedulerConfigPtr Config_;
    const TSchedulerConfigPtr InitialConfig_;
    TBootstrap* const Bootstrap_;

    const std::unique_ptr<TMasterConnector> MasterConnector_;
    std::atomic<bool> IsConnected_ = {false};

    //! Ordinal number of this scheduler incarnation. It is used
    //! to discard late callbacks that are submitted by still
    //! running controllers.
    //! This field is incremented on each OnMasterConnected and
    //! should be accessed only from scheduler control thread.
    int SchedulerIncarnation_ = -1;

    TOperationsCleanerPtr OperationsCleaner_;

    TActionQueuePtr ControllerDtor_ = New<TActionQueue>("ControllerDtor");

    ISchedulerStrategyPtr Strategy_;

    THashMap<TOperationId, TOperationPtr> IdToOperation_;

    mutable TReaderWriterSpinLock ExecNodeDescriptorsLock_;
    TRefCountedExecNodeDescriptorMapPtr CachedExecNodeDescriptors_ = New<TRefCountedExecNodeDescriptorMap>();

    TMemoryDistribution CachedExecNodeMemoryDistribution_;
    TIntrusivePtr<TSyncExpiringCache<TSchedulingTagFilter, TMemoryDistribution>> CachedExecNodeMemoryDistributionByTags_;

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
    TPeriodicExecutorPtr TransientOperationQueueScanPeriodExecutor_;

    TString ServiceAddress_;

    std::vector<TNodeShardPtr> NodeShards_;
    std::vector<IInvokerPtr> CancelableNodeShardInvokers_;

    THashMap<TNodeId, THashSet<TString>> NodeIdToTags_;

    THashMap<TSchedulingTagFilter, std::pair<TCpuInstant, TJobResources>> CachedResourceLimitsByTags_;

    TEventLogWriterPtr EventLogWriter_;
    std::unique_ptr<IYsonConsumer> EventLogWriterConsumer_;

    std::atomic<int> OperationArchiveVersion_ = {-1};

    TEnumIndexedVector<std::vector<TOperationPtr>, EOperationState> StateToTransientOperations_;
    TInstant OperationToAgentAssignmentFailureTime_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void DoAttachJobContext(
        const NYTree::TYPath& path,
        const TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId,
        const TString& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        MasterConnector_->AttachJobContext(path, chunkId, operationId, jobId, user);
    }


    void DoSetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert,
        TNullable<TDuration> timeout)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(operationId);
        if (!operation) {
            return;
        }

        operation->SetAlert(alertType, alert, timeout);
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


    void OnMasterConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        // NB: Must start the keeper before registering operations.
        const auto& responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Start();

        OperationsCleaner_->Start();
    }

    void OnMasterHandshake(const TMasterHandshakeResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ValidateConfig();

        {
            LOG_INFO("Connecting node shards");

            std::vector<TFuture<IInvokerPtr>> asyncInvokers;
            for (const auto& nodeShard : NodeShards_) {
                asyncInvokers.push_back(BIND(&TNodeShard::OnMasterConnected, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run());
            }

            auto invokerOrError = WaitFor(Combine(asyncInvokers));
            if (!invokerOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Error connecting node shards")
                    << invokerOrError;
            }

            const auto& invokers = invokerOrError.Value();
            for (size_t index = 0; index < NodeShards_.size(); ++index) {
                CancelableNodeShardInvokers_[index ] = invokers[index];
            }
        }

        {
            LOG_INFO("Registering existing operations");

            for (const auto& operation : result.Operations) {
                if (operation->GetMutationId()) {
                    TRspStartOperation response;
                    ToProto(response.mutable_operation_id(), operation->GetId());
                    auto responseMessage = CreateResponseMessage(response);
                    auto responseKeeper = Bootstrap_->GetResponseKeeper();
                    responseKeeper->EndRequest(operation->GetMutationId(), responseMessage);
                }

                RegisterOperation(operation, false);

                operation->SetStateAndEnqueueEvent(EOperationState::Orphaned);
                AddOperationToTransientQueue(operation);
            }
        }
    }

    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TransientOperationQueueScanPeriodExecutor_ = New<TPeriodicExecutor>(
            MasterConnector_->GetCancelableControlInvoker(EControlQueue::PeriodicActivity),
            BIND(&TImpl::ScanTransientOperationQueue, MakeWeak(this)),
            Config_->TransientOperationQueueScanPeriod);
        TransientOperationQueueScanPeriodExecutor_->Start();

        Strategy_->OnMasterConnected();

        LogEventFluently(ELogEventType::MasterConnected)
            .Item("address").Value(ServiceAddress_);
    }

    void DoCleanup()
    {
        NodeIdToTags_.clear();

        {
            auto error = TError("Master disconnected");
            for (const auto& pair : IdToOperation_) {
                const auto& operation = pair.second;
                if (!operation->IsFinishedState()) {
                    // This awakes those waiting for start promise.
                    SetOperationFinalState(
                        operation,
                        EOperationState::Aborted,
                        error);
                }
                operation->Cancel();
            }
            IdToOperation_.clear();
        }

        for (auto& queue : StateToTransientOperations_) {
            queue.clear();
        }

        const auto& responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Stop();

        if (TransientOperationQueueScanPeriodExecutor_) {
            TransientOperationQueueScanPeriodExecutor_->Stop();
            TransientOperationQueueScanPeriodExecutor_.Reset();
        }

        Strategy_->OnMasterDisconnected();
        OperationsCleaner_->Stop();
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LogEventFluently(ELogEventType::MasterDisconnected)
            .Item("address").Value(ServiceAddress_);

        if (Config_->TestingOptions->MasterDisconnectDelay) {
            Sleep(*Config_->TestingOptions->MasterDisconnectDelay);
        }

        DoCleanup();

        {
            LOG_INFO("Started disconnecting node shards");

            std::vector<TFuture<void>> asyncResults;
            for (const auto& nodeShard : NodeShards_) {
                asyncResults.push_back(BIND(&TNodeShard::OnMasterDisconnected, nodeShard)
                    .AsyncVia(nodeShard->GetInvoker())
                    .Run());
            }

            // XXX(babenko): fiber switch is forbidden here; do we actually need to wait for these results?
            Combine(asyncResults)
                .Get();

            LOG_INFO("Finished disconnecting node shards");
        }
    }


    void LogOperationFinished(const TOperationPtr& operation, ELogEventType logEventType, const TError& error)
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


    void RequestPoolTrees(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        static const TPoolTreeKeysHolder PoolTreeKeysHolder;

        LOG_INFO("Requesting pool trees");

        auto req = TYPathProxy::Get(GetPoolTreesPath());
        ToProto(req->mutable_attributes()->mutable_keys(), PoolTreeKeysHolder.Keys);
        batchReq->AddRequest(req, "get_pool_trees");
    }

    void HandlePoolTrees(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pool_trees");
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error getting pool trees");
            return;
        }

        const auto& rsp = rspOrError.Value();
        INodePtr poolTreesNode;
        try {
            poolTreesNode = ConvertToNode(TYsonString(rsp->value()));
        } catch (const std::exception& ex) {
            auto error = TError("Error parsing pool trees")
                << ex;
            SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            return;
        }

        Strategy_->UpdatePoolTrees(poolTreesNode);
    }


    void RequestNodesAttributes(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Requesting exec nodes information");

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
            LOG_WARNING(rspOrError, "Error getting exec nodes information");
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

            LOG_INFO("Exec nodes information updated");
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Error updating exec nodes information");
        }
    }

    // COMPAT(asaitgalin): Runtime params updates from Cypress will be replaced
    // with separate command and removed.
    void RequestOperationRuntimeParams(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TReqExecuteBatchPtr& batchReq)
    {
        static auto treeParamsTemplate = New<TOperationFairShareStrategyTreeOptions>();

        auto keySet = treeParamsTemplate->GetRegisteredKeys();
        std::vector<TString> keys(keySet.begin(), keySet.end());
        keys.push_back("owners");

        auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), keys);
        batchReq->AddRequest(req, "get_runtime_params");
    }

    void HandleOperationRuntimeParams(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_runtime_params");
        // COMPAT(babenko): Nirvana operations have no runtime params
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return;
        }
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error getting operation runtime parameters (OperationId: %v)",
                operation->GetId());
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto runtimeParamsNode = ConvertToNode(TYsonString(rsp->value()));

        try {
            auto runtimeParamsMap = runtimeParamsNode->AsMap();

            std::vector<TString> ownerList;
            auto owners = runtimeParamsMap->FindChild("owners");
            if (owners) {
                ownerList = ConvertTo<std::vector<TString>>(owners->AsList());
            }

            Strategy_->UpdateOperationRuntimeParameters(operation.Get(), runtimeParamsNode);

            if (operation->GetOwners() != ownerList) {
                operation->SetOwners(ownerList);
            }

            LOG_DEBUG("Operation runtime parameters updated from Cypress (OperationId: %v)",
                operation->GetId());
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Error parsing operation runtime parameters (OperationId: %v)",
                operation->GetId());
        }
    }


    void RequestConfig(const TObjectServiceProxy::TReqExecuteBatchPtr& batchReq)
    {
        LOG_INFO("Requesting scheduler configuration");

        auto req = TYPathProxy::Get("//sys/scheduler/config");
        batchReq->AddRequest(req, "get_config");
    }

    void HandleConfig(const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_config");
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No config in Cypress, just ignore.
            SetSchedulerAlert(ESchedulerAlertType::UpdateConfig, TError());
            return;
        }
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error getting scheduler configuration");
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
                nodeShard->GetInvoker()->Invoke(
                    BIND(&TNodeShard::UpdateConfig, nodeShard, Config_));
            }

            Strategy_->UpdateConfig(Config_);
            MasterConnector_->UpdateConfig(Config_);
            OperationsCleaner_->UpdateConfig(Config_->OperationsCleaner);

            ProfilingExecutor_->SetPeriod(Config_->ProfilingUpdatePeriod);
            LoggingExecutor_->SetPeriod(Config_->ClusterInfoLoggingPeriod);
            UpdateExecNodeDescriptorsExecutor_->SetPeriod(Config_->ExecNodeDescriptorsUpdatePeriod);
            if (TransientOperationQueueScanPeriodExecutor_) {
                TransientOperationQueueScanPeriodExecutor_->SetPeriod(Config_->TransientOperationQueueScanPeriod);
            }

            EventLogWriter_->UpdateConfig(Config_->EventLog);
        }
    }


    void RequestOperationArchiveVersion(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Requesting operation archive version");

        auto req = TYPathProxy::Get(GetOperationsArchiveVersionPath());
        batchReq->AddRequest(req, "get_operation_archive_version");
    }

    void HandleOperationArchiveVersion(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_operation_archive_version");
        if (!rspOrError.IsOK()) {
            LOG_INFO(rspOrError, "Error getting operation archive version");
            return;
        }

        try {
            auto version = ConvertTo<int>(TYsonString(rspOrError.Value()->value()));
            OperationArchiveVersion_.store(version, std::memory_order_relaxed);
            OperationsCleaner_->SetArchiveVersion(version);
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

        std::vector<TFuture<TRefCountedExecNodeDescriptorMapPtr>> shardDescriptorsFutures;
        for (const auto& nodeShard : NodeShards_) {
            shardDescriptorsFutures.push_back(BIND(&TNodeShard::GetExecNodeDescriptors, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run());
        }

        auto shardDescriptors = WaitFor(Combine(shardDescriptorsFutures))
            .ValueOrThrow();

        auto result = New<TRefCountedExecNodeDescriptorMap>();
        for (const auto& descriptors : shardDescriptors) {
            for (const auto& pair : *descriptors) {
                YCHECK(result->insert(pair).second);
            }
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

    virtual TRefCountedExecNodeDescriptorMapPtr CalculateExecNodeDescriptors(const TSchedulingTagFilter& filter) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TRefCountedExecNodeDescriptorMapPtr descriptors;
        {
            TReaderGuard guard(ExecNodeDescriptorsLock_);
            descriptors = CachedExecNodeDescriptors_;
        }

        if (filter.IsEmpty()) {
            return descriptors;
        }

        auto result = New<TRefCountedExecNodeDescriptorMap>();
        for (const auto& pair : *descriptors) {
            const auto& descriptor = pair.second;
            if (filter.CanSchedule(descriptor.Tags)) {
                YCHECK(result->emplace(descriptor.Id, descriptor).second);
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

            for (const auto& pair : *CachedExecNodeDescriptors_) {
                const auto& descriptor = pair.second;
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

        ValidateOperationState(operation, EOperationState::Starting);

        try {
            // NB(babenko): now we only validate this on start but not during revival
            Strategy_->ValidateOperationCanBeRegistered(operation.Get());

            WaitFor(MasterConnector_->CreateOperationNode(operation))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to start")
                << ex;
            operation->SetStarted(wrappedError);
            return;
        }

        ValidateOperationState(operation, EOperationState::Starting);

        RegisterOperation(operation, true);

        operation->SetStateAndEnqueueEvent(EOperationState::WaitingForAgent);
        AddOperationToTransientQueue(operation);

        // NB: Once we've registered the operation in Cypress we're free to complete
        // StartOperation request. Preparation will happen in a non-blocking
        // fashion.
        operation->SetStarted(TError());
    }

    NYson::TYsonString BuildBriefSpec(const TOperationPtr& operation) const
    {
        auto briefSpec = BuildYsonStringFluently()
            .BeginMap()
                .Items(operation->ControllerAttributes().InitializationAttributes->BriefSpec)
                .Do(BIND(&ISchedulerStrategy::BuildBriefSpec, Strategy_, operation->GetId()))
            .EndMap();
        return briefSpec;
    }

    void DoInitializeOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        auto operationId = operation->GetId();

        ValidateOperationState(operation, EOperationState::Initializing);

        try {
            RegisterAssignedOperation(operation);

            const auto& controller = operation->GetController();

            auto initializationResult = WaitFor(controller->Initialize(Null))
                .ValueOrThrow();

            ValidateOperationState(operation, EOperationState::Initializing);

            operation->ControllerAttributes().InitializationAttributes = initializationResult.Attributes;
            operation->BriefSpec() = BuildBriefSpec(operation);

            WaitFor(MasterConnector_->UpdateInitializedOperationNode(operation))
                .ThrowOnError();

            ValidateOperationState(operation, EOperationState::Initializing);
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to initialize")
                << ex;
            OnOperationFailed(operation, wrappedError);
            return;
        }

        ValidateOperationState(operation, EOperationState::Initializing);

        LogEventFluently(ELogEventType::OperationStarted)
            .Do(std::bind(&TImpl::BuildOperationInfoForEventLog, MakeStrong(this), operation, _1))
            .Do(std::bind(&ISchedulerStrategy::BuildOperationInfoForEventLog, Strategy_, operation.Get(), _1));

        LOG_INFO("Preparing operation (OperationId: %v)",
            operationId);

        operation->SetStateAndEnqueueEvent(EOperationState::Preparing);

        try {
            // Run async preparation.
            const auto& controller = operation->GetController();

            {
                TWallTimer timer;
                auto result = WaitFor(controller->Prepare())
                    .ValueOrThrow();

                operation->UpdateControllerTimeStatistics("/prepare", timer.GetElapsedTime());
                operation->ControllerAttributes().PrepareAttributes = std::move(result.Attributes);
            }

            ValidateOperationState(operation, EOperationState::Preparing);

            operation->SetStateAndEnqueueEvent(EOperationState::Pending);
            operation->SetPrepared(true);
            if (operation->GetActivated()) {
                MaterializeOperation(operation);
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to prepare")
                << ex;
            OnOperationFailed(operation, wrappedError);
            return;
        }

        LOG_INFO("Operation prepared (OperationId: %v)",
            operationId);

        LogEventFluently(ELogEventType::OperationPrepared)
            .Item("operation_id").Value(operationId)
            .Item("unrecognized_spec").Value(operation->ControllerAttributes().InitializationAttributes->UnrecognizedSpec);
    }

    void DoReviveOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        auto operationId = operation->GetId();

        ValidateOperationState(operation, EOperationState::Reviving);

        LOG_INFO("Reviving operation (OperationId: %v)",
            operationId);

        try {
            RegisterAssignedOperation(operation);

            const auto& controller = operation->GetController();

            {
                YCHECK(operation->RevivalDescriptor());
                auto result = WaitFor(controller->Initialize(operation->RevivalDescriptor()))
                    .ValueOrThrow();

                operation->ControllerAttributes().InitializationAttributes = std::move(result.Attributes);
                operation->BriefSpec() = BuildBriefSpec(operation);
            }

            ValidateOperationState(operation, EOperationState::Reviving);

            WaitFor(MasterConnector_->UpdateInitializedOperationNode(operation))
                .ThrowOnError();

            ValidateOperationState(operation, EOperationState::Reviving);

            {
                auto result = WaitFor(controller->Revive())
                    .ValueOrThrow();

                operation->ControllerAttributes().PrepareAttributes = result.Attributes;
                operation->SetRevivedFromSnapshot(result.RevivedFromSnapshot);
                operation->RevivedJobs() = std::move(result.RevivedJobs);
            }

            ValidateOperationState(operation, EOperationState::Reviving);

            LOG_INFO("Operation has been revived (OperationId: %v)",
                operationId);

            operation->RevivalDescriptor().Reset();
            operation->SetStateAndEnqueueEvent(EOperationState::Pending);
            operation->SetPrepared(true);

            if (operation->GetActivated()) {
                MaterializeOperation(operation);
            }
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);
            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            OnOperationFailed(operation, wrappedError);
        }
    }

    TFuture<void> ResetOperationRevival(const TOperationPtr& operation)
    {
        std::vector<TFuture<void>> asyncResults;
        for (int shardId = 0; shardId < NodeShards_.size(); ++shardId) {
            auto asyncResult = BIND(&TNodeShard::ResetOperationRevival, NodeShards_[shardId])
                .AsyncVia(NodeShards_[shardId]->GetInvoker())
                .Run(operation->GetId());
            asyncResults.emplace_back(std::move(asyncResult));
        }
        return Combine(asyncResults);
    }

    TFuture<void> RegisterJobsFromRevivedOperation(const TOperationPtr& operation)
    {
        auto jobs = std::move(operation->RevivedJobs());
        LOG_INFO("Registering running jobs from the revived operation (OperationId: %v, JobCount: %v)",
            operation->GetId(),
            jobs.size());

        // First, unfreeze operation and register jobs in strategy. Do this synchronously as we are in the scheduler control thread.
        Strategy_->RegisterJobs(operation->GetId(), jobs);

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
            auto asyncResult = BIND(&TNodeShard::FinishOperationRevival, NodeShards_[shardId])
                .AsyncVia(NodeShards_[shardId]->GetInvoker())
                .Run(operation->GetId(), std::move(jobsByShardId[shardId]));
            asyncResults.emplace_back(std::move(asyncResult));
        }
        return Combine(asyncResults);
    }

    void RegisterOperation(const TOperationPtr& operation, bool jobsReady)
    {
        YCHECK(IdToOperation_.emplace(operation->GetId(), operation).second);

        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        auto controller = agentTracker->CreateController(operation);
        operation->SetController(controller);

        Strategy_->RegisterOperation(operation.Get());
        operation->PoolTreeSchedulingTagFilters() = Strategy_->GetOperationPoolTreeSchedulingTagFilters(operation->GetId());

        for (const auto& nodeShard : NodeShards_) {
            nodeShard->GetInvoker()->Invoke(BIND(
                &TNodeShard::RegisterOperation,
                nodeShard,
                operation->GetId(),
                operation->GetController(),
                jobsReady));
        }

        MasterConnector_->RegisterOperation(operation);
        MasterConnector_->AddOperationWatcherRequester(
            operation,
            BIND(&TImpl::RequestOperationRuntimeParams, Unretained(this), operation));
        MasterConnector_->AddOperationWatcherHandler(
            operation,
            BIND(&TImpl::HandleOperationRuntimeParams, Unretained(this), operation));

        LOG_DEBUG("Operation registered (OperationId: %v, JobsReady: %v)",
            operation->GetId(),
            jobsReady);
    }

    void RegisterAssignedOperation(const TOperationPtr& operation)
    {
        auto agent = operation->GetAgentOrCancelFiber();
        const auto& controller = operation->GetController();
        controller->AssignAgent(agent);

        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        WaitFor(agentTracker->RegisterOperationAtAgent(operation))
            .ThrowOnError();
    }

    void UnregisterOperation(const TOperationPtr& operation)
    {
        YCHECK(IdToOperation_.erase(operation->GetId()) == 1);

        const auto& controller = operation->GetController();
        if (controller) {
            controller->RevokeAgent();
        }

        for (const auto& nodeShard : NodeShards_) {
            nodeShard->GetInvoker()->Invoke(BIND(
                &TNodeShard::UnregisterOperation,
                nodeShard,
                operation->GetId()));
        }

        Strategy_->UnregisterOperation(operation.Get());

        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        agentTracker->UnregisterOperationFromAgent(operation);

        MasterConnector_->UnregisterOperation(operation);

        LOG_DEBUG("Operation unregistered (OperationId: %v)",
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

        LOG_DEBUG("Requested node shards to abort all operation jobs (OperationId: %v)",
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
        operation->SetStateAndEnqueueEvent(state);
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
        operation->Cancel();
    }

    void DoCompleteOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishedState() || operation->IsFinishingState()) {
            // Operation is probably being aborted.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        const auto& operationId = operation->GetId();
        LOG_INFO("Completing operation (OperationId: %v)",
            operationId);

        operation->SetStateAndEnqueueEvent(EOperationState::Completing);
        operation->SetSuspended(false);

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

            // Should be called before commit in controller.
            auto operationInfoOrError = WaitFor(RequestOperationInfoFromControllerAgent(operation));
            if (!operationInfoOrError.IsOK()) {
                LOG_INFO(operationInfoOrError, "Failed to get operation info from controller agent "
                    "during operation completion (OperationId: %v)",
                    operation->GetId());
            }

            {
                const auto& controller = operation->GetController();
                WaitFor(controller->Commit())
                    .ThrowOnError();

                ValidateOperationState(operation, EOperationState::Completing);

                if (Config_->TestingOptions->FinishOperationTransitionDelay) {
                    Sleep(*Config_->TestingOptions->FinishOperationTransitionDelay);
                }
            }

            YCHECK(operation->GetState() == EOperationState::Completing);
            SetOperationFinalState(operation, EOperationState::Completed, TError());

            SubmitOperationToCleaner(
                operation,
                operationInfoOrError.IsOK() ? operationInfoOrError.Value() : nullptr);

            // Second flush: ensure that state is changed to Completed.
            {
                auto asyncResult = MasterConnector_->FlushOperationNode(operation);
                WaitFor(asyncResult)
                    .ThrowOnError();
                YCHECK(operation->GetState() == EOperationState::Completed);
            }

            // Notify controller that it is going to be disposed.
            const auto& controller = operation->GetController();
            Y_UNUSED(WaitFor(controller->Dispose()));

            FinishOperation(operation);
        } catch (const std::exception& ex) {
            OnOperationFailed(operation, ex);
            return;
        }

        LOG_INFO("Operation completed (OperationId: %v)",
             operationId);

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }

    void DoFailOperation(
        const TOperationPtr& operation,
        const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: finishing state is ok, do not skip operation fail in this case.
        if (operation->IsFinishedState()) {
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

        // NB: finishing state is ok, do not skip operation abort in this case.
        if (operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO(error, "Aborting operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        TerminateOperation(
            operation,
            EOperationState::Aborting,
            EOperationState::Aborted,
            ELogEventType::OperationAborted,
            error);
    }

    void DoSuspendOperation(
        const TOperationPtr& operation,
        const TError& error,
        bool abortRunningJobs,
        bool setAlert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: finishing state is ok, do not skip operation fail in this case.
        if (operation->IsFinishedState()) {
            // Operation is already terminated.
            return;
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        operation->SetSuspended(true);

        if (abortRunningJobs) {
            AbortOperationJobs(operation, error, /* terminated */ false);
        }

        if (setAlert) {
            operation->SetAlert(EOperationAlertType::OperationSuspended, error);
        }

        LOG_INFO(error, "Operation suspended (OperationId: %v)",
            operation->GetId());
    }

    TFuture<TControllerAgentServiceProxy::TRspGetOperationInfoPtr> RequestOperationInfoFromControllerAgent(
        const TOperationPtr& operation) const
    {
        auto agent = operation->FindAgent();
        YCHECK(agent);

        NControllerAgent::TControllerAgentServiceProxy proxy(agent->GetChannel());
        auto req = proxy.GetOperationInfo();
        req->SetTimeout(Config_->ControllerAgentLightRpcTimeout);
        ToProto(req->mutable_operation_id(), operation->GetId());

        return req->Invoke();
    }

    void SubmitOperationToCleaner(
        const TOperationPtr& operation,
        TControllerAgentServiceProxy::TRspGetOperationInfoPtr operationInfo) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TArchiveOperationRequest archivationReq;
        archivationReq.InitializeFromOperation(operation);
        if (operationInfo) {
            // TODO(asaitgalin): Can we build map in controller instead of
            // map fragment?
            archivationReq.Progress = BuildYsonStringFluently()
                .BeginMap()
                    .Items(TYsonString(operationInfo->progress(), EYsonType::MapFragment))
                .EndMap();

            archivationReq.BriefProgress = BuildYsonStringFluently()
                .BeginMap()
                    .Items(TYsonString(operationInfo->brief_progress(), EYsonType::MapFragment))
                .EndMap();
        }

        OperationsCleaner_->SubmitForArchivation(std::move(archivationReq));
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

        operation->SetStateAndEnqueueEvent(intermediateState);
        operation->SetSuspended(false);

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
        }

        TControllerAgentServiceProxy::TRspGetOperationInfoPtr operationInfo;
        if (operation->FindAgent()) {
            auto operationInfoOrError = WaitFor(RequestOperationInfoFromControllerAgent(operation));
            if (operationInfoOrError.IsOK()) {
                operationInfo = operationInfoOrError.Value();
            } else {
                LOG_INFO(operationInfoOrError, "Failed to get operation info from controller agent "
                    "during operation termination (OperationId: %v)",
                    operation->GetId());
            }
        }

        const auto& controller = operation->GetController();
        if (controller) {
            try {
                WaitFor(controller->Abort())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                auto error = TError("Failed to abort controller")
                    << TErrorAttribute("operation_id", operation->GetId())
                    << ex;
                MasterConnector_->Disconnect(error);
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

        SubmitOperationToCleaner(operation, operationInfo);

        if (controller) {
            // Notify controller that it is going to be disposed.
            const auto& controller = operation->GetController();
            Y_UNUSED(WaitFor(controller->Dispose()));
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
            WaitFor(revivalDescriptor.OutputTransaction->Commit())
                .ThrowOnError();
        }

        SetOperationFinalState(operation, EOperationState::Completed, TError());

        // Result is ignored since failure causes scheduler disconnection.
        Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());

        FinishOperation(operation);
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

        const auto& revivalDescriptor = *operation->RevivalDescriptor();
        abortTransaction(revivalDescriptor.AsyncTransaction);
        abortTransaction(revivalDescriptor.InputTransaction);
        abortTransaction(revivalDescriptor.OutputTransaction);

        SetOperationFinalState(operation, EOperationState::Aborted, error);

        // Result is ignored since failure causes scheduler disconnection.
        Y_UNUSED(WaitFor(MasterConnector_->FlushOperationNode(operation)));

        LogOperationFinished(operation, ELogEventType::OperationAborted, error);

        FinishOperation(operation);
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

    TYsonString BuildSuspiciousJobsYson()
    {
        TStringBuilder builder;
        for (const auto& pair : IdToOperation_) {
            const auto& operation = pair.second;
            builder.AppendString(operation->GetSuspiciousJobs().GetData());
        }
        return TYsonString(builder.Flush(), EYsonType::MapFragment);
    }

    void BuildStaticOrchid(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        RemoveExpiredResourceLimitsTags();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("connected").Value(IsConnected())
                // COMPAT(babenko): deprecate cell in favor of cluster
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
                .Item("cluster").BeginMap()
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
                .Item("controller_agents").DoMapFor(Bootstrap_->GetControllerAgentTracker()->GetAgents(), [] (TFluentMap fluent, const auto& agent) {
                    fluent
                        .Item(agent->GetId()).BeginMap()
                            .Item("state").Value(agent->GetState())
                            .DoIf(agent->GetState() == EControllerAgentState::Registered, [&] (TFluentMap fluent) {
                                fluent.Item("incarnation_id").Value(agent->GetIncarnationId());
                            })
                            .Item("operation_ids").DoListFor(agent->Operations(), [] (TFluentList fluent, const auto& operation) {
                                fluent.Item().Value(operation->GetId());
                            })
                        .EndMap();
                })
                .Item("suspicious_jobs").BeginMap()
                    .Items(BuildSuspiciousJobsYson())
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
                .Item("operations_cleaner").BeginMap()
                    .Do(std::bind(&TOperationsCleaner::BuildOrchid, OperationsCleaner_, _1))
                .EndMap()
            .EndMap();
    }

    TYsonString TryBuildOperationYson(const TOperationId& operationId) const
    {
        static const auto emptyMapFragment = TYsonString(TString(), EYsonType::MapFragment);

        auto replyNoOperation = [&] {
            return TYsonString();
        };

        // First fast check.
        auto operation = FindOperation(operationId);
        if (!operation) {
            return replyNoOperation();
        }

        auto codicilGuard = operation->MakeCodicilGuard();

        auto replyNoAgent = [&] {
            return BuildYsonStringFluently()
                .BeginMap()
                    .Do(BIND(&NScheduler::BuildFullOperationAttributes, operation))
                .EndMap();
        };

        auto agent = operation->FindAgent();
        if (!agent) {
            return replyNoAgent();
        }

        auto rspOrError = WaitFor(RequestOperationInfoFromControllerAgent(operation));
        auto rsp = rspOrError.IsOK() ? rspOrError.Value() : nullptr;
        if (!rsp) {
            LOG_DEBUG(rspOrError, "Failed to get operation info from controller; assuming empty response (OperationId: %v)",
                operationId);
        }

        // Recheck to make sure operation is still alive.
        if (!FindOperation(operationId)) {
            return replyNoOperation();
        }

        auto toYsonString = [] (const TProtoStringType& protoString) {
            return protoString.empty() ? emptyMapFragment : TYsonString(protoString, EYsonType::MapFragment);
        };

        auto controllerProgress = rsp ? toYsonString(rsp->progress()) : emptyMapFragment;
        auto controllerBriefProgress = rsp ? toYsonString(rsp->brief_progress()) : emptyMapFragment;
        auto controllerRunningJobs = rsp ? toYsonString(rsp->running_jobs()) : emptyMapFragment;
        auto controllerJobSplitterInfo = rsp ? toYsonString(rsp->job_splitter()) : emptyMapFragment;
        auto controllerMemoryUsage = rsp ? MakeNullable(rsp->controller_memory_usage()) : Null;
        auto controllerState = rsp ? MakeNullable(static_cast<NControllerAgent::EControllerState>(rsp->controller_state())  ) : Null;

        return BuildYsonStringFluently()
            .BeginMap()
                .Do(BIND(&NScheduler::BuildFullOperationAttributes, operation))
                .Item("agent_id").Value(agent->GetId())
                .Item("progress").BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        Strategy_->BuildOperationProgress(operation->GetId(), fluent);
                    })
                    .Items(controllerProgress)
                .EndMap()
                .Item("brief_progress").BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        Strategy_->BuildBriefOperationProgress(operation->GetId(), fluent);
                    })
                    .Items(controllerBriefProgress)
                .EndMap()
                .Item("running_jobs")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .BeginMap()
                        .Items(controllerRunningJobs)
                    .EndMap()
                .Item("job_splitter")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .BeginMap()
                        .Items(controllerJobSplitterInfo)
                    .EndMap()
                .Item("controller_memory_usage").Value(controllerMemoryUsage)
                .Item("controller_state").Value(controllerState)
                .DoIf(!rspOrError.IsOK(), [&] (TFluentMap fluent) {
                    fluent.Item("controller_error").Value(TError(rspOrError));
                })
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


    void AddOperationToTransientQueue(const TOperationPtr& operation)
    {
        StateToTransientOperations_[operation->GetState()].push_back(operation);

        if (TransientOperationQueueScanPeriodExecutor_) {
            TransientOperationQueueScanPeriodExecutor_->ScheduleOutOfBand();
        }

        LOG_DEBUG("Operation added to transient queue (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());
    }

    bool HandleWaitingForAgentOperation(const TOperationPtr& operation)
    {
        const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
        auto agent = agentTracker->PickAgentForOperation(operation, Config_->MinAgentCountForWaitingOperation);
        if (!agent) {
            LOG_DEBUG("Failed to assign operation to agent; backing off");
            OperationToAgentAssignmentFailureTime_ = TInstant::Now();
            return false;
        }

        agentTracker->AssignOperationToAgent(operation, agent);

        if (operation->RevivalDescriptor()) {
            operation->SetStateAndEnqueueEvent(EOperationState::Reviving);
            operation->GetCancelableControlInvoker()->Invoke(
                BIND(&TImpl::DoReviveOperation, MakeStrong(this), operation));
        } else {
            operation->SetStateAndEnqueueEvent(EOperationState::Initializing);
            operation->GetCancelableControlInvoker()->Invoke(
                BIND(&TImpl::DoInitializeOperation, MakeStrong(this), operation));
        }

        return true;
    }

    void HandleOrphanedOperation(const TOperationPtr& operation)
    {
        const auto& operationId = operation->GetId();

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_DEBUG("Handling orphaned operation (OperationId: %v)",
            operation->GetId());

        try {
            ValidateOperationState(operation, EOperationState::Orphaned);

            YCHECK(operation->RevivalDescriptor());
            const auto& revivalDescriptor = *operation->RevivalDescriptor();

            if (revivalDescriptor.OperationCommitted) {
                CompleteOperationWithoutRevival(operation);
                return;
            }

            if (revivalDescriptor.OperationAborting) {
                AbortOperationWithoutRevival(
                    operation,
                    TError("Operation aborted since it was found in \"aborting\" state during scheduler revival"));
                return;
            }

            if (revivalDescriptor.UserTransactionAborted) {
                AbortOperationWithoutRevival(
                    operation,
                    GetUserTransactionAbortedError(operation->GetUserTransactionId()));
                return;
            }

            WaitFor(Strategy_->ValidateOperationStart(operation.Get()))
                .ThrowOnError();

            operation->SetStateAndEnqueueEvent(EOperationState::WaitingForAgent);
            AddOperationToTransientQueue(operation);
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);
            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            OnOperationFailed(operation, wrappedError);
        }
    }

    void HandleOrphanedOperations()
    {
        auto& queuedOperations = StateToTransientOperations_[EOperationState::Orphaned];
        std::vector<TOperationPtr> operations;
        operations.reserve(queuedOperations.size());
        for (const auto& operation : queuedOperations) {
            if (operation->GetState() != EOperationState::Orphaned) {
                LOG_DEBUG("Operation is no longer orphaned (OperationId: %v, State: %v)",
                    operation->GetId(),
                    operation->GetState());
                continue;
            }
            operations.push_back(operation);
        }
        queuedOperations.clear();

        if (operations.empty()) {
            return;
        }

        auto result = WaitFor(MasterConnector_->FetchOperationRevivalDescriptors(operations));
        if (!result.IsOK()) {
            LOG_ERROR(result, "Error fetching revival descriptors");
            MasterConnector_->Disconnect(result);
            return;
        }

        for (const auto& operation : operations) {
            operation->GetCancelableControlInvoker()->Invoke(
                BIND(&TImpl::HandleOrphanedOperation, MakeStrong(this), operation));
        }
    }

    void ScanTransientOperationQueue()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Started scanning transient operation queue");

        if (TInstant::Now() > OperationToAgentAssignmentFailureTime_ + Config_->OperationToAgentAssignmentBackoff) {
            auto& queuedOperations = StateToTransientOperations_[EOperationState::WaitingForAgent];
            std::vector<TOperationPtr> newQueuedOperations;
            for (const auto& operation : queuedOperations) {
                if (operation->GetState() != EOperationState::WaitingForAgent) {
                    LOG_DEBUG("Operation is no longer waiting for agent (OperationId: %v, State: %v)",
                        operation->GetId(),
                        operation->GetState());
                    continue;
                }
                if (!HandleWaitingForAgentOperation(operation)) {
                    newQueuedOperations.push_back(operation);
                }
            }
            queuedOperations = std::move(newQueuedOperations);
        }

        HandleOrphanedOperations();

        LOG_DEBUG("Finished scanning transient operation queue");
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

        virtual IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            auto operationId = TOperationId::FromString(key);
            auto operationYson = Scheduler_->TryBuildOperationYson(operationId);
            if (!operationYson) {
                return nullptr;
            }
            return IYPathService::FromProducer(ConvertToProducer(std::move(operationYson)));
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

        virtual IYPathServicePtr FindItemService(TStringBuf key) const override
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

            auto getOperationIdCallback = BIND(&TNodeShard::FindOperationIdByJobId, nodeShard, jobId)
                .AsyncVia(nodeShard->GetInvoker())
                .Run();
            auto operationId = WaitFor(getOperationIdCallback)
                .ValueOrThrow();

            if (!operationId) {
                THROW_ERROR_EXCEPTION("Job %v is missing", jobId);
            }

            auto operation = Scheduler_->GetOperationOrThrow(operationId);
            auto agent = operation->GetAgentOrThrow();

            NControllerAgent::TControllerAgentServiceProxy proxy(agent->GetChannel());
            auto req = proxy.GetJobInfo();
            req->SetTimeout(Scheduler_->Config_->ControllerAgentLightRpcTimeout);
            ToProto(req->mutable_operation_id(), operationId);
            ToProto(req->mutable_job_id(), jobId);
            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();

            consumer->OnRaw(TYsonString(rsp->info()));
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

ISchedulerStrategyPtr TScheduler::GetStrategy() const
{
    return Impl_->GetStrategy();
}

const TOperationsCleanerPtr& TScheduler::GetOperationsCleaner() const
{
    return Impl_->GetOperationsCleaner();
}

IYPathServicePtr TScheduler::GetOrchidService() const
{
    return Impl_->GetOrchidService();
}

TRefCountedExecNodeDescriptorMapPtr TScheduler::GetCachedExecNodeDescriptors() const
{
    return Impl_->GetCachedExecNodeDescriptors();
}

const TSchedulerConfigPtr& TScheduler::GetConfig() const
{
    return Impl_->GetConfig();
}

int TScheduler::GetNodeShardId(TNodeId nodeId) const
{
    return Impl_->GetNodeShardId(nodeId);
}

const IInvokerPtr& TScheduler::GetCancelableNodeShardInvoker(int shardId) const
{
    return Impl_->GetCancelableNodeShardInvoker(shardId);
}

const std::vector<TNodeShardPtr>& TScheduler::GetNodeShards() const
{
    return Impl_->GetNodeShards();
}

bool TScheduler::IsConnected() const
{
    return Impl_->IsConnected();
}

void TScheduler::ValidateConnected()
{
    Impl_->ValidateConnected();
}

TMasterConnector* TScheduler::GetMasterConnector() const
{
    return Impl_->GetMasterConnector();
}

void TScheduler::Disconnect(const TError& error)
{
    Impl_->Disconnect(error);
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

void TScheduler::OnOperationCompleted(const TOperationPtr& operation)
{
    Impl_->OnOperationCompleted(operation);
}

void TScheduler::OnOperationAborted(const TOperationPtr& operation, const TError& error)
{
    Impl_->OnOperationAborted(operation, error);
}

void TScheduler::OnOperationFailed(const TOperationPtr& operation, const TError& error)
{
    Impl_->OnOperationFailed(operation, error);
}

void TScheduler::OnOperationSuspended(const TOperationPtr& operation, const TError& error)
{
    Impl_->OnOperationSuspended(operation, error);
}

void TScheduler::OnOperationAgentUnregistered(const TOperationPtr& operation)
{
    Impl_->OnOperationAgentUnregistered(operation);
}

void TScheduler::OnOperationBannedInTentativeTree(const TOperationPtr& operation, const TString& treeId)
{
    Impl_->OnOperationBannedInTentativeTree(operation, treeId);
}

TFuture<void> TScheduler::UpdateOperationParameters(
    TOperationPtr operation,
    const TString& user,
    INodePtr parameters)
{
    return Impl_->UpdateOperationParameters(operation, user, parameters);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
