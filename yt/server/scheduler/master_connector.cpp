#include "master_connector.h"

#include "config.h"
#include "private.h"
#include "helpers.h"
#include "scheduler.h"
#include "scheduler_strategy.h"

#include <yt/server/controller_agent/master_connector.h>
#include <yt/server/controller_agent/controller_agent.h>
#include <yt/server/controller_agent/operation_controller.h>

#include <yt/server/cell_scheduler/bootstrap.h>
#include <yt/server/cell_scheduler/config.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/update_executor.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/utilex/random.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NChunkClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NHiveClient;
using namespace NRpc;
using namespace NApi;
using namespace NSecurityClient;
using namespace NConcurrency;
using namespace NCellScheduler;
using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::GetDefaultAddress;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap)
        : Config(config)
        , Bootstrap(bootstrap)
        , OperationNodesUpdateExecutor_(New<TUpdateExecutor<TOperationId, TOperationNodeUpdate>>(
            BIND(&TImpl::UpdateOperationNode, Unretained(this)),
            BIND(&TImpl::IsOperationInFinishedState, Unretained(this)),
            BIND(&TImpl::OnOperationUpdateFailed, Unretained(this)),
            Logger))
    {
        Bootstrap
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetClusterDirectorySynchronizer()
            ->SubscribeSynchronized(BIND(&TImpl::OnClusterDirectorySynchronized, MakeWeak(this))
                .Via(Bootstrap->GetControlInvoker(EControlQueue::MasterConnector)));
    }

    void Start()
    {
        Bootstrap->GetControlInvoker()->Invoke(BIND(
            &TImpl::StartConnecting,
            MakeStrong(this)));
    }

    bool IsConnected() const
    {
        return Connected;
    }

    void Disconnect()
    {
        DoDisconnect();
    }

    void RandomDisconnect()
    {
        LOG_INFO("Disconnecting scheduler due to enabled random disconnection");
        DoDisconnect();
    }

    IInvokerPtr GetCancelableControlInvoker() const
    {
        return CancelableControlInvoker;
    }

    void BuildOperationAcl(const TOperationPtr& operation, TFluentAny fluent)
    {
        auto owners = operation->GetOwners();
        owners.push_back(operation->GetAuthenticatedUser());

        fluent
            .BeginList()
                .Item().BeginMap()
                    .Item("action").Value(ESecurityAction::Allow)
                    .Item("subjects").Value(owners)
                    .Item("permissions").BeginList()
                        .Item().Value(EPermission::Write)
                    .EndList()
                .EndMap()
            .EndList();
    }

    TFuture<void> CreateOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto operationId = operation->GetId();
        LOG_INFO("Creating operation node (OperationId: %v)",
            operationId);

        auto strategy = Bootstrap->GetScheduler()->GetStrategy();

        auto batchReq = StartObjectBatchRequest();

        auto operationYson = BuildYsonStringFluently()
            .BeginAttributes()
                .Do(BIND(&ISchedulerStrategy::BuildOperationAttributes, strategy, operationId))
                .Do(BIND(&BuildFullOperationAttributes, operation))
                .Item("brief_spec").BeginMap()
                    .Items(operation->ControllerAttributes().InitializationAttributes->BriefSpec)
                    .Do(BIND(&ISchedulerStrategy::BuildBriefSpec, strategy, operationId))
                .EndMap()
                .Item("progress").BeginMap().EndMap()
                .Item("brief_progress").BeginMap().EndMap()
                .Item("opaque").Value("true")
                    .Item("acl").Do(BIND(&TImpl::BuildOperationAcl, Unretained(this), operation))
                .Item("owners").Value(operation->GetOwners())
            .EndAttributes()
            .BeginMap()
                .Item("jobs").BeginAttributes()
                    .Item("opaque").Value("true")
                .EndAttributes()
                .BeginMap().EndMap()
            .EndMap()
            .GetData();

        auto paths = GetCompatibilityOperationPaths(operationId, operation->GetStorageMode());
        auto secureVaultPaths = GetCompatibilityOperationPaths(operationId, operation->GetStorageMode(), "secure_vault");

        for (const auto& path : paths) {
            auto req = TYPathProxy::Set(path);
            req->set_value(operationYson);
            req->set_recursive(true);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        if (operation->GetSecureVault()) {
            // Create secure vault.
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("inherit_acl", false);
            attributes->Set("value", operation->GetSecureVault());
            attributes->Set("acl", BuildYsonStringFluently()
                .Do(BIND(&TImpl::BuildOperationAcl, Unretained(this), operation)));

            for (const auto& path : secureVaultPaths) {
                auto req = TCypressYPathProxy::Create(path);
                req->set_type(static_cast<int>(EObjectType::Document));
                ToProto(req->mutable_node_attributes(), *attributes);
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
        }

        return batchReq->Invoke().Apply(
            BIND(&TImpl::OnOperationNodeCreated, MakeStrong(this), operation)
                .AsyncVia(GetCancelableControlInvoker()));
    }

    TFuture<void> ResetRevivingOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);
        YCHECK(operation->GetState() == EOperationState::Reviving);

        auto operationId = operation->GetId();
        LOG_INFO("Resetting reviving operation node (OperationId: %v)",
            operationId);

        auto batchReq = StartObjectBatchRequest();

        auto attributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Do(BIND(&BuildMutableOperationAttributes, operation))
                .Item("progress").BeginMap().EndMap()
                .Item("brief_progress").BeginMap().EndMap()
            .EndMap());

        auto paths = GetCompatibilityOperationPaths(operationId, operation->GetStorageMode());

        for (const auto& key : attributes->List()) {
            for (const auto& path : paths) {
                auto req = TYPathProxy::Set(path + "/@" + ToYPathLiteral(key));
                req->set_value(attributes->GetYson(key).GetData());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
        }

        return batchReq->Invoke().Apply(
            BIND(
                &TImpl::OnRevivingOperationNodeReset,
                MakeStrong(this),
                operation)
            .AsyncVia(GetCancelableControlInvoker()));
    }

    TFuture<void> FlushOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        return OperationNodesUpdateExecutor_->ExecuteUpdate(operation->GetId());
    }

    TFuture<void> UpdateOperationRuntimeParameters(
        TOperationPtr operation,
        const TOperationRuntimeParametersPtr& params)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TImpl::DoUpdateOperationRuntimeParameters, MakeStrong(this))
            .AsyncVia(GetCancelableControlInvoker())
            .Run(operation, params);
    }

    void DoUpdateOperationRuntimeParameters(
        TOperationPtr operation,
        const TOperationRuntimeParametersPtr& params)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto strategy = Bootstrap->GetScheduler()->GetStrategy();

        auto batchReq = StartObjectBatchRequest();
        auto paths = GetCompatibilityOperationPaths(operation->GetId(), operation->GetStorageMode());

        auto node = BuildYsonNodeFluently()
            .BeginMap()
                .Do(BIND(&ISchedulerStrategy::BuildOperationRuntimeParams, strategy, operation->GetId(), params))
            .EndMap();

        auto mapNode = node->AsMap();

        for (const auto& operationPath : paths) {
            for (const auto& pair : mapNode->GetChildren()) {
                const auto& key = pair.first;
                const auto& value = pair.second;

                auto req = TYPathProxy::Set(operationPath + "/@" + key);
                req->set_value(ConvertToYsonString(value).GetData());

                batchReq->AddRequest(req);
            }
        }

        auto rspOrError = WaitFor(batchReq->Invoke());
        auto error = GetCumulativeError(rspOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error updating operation %v runtime params", operation->GetId());
    }

    void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Alerts[alertType] = alert;
    }

    void AddGlobalWatcherRequester(TWatcherRequester requester)
    {
        GlobalWatcherRequesters.push_back(requester);
    }

    void AddGlobalWatcherHandler(TWatcherHandler handler)
    {
        GlobalWatcherHandlers.push_back(handler);
    }

    void AddGlobalWatcher(TWatcherRequester requester, TWatcherHandler handler, TDuration period)
    {
        CustomGlobalWatcherRecords.push_back(TPeriodicExecutorRecord{std::move(requester), std::move(handler), period});
    }

    void AddOperationWatcherRequester(TOperationPtr operation, TWatcherRequester requester)
    {
        auto* list = GetOrCreateWatcherList(operation);
        list->WatcherRequesters.push_back(requester);
    }

    void AddOperationWatcherHandler(TOperationPtr operation, TWatcherHandler handler)
    {
        auto* list = GetOrCreateWatcherList(operation);
        list->WatcherHandlers.push_back(handler);
    }

    void UpdateConfig(const TSchedulerConfigPtr& config)
    {
        Config = config;

        if (Connected) {
            OperationNodesUpdateExecutor_->SetPeriod(Config->OperationsUpdatePeriod);
        }
        if (WatchersExecutor) {
            WatchersExecutor->SetPeriod(Config->WatchersUpdatePeriod);
        }
        if (AlertsExecutor) {
            AlertsExecutor->SetPeriod(Config->AlertsUpdatePeriod);
        }

        ScheduleTestingDisconnection();
    }

    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* const Bootstrap;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;

    std::atomic<bool> Connected = {false};

    ITransactionPtr LockTransaction;

    TPeriodicExecutorPtr WatchersExecutor;
    TPeriodicExecutorPtr AlertsExecutor;

    struct TPeriodicExecutorRecord
    {
        TWatcherRequester Requester;
        TWatcherHandler Handler;
        TDuration Period;
    };

    std::vector<TWatcherRequester> GlobalWatcherRequesters;
    std::vector<TWatcherHandler>   GlobalWatcherHandlers;

    std::vector<TPeriodicExecutorRecord> CustomGlobalWatcherRecords;
    std::vector<TPeriodicExecutorPtr> CustomGlobalWatcherExecutors;

    TEnumIndexedVector<TError, ESchedulerAlertType> Alerts;

    const TCallback<TFuture<void>()> VoidCallback_ = BIND([] {return VoidFuture;});

    struct TOperationNodeUpdate
    {
        explicit TOperationNodeUpdate(const TOperationPtr& operation)
            : Operation(operation)
        { }

        TOperationPtr Operation;
    };

    TIntrusivePtr<TUpdateExecutor<TOperationId, TOperationNodeUpdate>> OperationNodesUpdateExecutor_;

    struct TWatcherList
    {
        explicit TWatcherList(TOperationPtr operation)
            : Operation(operation)
        { }

        TOperationPtr Operation;
        std::vector<TWatcherRequester> WatcherRequesters;
        std::vector<TWatcherHandler>   WatcherHandlers;
    };

    yhash<TOperationId, TWatcherList> WatcherLists;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void ScheduleTestingDisconnection()
    {
        if (Config->TestingOptions->EnableRandomMasterDisconnection) {
            TDelayedExecutor::Submit(
                BIND(&TImpl::RandomDisconnect, MakeStrong(this))
                    .Via(Bootstrap->GetControlInvoker()),
                RandomDuration(Config->TestingOptions->RandomMasterDisconnectionMaxBackoff));
        }
    }

    void StartConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Connecting to master");

        auto pipeline = New<TRegistrationPipeline>(this);
        BIND(&TRegistrationPipeline::Run, pipeline)
            .AsyncVia(Bootstrap->GetControlInvoker())
            .Run()
            .Subscribe(BIND(&TImpl::OnConnected, MakeStrong(this))
                .Via(Bootstrap->GetControlInvoker()));
    }

    void OnConnected(const TErrorOr<TMasterHandshakeResult>& resultOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!resultOrError.IsOK()) {
            LOG_ERROR(resultOrError, "Error connecting to master");
            TDelayedExecutor::Submit(
                BIND(&TImpl::StartConnecting, MakeStrong(this))
                    .Via(Bootstrap->GetControlInvoker()),
                Config->ConnectRetryBackoffTime);
            return;
        }

        LOG_INFO("Master connected");

        YCHECK(!Connected);
        Connected.store(true);

        CancelableContext = New<TCancelableContext>();
        CancelableControlInvoker = CancelableContext->CreateInvoker(
            Bootstrap->GetControlInvoker(EControlQueue::MasterConnector));

        const auto& result = resultOrError.Value();
        for (auto operationReport : result.OperationReports) {
            const auto& operation = operationReport.Operation;
            OperationNodesUpdateExecutor_->AddUpdate(
                operation->GetId(),
                TOperationNodeUpdate(operation));
        }

        LockTransaction->SubscribeAborted(
            BIND(&TImpl::OnLockTransactionAborted, MakeWeak(this))
                .Via(CancelableControlInvoker));

        Bootstrap->GetControllerAgent()->Connect();

        StartPeriodicActivities();

        try {
            MasterConnected_.Fire(result);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Master connection failed");
            Disconnect();
        }

        ScheduleTestingDisconnection();
    }

    void OnLockTransactionAborted()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_WARNING("Lock transaction aborted");

        Disconnect();
    }



    class TRegistrationPipeline
        : public TRefCounted
    {
    public:
        explicit TRegistrationPipeline(TIntrusivePtr<TImpl> owner)
            : Owner(owner)
            , ServiceAddresses(Owner->Bootstrap->GetLocalAddresses())
        { }

        TMasterHandshakeResult Run()
        {
            RegisterInstance();
            StartLockTransaction();
            TakeLock();
            AssumeControl();
            UpdateGlobalWatchers();
            SyncClusterDirectory();
            ListOperations();
            RequestOperationAttributes();
            RequestCommittedFlag();
            return Result;
        }

    private:
        const TIntrusivePtr<TImpl> Owner;

        const TAddressMap ServiceAddresses;

        struct TReviveOperationInfo
        {
            TOperationId Id;
            EOperationCypressStorageMode StorageMode;
        };

        std::vector<TReviveOperationInfo> RunningOperations;
        TMasterHandshakeResult Result;

        // - Register scheduler instance.
        void RegisterInstance()
        {
            TObjectServiceProxy proxy(Owner
                ->Bootstrap
                ->GetMasterClient()
                ->GetMasterChannelOrThrow(EMasterChannelKind::Leader));
            auto batchReq = proxy.ExecuteBatch();
            auto path = "//sys/scheduler/instances/" + ToYPathLiteral(GetDefaultAddress(ServiceAddresses));
            {
                auto req = TCypressYPathProxy::Create(path);
                req->set_ignore_existing(true);
                req->set_type(static_cast<int>(EObjectType::MapNode));
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TCypressYPathProxy::Create(path + "/orchid");
                req->set_ignore_existing(true);
                req->set_type(static_cast<int>(EObjectType::Orchid));
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("remote_addresses", ServiceAddresses);
                ToProto(req->mutable_node_attributes(), *attributes);
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }

        // - Start lock transaction.
        void StartLockTransaction()
        {
            TTransactionStartOptions options;
            options.AutoAbort = true;
            options.Timeout = Owner->Config->LockTransactionTimeout;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Scheduler lock at %v", GetDefaultAddress(ServiceAddresses)));
            options.Attributes = std::move(attributes);

            auto client = Owner->Bootstrap->GetMasterClient();
            auto transactionOrError = WaitFor(Owner->Bootstrap->GetMasterClient()->StartTransaction(
                ETransactionType::Master,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting lock transaction");

            Owner->LockTransaction = transactionOrError.Value();

            LOG_INFO("Lock transaction is %v", Owner->LockTransaction->GetId());
        }

        // - Take lock.
        void TakeLock()
        {
            auto result = WaitFor(Owner->LockTransaction->LockNode("//sys/scheduler/lock", ELockMode::Exclusive));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error taking scheduler lock");
        }

        // - Publish scheduler address.
        // - Update orchid address.
        void AssumeControl()
        {
            auto batchReq = Owner->StartObjectBatchRequest();
            auto addresses = Owner->Bootstrap->GetLocalAddresses();
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@addresses");
                req->set_value(ConvertToYsonString(addresses).GetData());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/orchid/@remote_addresses");
                req->set_value(ConvertToYsonString(addresses).GetData());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@connection_time");
                req->set_value(ConvertToYsonString(TInstant::Now()).GetData());
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }

        void SyncClusterDirectory()
        {
            WaitFor(Owner
                ->Bootstrap
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetClusterDirectorySynchronizer()
                ->Sync())
                .ThrowOnError();
        }

        // - Request operations and their states.
        void ListOperations()
        {
            static const std::vector<TString> attributeKeys = {"state"};

            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);
            {
                auto req = TYPathProxy::List("//sys/operations");
                ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                batchReq->AddRequest(req, "list_operations");
            }

            for (int hash = 0x0; hash <= 0xFF; ++hash) {
                auto hashStr = Format("%02x", hash);
                auto req = TYPathProxy::List("//sys/operations/" + hashStr);
                ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                batchReq->AddRequest(req, "list_operations_" + hashStr);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError);
            const auto& batchRsp = batchRspOrError.Value();

            std::vector<std::pair<EOperationState, TReviveOperationInfo>> operations;
            yhash<TOperationId, EOperationState> operationIdToState;

            auto listOperationsRsp = batchRsp->GetResponse<TYPathProxy::TRspList>("list_operations")
                .ValueOrThrow();

            auto operationsListNode = ConvertToNode(TYsonString(listOperationsRsp->value()));
            auto operationsList = operationsListNode->AsList();

            for (const auto& operationNode : operationsList->GetChildren()) {
                auto operationNodeKey = operationNode->GetValue<TString>();
                // NOTE: This is hash bucket, just skip it.
                if (operationNodeKey.length() == 2) {
                    continue;
                }

                auto id = TOperationId::FromString(operationNodeKey);
                operationIdToState[id] = operationNode->Attributes().Get<EOperationState>("state");
            }

            for (int hash = 0x0; hash <= 0xFF; ++hash) {
                auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>(
                    "list_operations_" + Format("%02x", hash));

                if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    continue;
                }

                auto hashBucketRsp = rspOrError.ValueOrThrow();
                auto hashBucketListNode = ConvertToNode(TYsonString(hashBucketRsp->value()));
                auto hashBucketList = hashBucketListNode->AsList();

                for (const auto& operationNode : hashBucketList->GetChildren()) {
                    auto id = TOperationId::FromString(operationNode->GetValue<TString>());
                    YCHECK((id.Parts32[0] & 0xff) == hash);

                    auto state = operationNode->Attributes().Find<EOperationState>("state");

                    auto stateIt = operationIdToState.find(id);

                    // Neither //sys/operations/<id> node nor //sys/operations/<hash>/<id> has
                    // "state" attribute.
                    if (!state && stateIt == operationIdToState.end()) {
                        LOG_WARNING("Operation %v does not have \"state\" attribute, skipping it", id);
                        continue;
                    }

                    if (!state) {
                        operations.emplace_back(
                            stateIt->second,
                            TReviveOperationInfo{id, EOperationCypressStorageMode::SimpleHashBuckets});
                        operationIdToState.erase(stateIt);
                        continue;
                    } else if (stateIt == operationIdToState.end()) {
                        operations.emplace_back(*state, TReviveOperationInfo{id, EOperationCypressStorageMode::HashBuckets});
                    } else {
                        if (stateIt->second != *state) {
                            LOG_WARNING("Operation has two operation nodes with different states "
                                "(OperationId: %v, State: %Qv, HashBucketState: %Qv)",
                                id,
                                stateIt->second,
                                *state);
                        }

                        operations.emplace_back(
                            stateIt->second,
                            TReviveOperationInfo{id, EOperationCypressStorageMode::Compatible});

                        operationIdToState.erase(stateIt);
                    }
                }
            }

            size_t runningOperationsWithUndefinedStorageSchema = 0;
            for (const auto& pair : operationIdToState) {
                if (IsOperationInProgress(pair.second)) {
                    ++runningOperationsWithUndefinedStorageSchema;
                }
            }

            YCHECK(runningOperationsWithUndefinedStorageSchema == 0 &&
                   "Operations node contains operations with undefined storage schema");

            for (const auto& operationInfo : operations) {
                if (IsOperationInProgress(operationInfo.first)) {
                    RunningOperations.push_back(operationInfo.second);
                }
            }

            LOG_INFO("Operations list received (RunningOperationCount: %v)", RunningOperations.size());
        }

        // - Request attributes for unfinished operations.
        // - Recreate operation instance from fetched data.
        void RequestOperationAttributes()
        {
            // Keep stuff below in sync with #TryCreateOperationFromAttributes.
            static const std::vector<TString> attributeKeys = {
                "operation_type",
                "mutation_id",
                "user_transaction_id",
                "async_scheduler_transaction_id",
                "input_transaction_id",
                "output_transaction_id",
                "completion_transaction_id",
                "debug_output_transaction_id",
                "spec",
                "owners",
                "authenticated_user",
                "start_time",
                "state",
                "suspended",
                "events",
                "slot_index_per_pool_tree",
                "scheduling_options_per_pool_tree"
            };

            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);
            {
                LOG_INFO("Fetching attributes and secure vaults for unfinished operations (UnfinishedOperationCount: %v)",
                    RunningOperations.size());

                for (const auto& operationInfo : RunningOperations) {
                    // Keep stuff below in sync with CreateOperationFromAttributes.
                    const auto& operationId = operationInfo.Id;

                    NYTree::TYPath operationAttributesPath;
                    NYTree::TYPath secureVaultPath;

                    if (operationInfo.StorageMode == EOperationCypressStorageMode::Compatible ||
                        operationInfo.StorageMode == EOperationCypressStorageMode::SimpleHashBuckets)
                    {
                        operationAttributesPath = GetOperationPath(operationId) + "/@";
                        secureVaultPath = GetSecureVaultPath(operationId);
                    } else {
                        operationAttributesPath = GetNewOperationPath(operationId) + "/@";
                        secureVaultPath = GetNewSecureVaultPath(operationId);
                    }

                    // Retrieve operation attributes.
                    {
                        auto req = TYPathProxy::Get(operationAttributesPath);
                        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                        batchReq->AddRequest(req, "get_op_attr_" + ToString(operationInfo.Id));
                    }
                    // Retrieve operation completion transaction id.
                    {
                        auto req = TYPathProxy::Get(GetNewOperationPath(operationId) + "/@");
                        std::vector<TString> attributeKeys{
                            "completion_transaction_id",
                        };
                        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                        batchReq->AddRequest(req, "get_op_completion_tx_id_" + ToString(operationInfo.Id));
                    }

                    // Retrieve secure vault.
                    {
                        auto req = TYPathProxy::Get(secureVaultPath);
                        batchReq->AddRequest(req, "get_op_secure_vault_" + ToString(operationInfo.Id));
                    }
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError);
            const auto& batchRsp = batchRspOrError.Value();

            {
                for (const auto& operationInfo : RunningOperations) {
                    auto attributesRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                        "get_op_attr_" + ToString(operationInfo.Id))
                        .ValueOrThrow();

                    auto completionTxIdRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                        "get_op_completion_tx_id_" + ToString(operationInfo.Id))
                        .ValueOrThrow();

                    auto secureVaultRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                        "get_op_secure_vault_" + ToString(operationInfo.Id));

                    auto attributesNode = ConvertToAttributes(TYsonString(attributesRsp->value()));
                    auto completionTxIdAttribute = ConvertToAttributes(TYsonString(completionTxIdRsp->value()));
                    attributesNode->MergeFrom(*completionTxIdAttribute);

                    IMapNodePtr secureVault;
                    if (secureVaultRspOrError.IsOK()) {
                        const auto& secureVaultRsp = secureVaultRspOrError.Value();
                        auto secureVaultNode = ConvertToNode(TYsonString(secureVaultRsp->value()));
                        // It is a pretty strange situation when the node type different
                        // from map, but still we should consider it.
                        if (secureVaultNode->GetType() == ENodeType::Map) {
                            secureVault = secureVaultNode->AsMap();
                        } else {
                            LOG_ERROR("Invalid secure vault node type (OperationId: %v, ActualType: %v, ExpectedType: %v)",
                                operationInfo.Id,
                                secureVaultNode->GetType(),
                                ENodeType::Map);
                            // TODO(max42): (YT-5651) Do not just ignore such a situation!
                        }
                    } else if (secureVaultRspOrError.GetCode() != NYTree::EErrorCode::ResolveError) {
                        THROW_ERROR_EXCEPTION("Error while attempting to fetch the secure vault of operation (OperationId: %v)",
                            operationInfo.Id)
                            << secureVaultRspOrError;
                    }

                    auto operationReport = Owner->CreateOperationFromAttributes(
                        operationInfo.Id,
                        *attributesNode,
                        std::move(secureVault),
                        operationInfo.StorageMode);

                    if (operationReport.Operation) {
                        Result.OperationReports.push_back(operationReport);
                    }
                }
            }
        }

        // - Request committed flag for operations.
        void RequestCommittedFlag()
        {
            std::vector<TOperationReport*> operationsWithOutputTransaction;

            auto getBatchKey = [] (const TOperationReport& report) {
                return "get_op_committed_attr_" + ToString(report.Operation->GetId());
            };

            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);

            {
                LOG_INFO("Fetching committed attribute for operations");
                for (auto& report : Result.OperationReports) {
                    if (!report.ControllerTransactions->Output) {
                        continue;
                    }

                    operationsWithOutputTransaction.push_back(&report);

                    for (auto transactionId : {
                        report.ControllerTransactions->Output->GetId(),
                        NullTransactionId})
                    {
                        {
                            auto req = TYPathProxy::Get(GetOperationPath(report.Operation->GetId()) + "/@");
                            std::vector<TString> attributeKeys{
                                "committed"
                            };
                            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                            SetTransactionId(req, transactionId);
                            batchReq->AddRequest(req, getBatchKey(report));
                        }
                        {
                            auto req = TYPathProxy::Get(GetNewOperationPath(report.Operation->GetId()) + "/@");
                            std::vector<TString> attributeKeys{
                                "committed"
                            };
                            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                            SetTransactionId(req, transactionId);
                            batchReq->AddRequest(req, getBatchKey(report) + "_new");
                        }
                    }
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError);
            const auto& batchRsp = batchRspOrError.Value();

            {
                for (int index = 0; index < static_cast<int>(operationsWithOutputTransaction.size()); ++index) {
                    auto* report = operationsWithOutputTransaction[index];
                    auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>(getBatchKey(*report));
                    auto rspsNew = batchRsp->GetResponses<TYPathProxy::TRspGet>(getBatchKey(*report) + "_new");

                    YCHECK(rsps.size() == 2);
                    YCHECK(rspsNew.size() == 2);

                    for (size_t rspIndex = 0; rspIndex < 2; ++rspIndex) {
                        std::unique_ptr<IAttributeDictionary> attributes;
                        auto updateAttributes = [&] (const TErrorOr<TIntrusivePtr<TYPathProxy::TRspGet>>& response) {
                            if (!response.IsOK()) {
                                return;
                            }

                            auto responseAttributes = ConvertToAttributes(TYsonString(response.Value()->value()));

                            if (attributes) {
                                attributes->MergeFrom(*responseAttributes);
                            } else {
                                attributes = std::move(responseAttributes);
                            }
                        };

                        updateAttributes(rsps[rspIndex]);
                        updateAttributes(rspsNew[rspIndex]);

                        // Commit transaction may be missing or aborted.
                        if (!attributes) {
                            continue;
                        }

                        if (attributes->Get<bool>("committed", false)) {
                            report->IsCommitted = true;
                            if (rspIndex == 0) {
                                report->ShouldCommitOutputTransaction = true;
                            }
                            break;
                        }
                    }
                }
            }
        }

        // Update global watchers.
        void UpdateGlobalWatchers()
        {
            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (auto requester : Owner->GlobalWatcherRequesters) {
                requester.Run(batchReq);
            }
            for (const auto& record : Owner->CustomGlobalWatcherRecords) {
                record.Requester.Run(batchReq);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            auto watcherResponses = batchRspOrError.ValueOrThrow();

            for (auto handler : Owner->GlobalWatcherHandlers) {
                handler.Run(watcherResponses);
            }
            for (const auto& record : Owner->CustomGlobalWatcherRecords) {
                record.Handler.Run(watcherResponses);
            }
        }
    };

    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequest(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTag)
    {
        TObjectServiceProxy proxy(Bootstrap
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(channelKind, cellTag));
        auto batchReq = proxy.ExecuteBatch();
        YCHECK(LockTransaction);
        auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
        auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
        ToProto(prerequisiteTransaction->mutable_transaction_id(), LockTransaction->GetId());
        return batchReq;
    }

    void DoDisconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Connected)
            return;

        LOG_WARNING("Master disconnected");

        Connected.store(false);

        Bootstrap->GetControllerAgent()->Disconnect();

        LockTransaction.Reset();

        OperationNodesUpdateExecutor_->Clear();
        OperationNodesUpdateExecutor_->StopPeriodicUpdates();

        ClearWatcherLists();

        StopPeriodicActivities();

        CancelableContext->Cancel();

        MasterDisconnected_.Fire();

        StartConnecting();
    }


    TOperationReport CreateOperationFromAttributes(
        const TOperationId& operationId,
        const IAttributeDictionary& attributes,
        IMapNodePtr secureVault,
        EOperationCypressStorageMode storageMode)
    {
        auto attachTransaction = [&] (const TTransactionId& transactionId, bool ping, const TString& name = TString()) -> ITransactionPtr {
            if (!transactionId) {
                if (name) {
                    LOG_INFO("Missing %v transaction (OperationId: %v, TransactionId: %v)",
                        name,
                        operationId,
                        transactionId);
                }
                return nullptr;
            }
            try {
                auto connection = Bootstrap->GetRemoteConnectionOrThrow(CellTagFromId(transactionId));
                auto client = connection->CreateNativeClient(TClientOptions(SchedulerUserName));

                TTransactionAttachOptions options;
                options.Ping = ping;
                options.PingAncestors = false;
                return client->AttachTransaction(transactionId, options);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error attaching operation transaction (OperationId: %v, TransactionId: %v)",
                    operationId,
                    transactionId);
                return nullptr;
            }
        };

        TOperationReport result;

        auto userTransactionId = attributes.Get<TTransactionId>("user_transaction_id");
        auto userTransaction = attachTransaction(
            attributes.Get<TTransactionId>("user_transaction_id"),
            false);

        result.ControllerTransactions = New<NControllerAgent::TControllerTransactions>();
        result.ControllerTransactions->Async = attachTransaction(
            attributes.Get<TTransactionId>("async_scheduler_transaction_id"),
            true,
            "async transaction");
        result.ControllerTransactions->Input = attachTransaction(
            attributes.Get<TTransactionId>("input_transaction_id"),
            true,
            "input transaction");
        result.ControllerTransactions->Output = attachTransaction(
            attributes.Get<TTransactionId>("output_transaction_id"),
            true,
            "output transaction");

        result.ControllerTransactions->Completion = attachTransaction(
            attributes.Get<TTransactionId>("completion_transaction_id", TTransactionId()),
            true,
            "completion transaction");

        // COMPAT(ermolovd). We use NullTransactionId as default value for the transition period.
        // Once all clusters are updated to version that creates debug_output transaction
        // this default value can be removed as in other transactions above.
        result.ControllerTransactions->DebugOutput = attachTransaction(
            attributes.Get<TTransactionId>("debug_output_transaction_id", NullTransactionId),
            true,
            "debug output transaction");

        auto spec = attributes.Get<INodePtr>("spec")->AsMap();

        // COMPAT
        TOperationSpecBasePtr operationSpec;
        try {
            operationSpec = ConvertTo<TOperationSpecBasePtr>(spec);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing operation spec (OperationId: %v)",
                operationId);
            return TOperationReport();
        }

        auto runtimeParams = New<TOperationRuntimeParameters>();
        runtimeParams->Owners = attributes.Get<std::vector<TString>>("owners", operationSpec->Owners);
        // Merge initial scheduling options and scheduling options set by user while operation was running.
        auto schedulingOptions = attributes.Find<INodePtr>("scheduling_options_per_pool_tree");
        if (schedulingOptions) {
            Deserialize(runtimeParams->SchedulingOptionsPerPoolTree, schedulingOptions);
        }

        result.Operation = New<TOperation>(
            operationId,
            attributes.Get<EOperationType>("operation_type"),
            attributes.Get<TMutationId>("mutation_id"),
            userTransactionId,
            spec,
            runtimeParams,
            attributes.Get<TString>("authenticated_user"),
            attributes.Get<TInstant>("start_time"),
            Bootstrap->GetControlInvoker(),
            storageMode,
            attributes.Get<EOperationState>("state"),
            attributes.Get<bool>("suspended"),
            attributes.Get<std::vector<TOperationEvent>>("events", {}));

        auto slotIndexMap = attributes.Find<yhash<TString, int>>("slot_index_per_pool_tree");
        if (slotIndexMap) {
            for (const auto& pair : *slotIndexMap) {
                result.Operation->SetSlotIndex(pair.first, pair.second);
            }
        }

        result.Operation->SetSecureVault(std::move(secureVault));

        result.UserTransactionAborted = !userTransaction && userTransactionId;
        result.IsAborting = result.Operation->GetState() == EOperationState::Aborting;

        return result;
    }


    void StartPeriodicActivities()
    {
        OperationNodesUpdateExecutor_->StartPeriodicUpdates(
            CancelableControlInvoker,
            Config->OperationsUpdatePeriod);

        WatchersExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateWatchers, MakeWeak(this)),
            Config->WatchersUpdatePeriod,
            EPeriodicExecutorMode::Automatic);
        WatchersExecutor->Start();

        AlertsExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateAlerts, MakeWeak(this)),
            Config->AlertsUpdatePeriod,
            EPeriodicExecutorMode::Automatic);
        AlertsExecutor->Start();

        for (const auto& record : CustomGlobalWatcherRecords) {
            auto executor = New<TPeriodicExecutor>(
                CancelableControlInvoker,
                BIND(&TImpl::ExecuteCustomWatcherUpdate, MakeWeak(this), record.Requester, record.Handler),
                record.Period,
                EPeriodicExecutorMode::Automatic);
            executor->Start();
            CustomGlobalWatcherExecutors.push_back(executor);
        }
    }

    void StopPeriodicActivities()
    {
        if (WatchersExecutor) {
            WatchersExecutor->Stop();
            WatchersExecutor.Reset();
        }

        if (AlertsExecutor) {
            AlertsExecutor->Stop();
            AlertsExecutor.Reset();
        }

        for (const auto& executor : CustomGlobalWatcherExecutors) {
            executor->Stop();
        }
        CustomGlobalWatcherExecutors.clear();
    }

    TWatcherList* GetOrCreateWatcherList(TOperationPtr operation)
    {
        auto it = WatcherLists.find(operation->GetId());
        if (it == WatcherLists.end()) {
            it = WatcherLists.insert(std::make_pair(
                operation->GetId(),
                TWatcherList(operation))).first;
        }
        return &it->second;
    }

    TWatcherList* FindWatcherList(TOperationPtr operation)
    {
        auto it = WatcherLists.find(operation->GetId());
        return it == WatcherLists.end() ? nullptr : &it->second;
    }

    void ClearWatcherLists()
    {
        WatcherLists.clear();
    }

    bool IsOperationInFinishedState(const TOperationNodeUpdate* update) const
    {
        const auto& operation = update->Operation;
        return operation->IsFinishedState();
    }

    void OnOperationUpdateFailed(const TError& error)
    {
        YCHECK(!error.IsOK());
        LOG_ERROR(error, "Failed to update operation node");

        if (!Connected) {
            Disconnect();
        }
    }

    void UpdateOperationNodeAttributes(TOperationPtr operation)
    {
        operation->SetShouldFlush(false);

        auto batchReq = StartObjectBatchRequest();
        GenerateMutationId(batchReq);

        auto paths = GetCompatibilityOperationPaths(operation->GetId(), operation->GetStorageMode());
        for (const auto& operationPath : paths) {
            // Set operation acl if needed.
            if (operation->GetShouldFlushAcl()) {
                operation->SetShouldFlushAcl(false);

                auto aclBatchReq = StartObjectBatchRequest();
                auto req = TYPathProxy::Set(operationPath + "/@acl");
                req->set_value(BuildYsonStringFluently()
                    .Do(BIND(&TImpl::BuildOperationAcl, Unretained(this), operation))
                    .GetData());
                aclBatchReq->AddRequest(req, "set_acl");

                auto aclBatchRspOrError = WaitFor(aclBatchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(aclBatchRspOrError);

                auto rspOrErr = aclBatchRspOrError.Value()->GetResponse("set_acl");
                if (!rspOrErr.IsOK()) {
                    auto error = TError("Failed to set operation ACL")
                        << TErrorAttribute("operation_id", operation->GetId())
                        << rspOrErr;
                    operation->MutableAlerts()[EOperationAlertType::InvalidAcl] = error;
                    LOG_INFO(error);
                } else {
                    if (!operation->Alerts()[EOperationAlertType::InvalidAcl].IsOK()) {
                        operation->MutableAlerts()[EOperationAlertType::InvalidAcl] = TError();
                    }
                }
            }

            auto multisetReq = TYPathProxy::Multiset(operationPath + "/@");

            // Set suspended flag.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("suspended");
                req->set_value(ConvertToYsonString(operation->GetSuspended()).GetData());
            }

            // Set events.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("events");
                req->set_value(ConvertToYsonString(operation->GetEvents()).GetData());
            }

            // Set result.
            if (operation->IsFinishedState()) {
                auto req = multisetReq->add_subrequests();
                req->set_key("result");
                auto error = FromProto<TError>(operation->Result().error());
                auto errorString = BuildYsonStringFluently()
                    .BeginMap()
                    .Item("error").Value(error)
                    .EndMap();
                req->set_value(errorString.GetData());
            }

            // Set end time, if given.
            if (operation->GetFinishTime()) {
                auto req = multisetReq->add_subrequests();
                req->set_key("finish_time");
                req->set_value(ConvertToYsonString(*operation->GetFinishTime()).GetData());
            }

            // Set state.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("state");
                req->set_value(ConvertToYsonString(operation->GetState()).GetData());
            }

            // Set alerts.
            {
                auto req = multisetReq->add_subrequests();
                req->set_key("alerts");
                const auto& alerts = operation->Alerts();
                req->set_value(BuildYsonStringFluently()
                    .DoMapFor(TEnumTraits<EOperationAlertType>::GetDomainValues(),
                        [&] (TFluentMap fluent, EOperationAlertType alertType) {
                            if (!alerts[alertType].IsOK()) {
                                fluent.Item(FormatEnum(alertType)).Value(alerts[alertType]);
                            }
                        })
                    .GetData());
            }

            batchReq->AddRequest(multisetReq, "update_op_node");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));

        LOG_DEBUG("Operation node updated (OperationId: %v)", operation->GetId());
    }

    void DoUpdateOperationNode(TOperationPtr operation)
    {
        try {
            UpdateOperationNodeAttributes(operation);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating operation node %v",
                operation->GetId())
                << ex;
        }
    }

    TCallback<TFuture<void>()> UpdateOperationNode(const TOperationId&, TOperationNodeUpdate* update)
    {
        if (update->Operation->GetShouldFlush()) {
            return BIND(&TImpl::DoUpdateOperationNode,
                MakeStrong(this),
                update->Operation)
                .AsyncVia(CancelableControlInvoker);
        } else {
            return VoidCallback_;
        }
    }

    void OnOperationNodeCreated(
        TOperationPtr operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();
        auto error = GetCumulativeError(batchRspOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error creating operation node %v",
            operationId);

        OperationNodesUpdateExecutor_->AddUpdate(operation->GetId(), TOperationNodeUpdate(operation));

        LOG_INFO("Operation node created (OperationId: %v)",
            operationId);
    }

    void OnRevivingOperationNodeReset(
        TOperationPtr operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto operationId = operation->GetId();

        auto error = GetCumulativeError(batchRspOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error resetting reviving operation node %v",
            operationId);

        LOG_INFO("Reviving operation node reset (OperationId: %v)",
            operationId);
    }

    void ExecuteCustomWatcherUpdate(const TWatcherRequester& requester, const TWatcherHandler& handler)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);
        requester.Run(batchReq);
        auto batchRspOrError = WaitFor(batchReq->Invoke());
        if (!batchRspOrError.IsOK()) {
            LOG_ERROR(batchRspOrError, "Error updating custom watcher");
            return;
        }
        handler.Run(batchRspOrError.Value());
    }

    void UpdateWatchers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_INFO("Updating watchers");

        // Global watchers.
        {
            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (auto requester : GlobalWatcherRequesters) {
                requester.Run(batchReq);
            }
            batchReq->Invoke().Subscribe(
                BIND(&TImpl::OnGlobalWatchersUpdated, MakeStrong(this))
                    .Via(CancelableControlInvoker));
        }

        // Purge obsolete watchers.
        {
            auto it = WatcherLists.begin();
            while (it != WatcherLists.end()) {
                auto jt = it++;
                const auto& list = jt->second;
                if (list.Operation->IsFinishedState()) {
                    WatcherLists.erase(jt);
                }
            }
        }

        // Per-operation watchers.
        for (const auto& pair : WatcherLists) {
            const auto& list = pair.second;
            auto operation = list.Operation;
            if (operation->GetState() != EOperationState::Running)
                continue;

            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (auto requester : list.WatcherRequesters) {
                requester.Run(batchReq);
            }
            batchReq->Invoke().Subscribe(
                BIND(&TImpl::OnOperationWatchersUpdated, MakeStrong(this), operation)
                    .Via(CancelableControlInvoker));
        }
    }

    void OnGlobalWatchersUpdated(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        if (!batchRspOrError.IsOK()) {
            LOG_ERROR(batchRspOrError, "Error updating global watchers");
            return;
        }

        const auto& batchRsp = batchRspOrError.Value();
        for (auto handler : GlobalWatcherHandlers) {
            handler.Run(batchRsp);
        }

        LOG_INFO("Global watchers updated");
    }

    void OnOperationWatchersUpdated(TOperationPtr operation, const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        if (!batchRspOrError.IsOK()) {
            LOG_ERROR(batchRspOrError, "Error updating operation watchers (OperationId: %v)",
                operation->GetId());
            return;
        }

        if (operation->GetState() != EOperationState::Running)
            return;

        auto* list = FindWatcherList(operation);
        if (!list)
            return;

        const auto& batchRsp = batchRspOrError.Value();
        for (auto handler : list->WatcherHandlers) {
            handler.Run(batchRsp);
        }

        LOG_INFO("Operation watchers updated (OperationId: %v)",
            operation->GetId());
    }

    void UpdateAlerts()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        std::vector<TError> alerts;
        for (auto alertType : TEnumTraits<ESchedulerAlertType>::GetDomainValues()) {
            const auto& alert = Alerts[alertType];
            if (!alert.IsOK()) {
                alerts.push_back(alert);
            }
        }

        TObjectServiceProxy proxy(Bootstrap
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, PrimaryMasterCellTag));
        auto req = TYPathProxy::Set("//sys/scheduler/@alerts");
        req->set_value(ConvertToYsonString(alerts).GetData());

        auto rspOrError = WaitFor(proxy.Execute(req));
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error updating scheduler alerts");
        }
    }

    void OnClusterDirectorySynchronized(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SetSchedulerAlert(ESchedulerAlertType::SyncClusterDirectory, error);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    TSchedulerConfigPtr config,
    NCellScheduler::TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TMasterConnector::~TMasterConnector() = default;

void TMasterConnector::Start()
{
    Impl->Start();
}

bool TMasterConnector::IsConnected() const
{
    return Impl->IsConnected();
}

void TMasterConnector::Disconnect()
{
    return Impl->Disconnect();
}

IInvokerPtr TMasterConnector::GetCancelableControlInvoker() const
{
    return Impl->GetCancelableControlInvoker();
}

TFuture<void> TMasterConnector::CreateOperationNode(TOperationPtr operation)
{
    return Impl->CreateOperationNode(operation);
}

TFuture<void> TMasterConnector::ResetRevivingOperationNode(TOperationPtr operation)
{
    return Impl->ResetRevivingOperationNode(operation);
}

TFuture<void> TMasterConnector::FlushOperationNode(TOperationPtr operation)
{
    return Impl->FlushOperationNode(operation);
}

TFuture<void> TMasterConnector::UpdateOperationRuntimeParameters(
    TOperationPtr operation,
    const TOperationRuntimeParametersPtr& params)
{
    return Impl->UpdateOperationRuntimeParameters(operation, params);
}

void TMasterConnector::SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
{
    Impl->SetSchedulerAlert(alertType, alert);
}

void TMasterConnector::UpdateConfig(const TSchedulerConfigPtr& config)
{
    Impl->UpdateConfig(config);
}

void TMasterConnector::AddGlobalWatcherRequester(TWatcherRequester requester)
{
    Impl->AddGlobalWatcherRequester(requester);
}

void TMasterConnector::AddGlobalWatcherHandler(TWatcherHandler handler)
{
    Impl->AddGlobalWatcherHandler(handler);
}

void TMasterConnector::AddGlobalWatcher(TWatcherRequester requester, TWatcherHandler handler, TDuration period)
{
    Impl->AddGlobalWatcher(std::move(requester), std::move(handler), period);
}

void TMasterConnector::AddOperationWatcherRequester(TOperationPtr operation, TWatcherRequester requester)
{
    Impl->AddOperationWatcherRequester(operation, requester);
}

void TMasterConnector::AddOperationWatcherHandler(TOperationPtr operation, TWatcherHandler handler)
{
    Impl->AddOperationWatcherHandler(operation, handler);
}

DELEGATE_SIGNAL(TMasterConnector, void(const TMasterHandshakeResult& result), MasterConnected, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterDisconnected, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

