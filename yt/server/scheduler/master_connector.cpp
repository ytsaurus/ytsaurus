#include "master_connector.h"
#include "config.h"
#include "helpers.h"
#include "scheduler.h"
#include "scheduler_strategy.h"

#include <yt/server/controller_agent/master_connector.h>
#include <yt/server/controller_agent/controller_agent.h>
#include <yt/server/controller_agent/operation_controller.h>

#include <yt/server/cell_scheduler/bootstrap.h>
#include <yt/server/cell_scheduler/config.h>

#include <yt/server/misc/update_executor.h>

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

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/utilex/random.h>

#include <yt/core/actions/cancelable_context.h>

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

using std::placeholders::_1;

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
    { }

    void Start()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Bootstrap
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetClusterDirectorySynchronizer()
            ->SubscribeSynchronized(BIND(&TImpl::OnClusterDirectorySynchronized, MakeWeak(this))
                .Via(Bootstrap->GetControlInvoker(EControlQueue::MasterConnector)));

        Bootstrap->GetControlInvoker()->Invoke(BIND(
            &TImpl::StartConnecting,
            MakeStrong(this)));
    }

    EMasterConnectorState GetState() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return State.load();
    }

    TInstant GetConnectionTime() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ConnectionTime.load();
    }

    void Disconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoDisconnect();
    }

    IInvokerPtr GetCancelableControlInvoker() const
    {
        // XXX(babenko): fixme
        //VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EMasterConnectorState::Disconnected);

        return CancelableControlInvoker;
    }

    void StartOperationNodeUpdates(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EMasterConnectorState::Disconnected);

        OperationNodesUpdateExecutor_->AddUpdate(operation->GetId(), TOperationNodeUpdate(operation));
    }

    TFuture<void> CreateOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EMasterConnectorState::Disconnected);

        auto operationId = operation->GetId();
        LOG_INFO("Creating operation node (OperationId: %v)",
            operationId);

        auto strategy = Bootstrap->GetScheduler()->GetStrategy();

        auto batchReq = StartObjectBatchRequest();

        {
            auto req = TCypressYPathProxy::Create(GetNewOperationPath(operationId));
            req->set_type(static_cast<int>(EObjectType::MapNode));
            req->set_recursive(true);
            req->set_force(true);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

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
                    .Item("acl").Do(std::bind(&TImpl::BuildOperationAcl, operation, _1))
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
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        if (operation->GetSecureVault()) {
            // Create secure vault.
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("inherit_acl", false);
            attributes->Set("value", operation->GetSecureVault());
            attributes->Set("acl", BuildYsonStringFluently()
                .Do(std::bind(&TImpl::BuildOperationAcl, operation, _1)));

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
                .AsyncVia(CancelableControlInvoker));
    }

    TFuture<void> ResetRevivingOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EMasterConnectorState::Disconnected);
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
            .AsyncVia(CancelableControlInvoker));
    }

    TFuture<void> FlushOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EMasterConnectorState::Disconnected);

        return OperationNodesUpdateExecutor_->ExecuteUpdate(operation->GetId());
    }


    void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Alerts[alertType] = alert;
    }

    void AddGlobalWatcherRequester(TWatcherRequester requester)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        GlobalWatcherRequesters.push_back(requester);
    }

    void AddGlobalWatcherHandler(TWatcherHandler handler)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        GlobalWatcherHandlers.push_back(handler);
    }

    void AddGlobalWatcher(TWatcherRequester requester, TWatcherHandler handler, TDuration period)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CustomGlobalWatcherRecords.push_back(TPeriodicExecutorRecord{std::move(requester), std::move(handler), period});
    }

    void AddOperationWatcherRequester(const TOperationPtr& operation, TWatcherRequester requester)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EMasterConnectorState::Disconnected);

        auto* list = GetOrCreateWatcherList(operation);
        list->WatcherRequesters.push_back(requester);
    }

    void AddOperationWatcherHandler(const TOperationPtr& operation, TWatcherHandler handler)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EMasterConnectorState::Disconnected);

        auto* list = GetOrCreateWatcherList(operation);
        list->WatcherHandlers.push_back(handler);
    }

    void UpdateConfig(const TSchedulerConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Config = config;

        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->SetPeriod(Config->OperationsUpdatePeriod);
        }
        if (WatchersExecutor) {
            WatchersExecutor->SetPeriod(Config->WatchersUpdatePeriod);
        }
        if (AlertsExecutor) {
            AlertsExecutor->SetPeriod(Config->AlertsUpdatePeriod);
        }

        ScheduleTestingDisconnect();
    }

    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnecting);
    DEFINE_SIGNAL(void(), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* const Bootstrap;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;

    std::atomic<EMasterConnectorState> State = {EMasterConnectorState::Disconnected};
    std::atomic<TInstant> ConnectionTime;

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
        explicit TWatcherList(const TOperationPtr& operation)
            : Operation(operation)
        { }

        TOperationPtr Operation;
        std::vector<TWatcherRequester> WatcherRequesters;
        std::vector<TWatcherHandler>   WatcherHandlers;
    };

    yhash<TOperationId, TWatcherList> WatcherLists;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void ScheduleConnectRetry()
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::StartConnecting, MakeStrong(this))
                .Via(Bootstrap->GetControlInvoker()),
            Config->ConnectRetryBackoffTime);
    }

    void ScheduleTestingDisconnect()
    {
        if (Config->TestingOptions->EnableRandomMasterDisconnection) {
            TDelayedExecutor::Submit(
                BIND(&TImpl::RandomDisconnect, MakeStrong(this))
                    .Via(Bootstrap->GetControlInvoker()),
                RandomDuration(Config->TestingOptions->RandomMasterDisconnectionMaxBackoff));
        }
    }

    void RandomDisconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Disconnecting scheduler due to enabled random disconnection");
        DoDisconnect();
    }

    void StartConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Connecting to master");

        YCHECK(State == EMasterConnectorState::Disconnected);
        State = EMasterConnectorState::Connecting;

        YCHECK(!CancelableContext);
        CancelableContext = New<TCancelableContext>();

        YCHECK(!CancelableControlInvoker);
        CancelableControlInvoker = CancelableContext->CreateInvoker(
            Bootstrap->GetControlInvoker(EControlQueue::MasterConnector));

        OperationNodesUpdateExecutor_ = New<TUpdateExecutor<TOperationId, TOperationNodeUpdate>>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateOperationNode, Unretained(this)),
            BIND(&TImpl::IsOperationInFinishedState, Unretained(this)),
            BIND(&TImpl::OnOperationUpdateFailed, Unretained(this)),
            Config->OperationsUpdatePeriod,
            Logger);

        WatchersExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateWatchers, MakeWeak(this)),
            Config->WatchersUpdatePeriod,
            EPeriodicExecutorMode::Automatic);

        AlertsExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateAlerts, MakeWeak(this)),
            Config->AlertsUpdatePeriod,
            EPeriodicExecutorMode::Automatic);

        for (const auto& record : CustomGlobalWatcherRecords) {
            auto executor = New<TPeriodicExecutor>(
                CancelableControlInvoker,
                BIND(&TImpl::ExecuteCustomWatcherUpdate, MakeWeak(this), record.Requester, record.Handler),
                record.Period,
                EPeriodicExecutorMode::Automatic);
            CustomGlobalWatcherExecutors.push_back(executor);
        }

        auto pipeline = New<TRegistrationPipeline>(this);
        BIND(&TRegistrationPipeline::Run, pipeline)
            .AsyncVia(CancelableControlInvoker)
            .Run()
            .Subscribe(BIND(&TImpl::OnConnected, MakeStrong(this))
                .Via(CancelableControlInvoker));
    }

    void OnConnected(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State == EMasterConnectorState::Connecting);

        if (!error.IsOK()) {
            LOG_WARNING(error, "Error connecting to master");
            DoCleanup();
            ScheduleConnectRetry();
            return;
        }

        State.store(EMasterConnectorState::Connected);
        ConnectionTime.store(TInstant::Now());

        LOG_INFO("Master connected");

        TForbidContextSwitchGuard contextSwitchGuard;

        LockTransaction->SubscribeAborted(
            BIND(&TImpl::OnLockTransactionAborted, MakeWeak(this))
                .Via(CancelableControlInvoker));

        StartPeriodicActivities();

        ScheduleTestingDisconnect();

        try {
            MasterConnected_.Fire();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Master connection failed");
            Disconnect();
            return;
        }
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

        void Run()
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
            FireConnectingSignal();
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
                "slot_index_per_pool_tree"
            };

            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);
            {
                LOG_INFO("Fetching attributes and secure vaults for unfinished operations (UnfinishedOperationCount: %v)",
                    RunningOperations.size());

                for (const auto& operationInfo : RunningOperations) {
                    // Keep stuff below in sync with #TryCreateOperationFromAttributes.

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

                    auto operation = Owner->TryCreateOperationFromAttributes(
                        operationInfo.Id,
                        *attributesNode,
                        std::move(secureVault),
                        operationInfo.StorageMode);
                    if (operation) {
                        Result.Operations.push_back(operation);
                    }
                }
            }
        }

        void RequestCommittedFlag()
        {
            std::vector<TOperationPtr> operationsWithOutputTransaction;

            auto getBatchKey = [] (const TOperationPtr& operation) {
                return "get_op_committed_attr_" + ToString(operation->GetId());
            };

            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);

            {
                LOG_INFO("Fetching committed attribute for operations");
                for (const auto& operation : Result.Operations) {
                    if (!operation->RevivalDescriptor()) {
                        continue;
                    }

                    auto& revivalDescriptor = *operation->RevivalDescriptor();
                    if (!revivalDescriptor.ControllerTransactions->Output) {
                        continue;
                    }

                    operationsWithOutputTransaction.push_back(operation);

                    for (auto transactionId : {
                        revivalDescriptor.ControllerTransactions->Output->GetId(),
                        NullTransactionId})
                    {
                        {
                            auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()) + "/@");
                            std::vector<TString> attributeKeys{
                                "committed"
                            };
                            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                            SetTransactionId(req, transactionId);
                            batchReq->AddRequest(req, getBatchKey(operation));
                        }
                        {
                            auto req = TYPathProxy::Get(GetNewOperationPath(operation->GetId()) + "/@");
                            std::vector<TString> attributeKeys{
                                "committed"
                            };
                            ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                            SetTransactionId(req, transactionId);
                            batchReq->AddRequest(req, getBatchKey(operation) + "_new");
                        }
                    }
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError);
            const auto& batchRsp = batchRspOrError.Value();

            {
                for (const auto& operation : operationsWithOutputTransaction) {
                    auto& revivalDescriptor = *operation->RevivalDescriptor();
                    auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>(getBatchKey(operation));
                    auto rspsNew = batchRsp->GetResponses<TYPathProxy::TRspGet>(getBatchKey(operation) + "_new");

                    YCHECK(rsps.size() == 2);
                    YCHECK(rspsNew.size() == 2);

                    for (size_t rspIndex = 0; rspIndex < 2; ++rspIndex) {
                        std::unique_ptr<IAttributeDictionary> attributes;
                        auto updateAttributes = [&] (const TErrorOr<TIntrusivePtr<TYPathProxy::TRspGet>>& rspOrError) {
                            if (!rspOrError.IsOK()) {
                                return;
                            }

                            auto responseAttributes = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
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
                            revivalDescriptor.OperationCommitted = true;
                            if (rspIndex == 0) {
                                revivalDescriptor.ShouldCommitOutputTransaction = true;
                            }
                            break;
                        }
                    }
                }
            }
        }

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

        void FireConnectingSignal()
        {
            Owner->MasterConnecting_.Fire(Result);
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


    void DoCleanup()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LockTransaction.Reset();

        ClearWatcherLists();

        StopPeriodicActivities();

        if (CancelableContext) {
            CancelableContext->Cancel();
            CancelableContext.Reset();
        }

        CancelableControlInvoker.Reset();

        State.store(EMasterConnectorState::Disconnected);
    }

    void DoDisconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (State == EMasterConnectorState::Connected) {
            try {
                MasterDisconnected_.Fire();
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Error disconnecting from master");
            }

            LOG_WARNING("Master disconnected");
        }

        DoCleanup();
        StartConnecting();
    }


    static void BuildOperationAcl(const TOperationPtr& operation, TFluentAny fluent)
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

    TOperationPtr TryCreateOperationFromAttributes(
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

        auto state = attributes.Get<EOperationState>("state");

        auto specNode = attributes.Get<INodePtr>("spec")->AsMap();

        auto userTransactionId = attributes.Get<TTransactionId>("user_transaction_id");
        auto userTransaction = attachTransaction(
            attributes.Get<TTransactionId>("user_transaction_id"),
            false);

        TOperationRevivalDescriptor revivalDescriptor;
        revivalDescriptor.ControllerTransactions = New<NControllerAgent::TControllerTransactions>();
        revivalDescriptor.ControllerTransactions->Async = attachTransaction(
            attributes.Get<TTransactionId>("async_scheduler_transaction_id"),
            true,
            "async transaction");
        revivalDescriptor.ControllerTransactions->Input = attachTransaction(
            attributes.Get<TTransactionId>("input_transaction_id"),
            true,
            "input transaction");
        revivalDescriptor.ControllerTransactions->Output = attachTransaction(
            attributes.Get<TTransactionId>("output_transaction_id"),
            true,
            "output transaction");

        revivalDescriptor.ControllerTransactions->Completion = attachTransaction(
            attributes.Get<TTransactionId>("completion_transaction_id", TTransactionId()),
            true,
            "completion transaction");

        // COMPAT(ermolovd). We use NullTransactionId as default value for the transition period.
        // Once all clusters are updated to version that creates debug_output transaction
        // this default value can be removed as in other transactions above.
        revivalDescriptor.ControllerTransactions->DebugOutput = attachTransaction(
            attributes.Get<TTransactionId>("debug_output_transaction_id", NullTransactionId),
            true,
            "debug output transaction");

        TOperationSpecBasePtr spec;
        try {
            spec = ConvertTo<TOperationSpecBasePtr>(specNode);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing operation spec (OperationId: %v)",
                operationId);
            return nullptr;
        }

        revivalDescriptor.UserTransactionAborted = !userTransaction && userTransactionId;
        revivalDescriptor.OperationAborting = state == EOperationState::Aborting;

        auto operation = New<TOperation>(
            operationId,
            attributes.Get<EOperationType>("operation_type"),
            attributes.Get<TMutationId>("mutation_id"),
            userTransactionId,
            specNode,
            secureVault,
            BuildOperationRuntimeParams(spec),
            attributes.Get<TString>("authenticated_user"),
            attributes.Get<std::vector<TString>>("owners", spec->Owners),
            attributes.Get<TInstant>("start_time"),
            Bootstrap->GetControlInvoker(),
            storageMode,
            state,
            attributes.Get<bool>("suspended"),
            attributes.Get<std::vector<TOperationEvent>>("events", {}),
            revivalDescriptor);

        auto slotIndexMap = attributes.Find<yhash<TString, int>>("slot_index_per_pool_tree");
        if (slotIndexMap) {
            for (const auto& pair : *slotIndexMap) {
                operation->SetSlotIndex(pair.first, pair.second);
            }
        }

        return operation;
    }


    void StartPeriodicActivities()
    {
        OperationNodesUpdateExecutor_->Start();

        WatchersExecutor->Start();

        AlertsExecutor->Start();

        for (const auto& executor : CustomGlobalWatcherExecutors) {
            executor->Start();
        }
    }

    void StopPeriodicActivities()
    {
        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->Stop();
            OperationNodesUpdateExecutor_.Reset();
        }

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


    TWatcherList* GetOrCreateWatcherList(const TOperationPtr& operation)
    {
        auto it = WatcherLists.find(operation->GetId());
        if (it == WatcherLists.end()) {
            it = WatcherLists.insert(std::make_pair(
                operation->GetId(),
                TWatcherList(operation))).first;
        }
        return &it->second;
    }

    TWatcherList* FindWatcherList(const TOperationPtr& operation)
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
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& operation = update->Operation;
        return operation->IsFinishedState();
    }

    void OnOperationUpdateFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(!error.IsOK());
        LOG_ERROR(error, "Failed to update operation node");

        Disconnect();
    }

    void DoUpdateOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            operation->SetShouldFlush(false);

            auto batchReq = StartObjectBatchRequest();
            GenerateMutationId(batchReq);

            auto paths = GetCompatibilityOperationPaths(operation->GetId(), operation->GetStorageMode());
            for (const auto& operationPath : paths) {
                // Set operation acl.
                {
                    auto aclBatchReq = StartObjectBatchRequest();
                    auto req = TYPathProxy::Set(operationPath + "/@acl");
                    req->set_value(BuildYsonStringFluently()
                        .Do(std::bind(&TImpl::BuildOperationAcl, operation, _1))
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

                // Set suspended flag.
                {
                    auto req = TYPathProxy::Set(operationPath + "/@suspended");
                    req->set_value(ConvertToYsonString(operation->GetSuspended()).GetData());
                    batchReq->AddRequest(req, "update_op_node");
                }

                // Set events.
                {
                    auto req = TYPathProxy::Set(operationPath + "/@events");
                    req->set_value(ConvertToYsonString(operation->GetEvents()).GetData());
                    batchReq->AddRequest(req, "update_op_node");
                }

                // Set result.
                if (operation->IsFinishedState()) {
                    auto req = TYPathProxy::Set(operationPath + "/@result");
                    auto error = FromProto<TError>(operation->Result().error());
                    auto errorString = BuildYsonStringFluently()
                        .BeginMap()
                        .Item("error").Value(error)
                        .EndMap();
                    req->set_value(errorString.GetData());
                    batchReq->AddRequest(req, "update_op_node");
                }

                // Set end time, if given.
                if (operation->GetFinishTime()) {
                    auto req = TYPathProxy::Set(operationPath + "/@finish_time");
                    req->set_value(ConvertToYsonString(*operation->GetFinishTime()).GetData());
                    batchReq->AddRequest(req, "update_op_node");
                }

                // Set state.
                {
                    auto req = TYPathProxy::Set(operationPath + "/@state");
                    req->set_value(ConvertToYsonString(operation->GetState()).GetData());
                    batchReq->AddRequest(req, "update_op_node");
                }

                // Set alerts.
                {
                    auto req = TYPathProxy::Set(operationPath + "/@alerts");
                    const auto& alerts = operation->Alerts();
                    req->set_value(BuildYsonStringFluently()
                        .DoMapFor(TEnumTraits<EOperationAlertType>::GetDomainValues(),
                            [&] (TFluentMap fluent, EOperationAlertType alertType) {
                                if (!alerts[alertType].IsOK()) {
                                    fluent.Item(FormatEnum(alertType)).Value(alerts[alertType]);
                                }
                            })
                        .GetData());
                    batchReq->AddRequest(req, "update_op_node");
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));

            LOG_DEBUG("Operation node updated (OperationId: %v)", operation->GetId());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating operation node %v",
                operation->GetId())
                << ex;
        }
    }

    TCallback<TFuture<void>()> UpdateOperationNode(const TOperationId& /*operationId*/, TOperationNodeUpdate* update)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!update->Operation->GetShouldFlush()) {
            return {};
        }

        return BIND(&TImpl::DoUpdateOperationNode,
            MakeStrong(this),
            update->Operation)
            .AsyncVia(CancelableControlInvoker);
    }

    void OnOperationNodeCreated(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();
        auto error = GetCumulativeError(batchRspOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error creating operation node %v",
            operationId);

        LOG_INFO("Operation node created (OperationId: %v)",
            operationId);
    }

    void OnRevivingOperationNodeReset(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        YCHECK(State == EMasterConnectorState::Connected);

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
        YCHECK(State == EMasterConnectorState::Connected);

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

    void OnOperationWatchersUpdated(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State == EMasterConnectorState::Connected);

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
        YCHECK(State == EMasterConnectorState::Connected);

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

EMasterConnectorState TMasterConnector::GetState() const
{
    return Impl->GetState();
}

TInstant TMasterConnector::GetConnectionTime() const
{
    return Impl->GetConnectionTime();
}

void TMasterConnector::Disconnect()
{
    Impl->Disconnect();
}

IInvokerPtr TMasterConnector::GetCancelableControlInvoker() const
{
    return Impl->GetCancelableControlInvoker();
}

void TMasterConnector::StartOperationNodeUpdates(const TOperationPtr& operation)
{
    Impl->StartOperationNodeUpdates(operation);
}

TFuture<void> TMasterConnector::CreateOperationNode(const TOperationPtr& operation)
{
    return Impl->CreateOperationNode(operation);
}

TFuture<void> TMasterConnector::ResetRevivingOperationNode(const TOperationPtr& operation)
{
    return Impl->ResetRevivingOperationNode(operation);
}

TFuture<void> TMasterConnector::FlushOperationNode(const TOperationPtr& operation)
{
    return Impl->FlushOperationNode(operation);
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

void TMasterConnector::AddOperationWatcherRequester(const TOperationPtr& operation, TWatcherRequester requester)
{
    Impl->AddOperationWatcherRequester(operation, requester);
}

void TMasterConnector::AddOperationWatcherHandler(const TOperationPtr& operation, TWatcherHandler handler)
{
    Impl->AddOperationWatcherHandler(operation, handler);
}

DELEGATE_SIGNAL(TMasterConnector, void(const TMasterHandshakeResult& result), MasterConnecting, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterConnected, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterDisconnected, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

