#include "master_connector.h"
#include "config.h"
#include "helpers.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "bootstrap.h"

#include <yt/server/controller_agent/helpers.h>

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

using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::GetDefaultAddress;

using std::placeholders::_1;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    { }

    void Start()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetClusterDirectorySynchronizer()
            ->SubscribeSynchronized(BIND(&TImpl::OnClusterDirectorySynchronized, MakeWeak(this))
                .Via(Bootstrap_->GetControlInvoker(EControlQueue::MasterConnector)));

        StartConnecting(true);
    }

    EMasterConnectorState GetState() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return State_.load();
    }

    TInstant GetConnectionTime() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ConnectionTime_.load();
    }

    void Disconnect(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoDisconnect(error);
    }

    const IInvokerPtr& GetCancelableControlInvoker(EControlQueue queue = EControlQueue::MasterConnector) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        return CancelableControlInvokers_[queue];
    }

    void StartOperationNodeUpdates(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        OperationNodesUpdateExecutor_->AddUpdate(operation->GetId(), TOperationNodeUpdate(operation));
    }

    TFuture<void> CreateOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        auto operationId = operation->GetId();
        LOG_INFO("Creating operation node (OperationId: %v)",
            operationId);

        auto batchReq = StartObjectBatchRequest();

        auto operationYson = BuildYsonStringFluently()
            .BeginAttributes()
                .Do(BIND(&BuildMinimalOperationAttributes, operation))
                .Item("opaque").Value(true)
                .Item("acl").Do(std::bind(&TImpl::BuildOperationAcl, operation, _1))
                .Item("owners").Value(operation->GetOwners())
            .EndAttributes()
            .BeginMap()
                .Item("jobs").BeginAttributes()
                    .Item("opaque").Value(true)
                .EndAttributes()
                .BeginMap().EndMap()
            .EndMap()
            .GetData();

        auto paths = GetCompatibilityOperationPaths(operationId);
        auto secureVaultPaths = GetCompatibilityOperationPaths(operationId, "secure_vault");

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
                .AsyncVia(GetCancelableControlInvoker()));
    }

    TFuture<void> UpdateInitializedOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        auto operationId = operation->GetId();
        LOG_INFO("Updating initialized operation node (OperationId: %v)",
            operationId);

        auto strategy = Bootstrap_->GetScheduler()->GetStrategy();

        auto batchReq = StartObjectBatchRequest();

        bool isReviving = operation->GetState() == EOperationState::Reviving;
        auto attributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .DoIf(!isReviving, BIND(&ISchedulerStrategy::BuildOperationAttributes, strategy, operationId))
                .Do(BIND(&BuildFullOperationAttributes, operation))
                .Item("brief_spec").BeginMap()
                    .Items(operation->ControllerAttributes().InitializationAttributes->BriefSpec)
                    .Do(BIND(&ISchedulerStrategy::BuildBriefSpec, strategy, operationId))
                .EndMap()
            .EndMap());

        auto paths = GetCompatibilityOperationPaths(operationId);
        for (const auto& path : paths) {
            auto req = TYPathProxy::Multiset(path + "/@");
            GenerateMutationId(req);
            for (const auto& key : attributes->List()) {
                auto* subrequest = req->add_subrequests();
                subrequest->set_key(key);
                subrequest->set_value(attributes->GetYson(key).GetData());
            }
            batchReq->AddRequest(req);
        }

        return batchReq->Invoke().Apply(
            BIND(
                &TImpl::OnInitializedOperationNodeUpdated,
                MakeStrong(this),
                operation)
            .AsyncVia(GetCancelableControlInvoker()));
    }

    TFuture<void> FlushOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        LOG_INFO("Flushing operation node (OperationId: %v)",
            operation->GetId());

        return OperationNodesUpdateExecutor_->ExecuteUpdate(operation->GetId());
    }

    TFuture<void> FetchOperationRevivalDescriptors(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        return BIND(&TImpl::DoFetchOperationRevivalDescriptors, MakeStrong(this))
            .AsyncVia(GetCancelableControlInvoker())
            .Run(operations);
    }

    void AttachJobContext(
        const TYPath& path,
        const TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId,
        const TString& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            TJobFile file{
                jobId,
                path,
                chunkId,
                "input_context"
            };
            auto client = CreateNativeClient(Bootstrap_->GetMasterClient()->GetNativeConnection(), TClientOptions(user));
            SaveJobFiles(client, operationId, { file });
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error saving input context for job %v into %v", jobId, path)
                << ex;
        }
    }

    TFuture<void> FlushOperationRuntimeParameters(
        TOperationPtr operation,
        const TOperationRuntimeParametersPtr& params)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TImpl::DoFlushOperationRuntimeParameters, MakeStrong(this))
            .AsyncVia(GetCancelableControlInvoker())
            .Run(operation, params);
    }

    void DoFlushOperationRuntimeParameters(
        TOperationPtr operation,
        const TOperationRuntimeParametersPtr& params)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        LOG_INFO("Flushing operation runtime parameters (OperationId: %v)",
            operation->GetId());

        auto strategy = Bootstrap_->GetScheduler()->GetStrategy();

        auto batchReq = StartObjectBatchRequest();
        auto paths = GetCompatibilityOperationPaths(operation->GetId());

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

        LOG_INFO("Flushed operation runtime parameters (OperationId: %v)",
            operation->GetId());
    }

    void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Alerts_[alertType] = alert;
    }

    void AddGlobalWatcherRequester(TWatcherRequester requester)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        GlobalWatcherRequesters_.push_back(requester);
    }

    void AddGlobalWatcherHandler(TWatcherHandler handler)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        GlobalWatcherHandlers_.push_back(handler);
    }

    void AddGlobalWatcher(TWatcherRequester requester, TWatcherHandler handler, TDuration period)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CustomGlobalWatcherRecords_.push_back(TPeriodicExecutorRecord{std::move(requester), std::move(handler), period});
    }

    void AddOperationWatcherRequester(const TOperationPtr& operation, TWatcherRequester requester)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        auto* list = GetOrCreateWatcherList(operation);
        list->WatcherRequesters.push_back(requester);
    }

    void AddOperationWatcherHandler(const TOperationPtr& operation, TWatcherHandler handler)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EMasterConnectorState::Disconnected);

        auto* list = GetOrCreateWatcherList(operation);
        list->WatcherHandlers.push_back(handler);
    }

    void UpdateConfig(const TSchedulerConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Config_ = config;

        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->SetPeriod(Config_->OperationsUpdatePeriod);
        }
        if (WatchersExecutor_) {
            WatchersExecutor_->SetPeriod(Config_->WatchersUpdatePeriod);
        }
        if (AlertsExecutor_) {
            AlertsExecutor_->SetPeriod(Config_->AlertsUpdatePeriod);
        }

        ScheduleTestingDisconnect();
    }

    DEFINE_SIGNAL(void(), MasterConnecting);
    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterHandshake);
    DEFINE_SIGNAL(void(), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);

private:
    TSchedulerConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    TCancelableContextPtr CancelableContext_;
    TEnumIndexedVector<IInvokerPtr, EControlQueue> CancelableControlInvokers_;

    std::atomic<EMasterConnectorState> State_ = {EMasterConnectorState::Disconnected};
    std::atomic<TInstant> ConnectionTime_;

    ITransactionPtr LockTransaction_;

    TPeriodicExecutorPtr WatchersExecutor_;
    TPeriodicExecutorPtr AlertsExecutor_;

    struct TPeriodicExecutorRecord
    {
        TWatcherRequester Requester;
        TWatcherHandler Handler;
        TDuration Period;
    };

    std::vector<TWatcherRequester> GlobalWatcherRequesters_;
    std::vector<TWatcherHandler>   GlobalWatcherHandlers_;

    std::vector<TPeriodicExecutorRecord> CustomGlobalWatcherRecords_;
    std::vector<TPeriodicExecutorPtr> CustomGlobalWatcherExecutors_;

    TEnumIndexedVector<TError, ESchedulerAlertType> Alerts_;

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

    THashMap<TOperationId, TWatcherList> WatcherLists;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void ScheduleTestingDisconnect()
    {
        if (Config_->TestingOptions->EnableRandomMasterDisconnection) {
            TDelayedExecutor::Submit(
                BIND(&TImpl::RandomDisconnect, MakeStrong(this))
                    .Via(Bootstrap_->GetControlInvoker()),
                RandomDuration(Config_->TestingOptions->RandomMasterDisconnectionMaxBackoff));
        }
    }

    void RandomDisconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Config_->TestingOptions->EnableRandomMasterDisconnection) {
            DoDisconnect(TError("Disconnecting scheduler due to enabled random disconnection"));
        }
    }

    void StartConnecting(bool immediate)
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::DoStartConnecting, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()),
            immediate ? TDuration::Zero() : Config_->ConnectRetryBackoffTime);
    }

    void DoStartConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (State_ != EMasterConnectorState::Disconnected) {
            return;
        }
        State_ = EMasterConnectorState::Connecting;

        LOG_INFO("Connecting to master");

        YCHECK(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();

        for (auto queue : TEnumTraits<EControlQueue>::GetDomainValues()) {
            YCHECK(!CancelableControlInvokers_[queue]);
            CancelableControlInvokers_[queue] = CancelableContext_->CreateInvoker(
                Bootstrap_->GetControlInvoker(queue));
        }

        OperationNodesUpdateExecutor_ = New<TUpdateExecutor<TOperationId, TOperationNodeUpdate>>(
            GetCancelableControlInvoker(),
            BIND(&TImpl::UpdateOperationNode, Unretained(this)),
            BIND(&TImpl::IsOperationInFinishedState, Unretained(this)),
            BIND(&TImpl::OnOperationUpdateFailed, Unretained(this)),
            Config_->OperationsUpdatePeriod,
            Logger);

        WatchersExecutor_ = New<TPeriodicExecutor>(
            GetCancelableControlInvoker(),
            BIND(&TImpl::UpdateWatchers, MakeWeak(this)),
            Config_->WatchersUpdatePeriod,
            EPeriodicExecutorMode::Automatic);

        AlertsExecutor_ = New<TPeriodicExecutor>(
            GetCancelableControlInvoker(),
            BIND(&TImpl::UpdateAlerts, MakeWeak(this)),
            Config_->AlertsUpdatePeriod,
            EPeriodicExecutorMode::Automatic);

        for (const auto& record : CustomGlobalWatcherRecords_) {
            auto executor = New<TPeriodicExecutor>(
                GetCancelableControlInvoker(),
                BIND(&TImpl::ExecuteCustomWatcherUpdate, MakeWeak(this), record.Requester, record.Handler),
                record.Period,
                EPeriodicExecutorMode::Automatic);
            CustomGlobalWatcherExecutors_.push_back(executor);
        }

        auto pipeline = New<TRegistrationPipeline>(this);
        BIND(&TRegistrationPipeline::Run, pipeline)
            .AsyncVia(GetCancelableControlInvoker())
            .Run()
            .Subscribe(BIND(&TImpl::OnConnected, MakeStrong(this))
                .Via(GetCancelableControlInvoker()));
    }

    void OnConnected(const TError& error) noexcept
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ == EMasterConnectorState::Connecting);

        if (!error.IsOK()) {
            LOG_WARNING(error, "Error connecting to master");
            DoCleanup();
            StartConnecting(false);
            return;
        }

        TForbidContextSwitchGuard contextSwitchGuard;

        State_.store(EMasterConnectorState::Connected);
        ConnectionTime_.store(TInstant::Now());

        LOG_INFO("Master connected");

        LockTransaction_->SubscribeAborted(
            BIND(&TImpl::OnLockTransactionAborted, MakeWeak(this))
                .Via(GetCancelableControlInvoker()));

        StartPeriodicActivities();

        MasterConnected_.Fire();

        ScheduleTestingDisconnect();
    }

    void OnLockTransactionAborted()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Disconnect(TError("Lock transaction aborted"));
    }


    class TRegistrationPipeline
        : public TRefCounted
    {
    public:
        explicit TRegistrationPipeline(TIntrusivePtr<TImpl> owner)
            : Owner_(owner)
            , ServiceAddresses_(Owner_->Bootstrap_->GetLocalAddresses())
        { }

        void Run()
        {
            FireConnecting();
            RegisterInstance();
            StartLockTransaction();
            TakeLock();
            AssumeControl();
            UpdateGlobalWatchers();
            SyncClusterDirectory();
            ListOperations();
            RequestOperationAttributes();
            FireHandshake();
        }

    private:
        const TIntrusivePtr<TImpl> Owner_;
        const TAddressMap ServiceAddresses_;

        std::vector<TOperationId> OperationIds_;
        TMasterHandshakeResult Result_;

        void FireConnecting()
        {
            Owner_->MasterConnecting_.Fire();
        }

        // - Register scheduler instance.
        void RegisterInstance()
        {
            TObjectServiceProxy proxy(Owner_
                ->Bootstrap_
                ->GetMasterClient()
                ->GetMasterChannelOrThrow(EMasterChannelKind::Leader));
            auto batchReq = proxy.ExecuteBatch();
            auto path = "//sys/scheduler/instances/" + ToYPathLiteral(GetDefaultAddress(ServiceAddresses_));
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
                attributes->Set("remote_addresses", ServiceAddresses_);
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
            options.Timeout = Owner_->Config_->LockTransactionTimeout;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Scheduler lock at %v", GetDefaultAddress(ServiceAddresses_)));
            options.Attributes = std::move(attributes);

            auto client = Owner_->Bootstrap_->GetMasterClient();
            auto transactionOrError = WaitFor(Owner_->Bootstrap_->GetMasterClient()->StartTransaction(
                ETransactionType::Master,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError, "Error starting lock transaction");

            Owner_->LockTransaction_ = transactionOrError.Value();

            LOG_INFO("Lock transaction is %v", Owner_->LockTransaction_->GetId());
        }

        // - Take lock.
        void TakeLock()
        {
            auto result = WaitFor(Owner_->LockTransaction_->LockNode("//sys/scheduler/lock", ELockMode::Exclusive));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error taking scheduler lock");
        }

        // - Publish scheduler address.
        // - Update orchid address.
        void AssumeControl()
        {
            auto batchReq = Owner_->StartObjectBatchRequest();
            auto addresses = Owner_->Bootstrap_->GetLocalAddresses();
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
            WaitFor(Owner_
                ->Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetClusterDirectorySynchronizer()
                ->Sync())
                .ThrowOnError();
        }

        // - Request operations and their states.
        void ListOperations()
        {
            LOG_INFO("Started listing existing operations");

            static const std::vector<TString> attributeKeys = {
                "state"
            };

            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (int hash = 0x0; hash <= 0xFF; ++hash) {
                auto hashStr = Format("%02x", hash);
                auto req = TYPathProxy::List("//sys/operations/" + hashStr);
                ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                batchReq->AddRequest(req, "list_operations_" + hashStr);
            }

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

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

                    auto state = operationNode->Attributes().Get<EOperationState>("state");
                    if (IsOperationInProgress(state)) {
                        OperationIds_.push_back(id);
                        LOG_DEBUG("Found operation in Cypress (OperationId: %v, State: %v)",
                            id,
                            state);
                    }
                }
            }

            LOG_INFO("Finished listing existing operations");
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
                "spec",
                "owners",
                "authenticated_user",
                "start_time",
                "state",
                "events",
                "slot_index_per_pool_tree",
                "scheduling_options_per_pool_tree"
            };

            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            {
                LOG_INFO("Fetching attributes and secure vaults for unfinished operations (UnfinishedOperationCount: %v)",
                    OperationIds_.size());

                for (const auto& operationId : OperationIds_) {
                    // Keep stuff below in sync with #TryCreateOperationFromAttributes.

                    auto  operationAttributesPath = GetOperationPath(operationId) + "/@";
                    auto secureVaultPath = GetSecureVaultPath(operationId);

                    // Retrieve operation attributes.
                    {
                        auto req = TYPathProxy::Get(operationAttributesPath);
                        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                        batchReq->AddRequest(req, "get_op_attr_" + ToString(operationId));
                    }

                    // Retrieve operation completion transaction id.
                    {
                        auto req = TYPathProxy::Get(GetNewOperationPath(operationId) + "/@");
                        std::vector<TString> attributeKeys{
                            "output_completion_transaction_id",
                        };
                        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                        batchReq->AddRequest(req, "get_op_completion_tx_id_" + ToString(operationId));
                    }

                    // Retrieve secure vault.
                    {
                        auto req = TYPathProxy::Get(secureVaultPath);
                        batchReq->AddRequest(req, "get_op_secure_vault_" + ToString(operationId));
                    }
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError);
            const auto& batchRsp = batchRspOrError.Value();

            {
                for (const auto& operationId : OperationIds_) {
                    auto attributesRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                        "get_op_attr_" + ToString(operationId))
                        .ValueOrThrow();

                    auto completionTxIdRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                        "get_op_completion_tx_id_" + ToString(operationId))
                        .ValueOrThrow();

                    auto secureVaultRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                        "get_op_secure_vault_" + ToString(operationId));

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
                                operationId,
                                secureVaultNode->GetType(),
                                ENodeType::Map);
                            // TODO(max42): (YT-5651) Do not just ignore such a situation!
                        }
                    } else if (secureVaultRspOrError.GetCode() != NYTree::EErrorCode::ResolveError) {
                        THROW_ERROR_EXCEPTION("Error while attempting to fetch the secure vault of operation (OperationId: %v)",
                            operationId)
                            << secureVaultRspOrError;
                    }

                    auto operation = TryCreateOperationFromAttributes(
                        operationId,
                        *attributesNode,
                        secureVault);
                    if (operation) {
                        Result_.Operations.push_back(operation);
                    }
                }
            }
        }

        TOperationPtr TryCreateOperationFromAttributes(
            const TOperationId& operationId,
            const IAttributeDictionary& attributes,
            const IMapNodePtr& secureVault)
        {
            auto specNode = attributes.Get<INodePtr>("spec")->AsMap();

            TOperationSpecBasePtr spec;
            try {
                spec = ConvertTo<TOperationSpecBasePtr>(specNode);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error parsing operation spec (OperationId: %v)",
                    operationId);
                return nullptr;
            }

            auto runtimeParams = New<TOperationRuntimeParameters>();
            runtimeParams->Owners = attributes.Get<std::vector<TString>>("owners", spec->Owners);
            // Merge initial scheduling options and scheduling options set by user while operation was running.
            auto schedulingOptions = attributes.Find<INodePtr>("scheduling_options_per_pool_tree");
            if (schedulingOptions) {
                Deserialize(runtimeParams->SchedulingOptionsPerPoolTree, schedulingOptions);
            }

            auto operation = New<TOperation>(
                operationId,
                attributes.Get<EOperationType>("operation_type"),
                attributes.Get<TMutationId>("mutation_id"),
                attributes.Get<TTransactionId>("user_transaction_id"),
                specNode,
                secureVault,
                runtimeParams,
                attributes.Get<TString>("authenticated_user"),
                attributes.Get<TInstant>("start_time"),
                Owner_->Bootstrap_->GetControlInvoker(),
                attributes.Get<EOperationState>("state"),
                attributes.Get<std::vector<TOperationEvent>>("events", {}));

            auto slotIndexMap = attributes.Find<THashMap<TString, int>>("slot_index_per_pool_tree");
            if (slotIndexMap) {
                for (const auto& pair : *slotIndexMap) {
                    operation->SetSlotIndex(pair.first, pair.second);
                }
            }

            return operation;
        }

        void UpdateGlobalWatchers()
        {
            auto batchReq = Owner_->StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (auto requester : Owner_->GlobalWatcherRequesters_) {
                requester.Run(batchReq);
            }
            for (const auto& record : Owner_->CustomGlobalWatcherRecords_) {
                record.Requester.Run(batchReq);
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            auto watcherResponses = batchRspOrError.ValueOrThrow();

            for (auto handler : Owner_->GlobalWatcherHandlers_) {
                handler.Run(watcherResponses);
            }
            for (const auto& record : Owner_->CustomGlobalWatcherRecords_) {
                record.Handler.Run(watcherResponses);
            }
        }

        void FireHandshake()
        {
            Owner_->MasterHandshake_.Fire(Result_);
        }
    };

    void DoFetchOperationRevivalDescriptors(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Fetching operation revival descriptors (OperationCount: %v)",
            operations.size());

        {
            static const std::vector<TString> attributeKeys = {
                "async_scheduler_transaction_id",
                "input_transaction_id",
                "output_transaction_id",
                "debug_transaction_id",
                "output_completion_transaction_id",
                "debug_completion_transaction_id",
            };

            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);

            for (const auto& operation : operations) {
                const auto& operationId = operation->GetId();
                auto operationAttributesPath = GetOperationPath(operationId) + "/@";
                auto secureVaultPath = GetSecureVaultPath(operationId);

                // Retrieve operation attributes.
                {
                    auto req = TYPathProxy::Get(operationAttributesPath);
                    ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                    batchReq->AddRequest(req, "get_op_attr_" + ToString(operationId));
                }

                // Retrieve operation completion transaction id.
                {
                    auto req = TYPathProxy::Get(GetNewOperationPath(operationId) + "/@");
                    std::vector<TString> attributeKeys{
                        "output_completion_transaction_id",
                    };
                    ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                    batchReq->AddRequest(req, "get_op_completion_tx_id_" + ToString(operationId));
                }
            }

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

            for (const auto& operation : operations) {
                const auto& operationId = operation->GetId();

                auto attributesRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                    "get_op_attr_" + ToString(operationId))
                    .ValueOrThrow();

                auto completionTxIdRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(
                    "get_op_completion_tx_id_" + ToString(operationId))
                    .ValueOrThrow();

                auto attributes = ConvertToAttributes(TYsonString(attributesRsp->value()));
                auto completionTxIdAttribute = ConvertToAttributes(TYsonString(completionTxIdRsp->value()));
                attributes->MergeFrom(*completionTxIdAttribute);

                auto attachTransaction = [&] (const TTransactionId& transactionId, bool ping, const TString& name = TString()) -> ITransactionPtr {
                    if (!transactionId) {
                        if (name) {
                            LOG_DEBUG("Missing %v transaction (OperationId: %v, TransactionId: %v)",
                                name,
                                operationId,
                                transactionId);
                        }
                        return nullptr;
                    }
                    try {
                        auto connection = NControllerAgent::GetRemoteConnectionOrThrow(
                            Bootstrap_->GetMasterClient()->GetNativeConnection(),
                            CellTagFromId(transactionId));
                        auto client = connection->CreateNativeClient(TClientOptions(SchedulerUserName));

                        TTransactionAttachOptions options;
                        options.Ping = ping;
                        options.PingAncestors = false;
                        return client->AttachTransaction(transactionId, options);
                    } catch (const std::exception& ex) {
                        LOG_WARNING(ex, "Error attaching operation transaction (OperationId: %v, TransactionId: %v)",
                            operationId,
                            transactionId);
                        return nullptr;
                    }
                };

                TOperationRevivalDescriptor revivalDescriptor;
                revivalDescriptor.AsyncTransaction = attachTransaction(
                    attributes->Get<TTransactionId>("async_scheduler_transaction_id", NullTransactionId),
                    true,
                    "async");
                revivalDescriptor.InputTransaction = attachTransaction(
                    attributes->Get<TTransactionId>("input_transaction_id", NullTransactionId),
                    true,
                    "input");
                revivalDescriptor.OutputTransaction = attachTransaction(
                    attributes->Get<TTransactionId>("output_transaction_id", NullTransactionId),
                    true,
                    "output");
                revivalDescriptor.OutputCompletionTransaction = attachTransaction(
                    attributes->Get<TTransactionId>("output_completion_transaction_id", NullTransactionId),
                    true,
                    "output completion");
                revivalDescriptor.DebugTransaction = attachTransaction(
                    attributes->Get<TTransactionId>("debug_transaction_id", NullTransactionId),
                    true,
                    "debug");
                revivalDescriptor.DebugCompletionTransaction = attachTransaction(
                    attributes->Get<TTransactionId>("debug_completion_transaction_id", NullTransactionId),
                    true,
                    "debug completion");

                const auto& userTransactionId = operation->GetUserTransactionId();
                auto userTransaction = attachTransaction(userTransactionId, false);

                revivalDescriptor.UserTransactionAborted = !userTransaction && userTransactionId;

                for (const auto& event : operation->Events()) {
                    if (event.State == EOperationState::Aborting) {
                        revivalDescriptor.OperationAborting = true;
                        break;
                    }
                }

                operation->RevivalDescriptor() = std::move(revivalDescriptor);
            }
        }

        LOG_INFO("Fetching committed flags (OperationCount: %v)",
            operations.size());

        {
            std::vector<TOperationPtr> operationsWithOutputTransaction;

            auto getBatchKey = [] (const TOperationPtr& operation) {
                return "get_op_committed_attr_" + ToString(operation->GetId());
            };

            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);

            for (const auto& operation : operations) {
                auto& revivalDescriptor = *operation->RevivalDescriptor();
                if (!revivalDescriptor.OutputTransaction) {
                    continue;
                }

                operationsWithOutputTransaction.push_back(operation);

                for (auto transactionId : {
                    revivalDescriptor.OutputTransaction->GetId(),
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

            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();

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


    TObjectServiceProxy::TReqExecuteBatchPtr StartObjectBatchRequest(
        EMasterChannelKind channelKind = EMasterChannelKind::Leader,
        TCellTag cellTag = PrimaryMasterCellTag)
    {
        TObjectServiceProxy proxy(Bootstrap_
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(channelKind, cellTag));
        auto batchReq = proxy.ExecuteBatch();
        YCHECK(LockTransaction_);
        auto* prerequisitesExt = batchReq->Header().MutableExtension(TPrerequisitesExt::prerequisites_ext);
        auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
        ToProto(prerequisiteTransaction->mutable_transaction_id(), LockTransaction_->GetId());
        return batchReq;
    }


    void DoCleanup()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LockTransaction_.Reset();

        ClearWatcherLists();

        StopPeriodicActivities();

        if (CancelableContext_) {
            CancelableContext_->Cancel();
            CancelableContext_.Reset();
        }

        std::fill(CancelableControlInvokers_.begin(), CancelableControlInvokers_.end(), nullptr);

        State_.store(EMasterConnectorState::Disconnected);
        ConnectionTime_.store(TInstant::Zero());
    }

    void DoDisconnect(const TError& error) noexcept
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TForbidContextSwitchGuard contextSwitchGuard;

        if (State_ == EMasterConnectorState::Connected) {
            LOG_WARNING(error, "Disconnecting master");
            MasterDisconnected_.Fire();
            LOG_WARNING("Master disconnected");
        }

        DoCleanup();
        StartConnecting(true);
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



    void StartPeriodicActivities()
    {
        OperationNodesUpdateExecutor_->Start();

        WatchersExecutor_->Start();

        AlertsExecutor_->Start();

        for (const auto& executor : CustomGlobalWatcherExecutors_) {
            executor->Start();
        }
    }

    void StopPeriodicActivities()
    {
        if (OperationNodesUpdateExecutor_) {
            OperationNodesUpdateExecutor_->Stop();
            OperationNodesUpdateExecutor_.Reset();
        }

        if (WatchersExecutor_) {
            WatchersExecutor_->Stop();
            WatchersExecutor_.Reset();
        }

        if (AlertsExecutor_) {
            AlertsExecutor_->Stop();
            AlertsExecutor_.Reset();
        }

        for (const auto& executor : CustomGlobalWatcherExecutors_) {
            executor->Stop();
        }
        CustomGlobalWatcherExecutors_.clear();
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

        Disconnect(TError("Failed to update operation node") << error);
    }

    void DoUpdateOperationNode(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            operation->SetShouldFlush(false);

            auto batchReq = StartObjectBatchRequest();
            GenerateMutationId(batchReq);

            auto paths = GetCompatibilityOperationPaths(operation->GetId());
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
                    req->set_value(ConvertToYsonString(operation->Events()).GetData());
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
            .AsyncVia(GetCancelableControlInvoker());
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

    void OnInitializedOperationNodeUpdated(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();
        auto error = GetCumulativeError(batchRspOrError);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error updating initialized operation node %v",
            operationId);

        LOG_INFO("Initialized operation node updated (OperationId: %v)",
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
        YCHECK(State_ == EMasterConnectorState::Connected);

        LOG_DEBUG("Updating watchers");

        // Global watchers.
        {
            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower);
            for (auto requester : GlobalWatcherRequesters_) {
                requester.Run(batchReq);
            }
            batchReq->Invoke().Subscribe(
                BIND(&TImpl::OnGlobalWatchersUpdated, MakeStrong(this))
                    .Via(GetCancelableControlInvoker()));
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
                    .Via(GetCancelableControlInvoker()));
        }
    }

    void OnGlobalWatchersUpdated(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ == EMasterConnectorState::Connected);

        if (!batchRspOrError.IsOK()) {
            LOG_ERROR(batchRspOrError, "Error updating global watchers");
            return;
        }

        const auto& batchRsp = batchRspOrError.Value();
        for (auto handler : GlobalWatcherHandlers_) {
            handler.Run(batchRsp);
        }

        LOG_DEBUG("Global watchers updated");
    }

    void OnOperationWatchersUpdated(
        const TOperationPtr& operation,
        const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ == EMasterConnectorState::Connected);

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

        LOG_DEBUG("Operation watchers updated (OperationId: %v)",
            operation->GetId());
    }


    void UpdateAlerts()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ == EMasterConnectorState::Connected);

        std::vector<TError> alerts;
        for (auto alertType : TEnumTraits<ESchedulerAlertType>::GetDomainValues()) {
            const auto& alert = Alerts_[alertType];
            if (!alert.IsOK()) {
                alerts.push_back(alert);
            }
        }

        TObjectServiceProxy proxy(Bootstrap_
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
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TMasterConnector::~TMasterConnector() = default;

void TMasterConnector::Start()
{
    Impl_->Start();
}

EMasterConnectorState TMasterConnector::GetState() const
{
    return Impl_->GetState();
}

TInstant TMasterConnector::GetConnectionTime() const
{
    return Impl_->GetConnectionTime();
}

void TMasterConnector::Disconnect(const TError& error)
{
    Impl_->Disconnect(error);
}

const IInvokerPtr& TMasterConnector::GetCancelableControlInvoker(EControlQueue queue) const
{
    return Impl_->GetCancelableControlInvoker(queue);
}

void TMasterConnector::StartOperationNodeUpdates(const TOperationPtr& operation)
{
    Impl_->StartOperationNodeUpdates(operation);
}

TFuture<void> TMasterConnector::CreateOperationNode(const TOperationPtr& operation)
{
    return Impl_->CreateOperationNode(operation);
}

TFuture<void> TMasterConnector::UpdateInitializedOperationNode(const TOperationPtr& operation)
{
    return Impl_->UpdateInitializedOperationNode(operation);
}

TFuture<void> TMasterConnector::FlushOperationNode(const TOperationPtr& operation)
{
    return Impl_->FlushOperationNode(operation);
}

TFuture<void> TMasterConnector::FetchOperationRevivalDescriptors(const std::vector<TOperationPtr>& operations)
{
    return Impl_->FetchOperationRevivalDescriptors(operations);
}

void TMasterConnector::AttachJobContext(
    const TYPath& path,
    const TChunkId& chunkId,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TString& user)
{
    return Impl_->AttachJobContext(path, chunkId, operationId, jobId, user);
}

TFuture<void> TMasterConnector::FlushOperationRuntimeParameters(
    TOperationPtr operation,
    const TOperationRuntimeParametersPtr& params)
{
    return Impl_->FlushOperationRuntimeParameters(operation, params);
}

void TMasterConnector::SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
{
    Impl_->SetSchedulerAlert(alertType, alert);
}

void TMasterConnector::UpdateConfig(const TSchedulerConfigPtr& config)
{
    Impl_->UpdateConfig(config);
}

void TMasterConnector::AddGlobalWatcherRequester(TWatcherRequester requester)
{
    Impl_->AddGlobalWatcherRequester(requester);
}

void TMasterConnector::AddGlobalWatcherHandler(TWatcherHandler handler)
{
    Impl_->AddGlobalWatcherHandler(handler);
}

void TMasterConnector::AddGlobalWatcher(TWatcherRequester requester, TWatcherHandler handler, TDuration period)
{
    Impl_->AddGlobalWatcher(std::move(requester), std::move(handler), period);
}

void TMasterConnector::AddOperationWatcherRequester(const TOperationPtr& operation, TWatcherRequester requester)
{
    Impl_->AddOperationWatcherRequester(operation, requester);
}

void TMasterConnector::AddOperationWatcherHandler(const TOperationPtr& operation, TWatcherHandler handler)
{
    Impl_->AddOperationWatcherHandler(operation, handler);
}

DELEGATE_SIGNAL(TMasterConnector, void(), MasterConnecting, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(const TMasterHandshakeResult& result), MasterHandshake, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterConnected, *Impl_);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterDisconnected, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

