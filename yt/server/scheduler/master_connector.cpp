#include "master_connector.h"
#include "private.h"
#include "helpers.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "serialize.h"
#include "snapshot_builder.h"
#include "snapshot_downloader.h"
#include "config.h"

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
        , ClusterDirectory(Bootstrap->GetClusterDirectory())
        , ClusterDirectorySynchronizer(New<TClusterDirectorySynchronizer>(
            Config->ClusterDirectorySynchronizer,
            Bootstrap->GetMasterClient()->GetConnection(),
            ClusterDirectory))
    { }

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

    IInvokerPtr GetCancelableControlInvoker() const
    {
        return CancelableControlInvoker;
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

        auto owners = operation->GetOwners();
        owners.push_back(operation->GetAuthenticatedUser());

        {
            auto controller = operation->GetController();

            auto req = TYPathProxy::Set(GetOperationPath(operationId));
            req->set_value(BuildYsonStringFluently()
                .BeginAttributes()
                    .Do(BIND(&ISchedulerStrategy::BuildOperationAttributes, strategy, operationId))
                    .Do(BIND(&BuildInitializingOperationAttributes, operation))
                    .Item("brief_spec").BeginMap()
                        .Do(BIND(&IOperationController::BuildBriefSpec, operation->GetController()))
                        .Do(BIND(&ISchedulerStrategy::BuildBriefSpec, strategy, operationId))
                    .EndMap()
                    .Item("progress").BeginMap().EndMap()
                    .Item("brief_progress").BeginMap().EndMap()
                    .Item("opaque").Value("true")
                    .Item("acl").BeginList()
                        .Item().BeginMap()
                            .Item("action").Value(ESecurityAction::Allow)
                            .Item("subjects").Value(owners)
                            .Item("permissions").BeginList()
                                .Item().Value(EPermission::Write)
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndAttributes()
                .BeginMap()
                    .Item("jobs").BeginAttributes()
                        .Item("opaque").Value("true")
                    .EndAttributes()
                    .BeginMap().EndMap()
                .EndMap()
                .GetData());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        if (operation->GetSecureVault()) {
            // Create secure vault.
            auto req = TCypressYPathProxy::Create(GetSecureVaultPath(operationId));
            req->set_type(static_cast<int>(EObjectType::Document));

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("inherit_acl", false);
            attributes->Set("value", operation->GetSecureVault());
            attributes->Set("acl", BuildYsonStringFluently()
                .BeginList()
                    .Item().BeginMap()
                        .Item("action").Value(ESecurityAction::Allow)
                        .Item("subjects").Value(owners)
                        .Item("permissions").BeginList()
                            .Item().Value(EPermission::Read)
                        .EndList()
                    .EndMap()
                .EndList());
            ToProto(req->mutable_node_attributes(), *attributes);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
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
                .Do(BIND(&BuildRunningOperationAttributes, operation))
                .Item("progress").BeginMap().EndMap()
                .Item("brief_progress").BeginMap().EndMap()
            .EndMap());

        for (const auto& key : attributes->List()) {
            auto req = TYPathProxy::Set(GetOperationPath(operationId) + "/@" + ToYPathLiteral(key));
            req->set_value(attributes->GetYson(key).GetData());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
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

        auto id = operation->GetId();
        LOG_INFO("Flushing operation node (OperationId: %v)",
            id);

        auto* list = FindUpdateList(id);
        if (!list) {
            LOG_INFO("Operation node is not registered, omitting flush (OperationId: %v)",
                id);
            return VoidFuture;
        }

        return UpdateOperationNode(list).Apply(
            BIND(&TImpl::OnOperationNodeFlushed, MakeStrong(this), operation)
                .Via(CancelableControlInvoker));
    }


    void CreateJobNode(const TCreateJobNodeRequest& createJobNodeRequest)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Creating job node (OperationId: %v, JobId: %v, StderrChunkId: %v, FailContextChunkId: %v)",
            createJobNodeRequest.OperationId,
            createJobNodeRequest.JobId,
            createJobNodeRequest.StderrChunkId,
            createJobNodeRequest.FailContextChunkId);

        auto* list = GetUpdateList(createJobNodeRequest.OperationId);
        list->JobRequests.push_back(createJobNodeRequest);
    }

    void RegisterAlert(EAlertType alertType, const TError& alert)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Alerts[alertType] = alert;
    }

    void UnregisterAlert(EAlertType alertType)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Alerts[alertType] = TError();
    }

    TFuture<void> AttachToLivePreview(
        const TOperationId& operationId,
        const TTransactionId& transactionId,
        const TNodeId& tableId,
        const std::vector<TChunkTreeId>& childIds)
    {
        return BIND(&TImpl::DoAttachToLivePreview, MakeStrong(this))
            .AsyncVia(CancelableControlInvoker)
            .Run(operationId, transactionId, tableId, childIds);
    }

    TFuture<TOperationSnapshot> DownloadSnapshot(const TOperationId& operationId)
    {
        if (!Config->EnableSnapshotLoading) {
            return MakeFuture<TOperationSnapshot>(TError("Snapshot loading is disabled in configuration"));
        }

        return BIND(&TImpl::DoDownloadSnapshot, MakeStrong(this), operationId)
            .AsyncVia(CancelableControlInvoker)
            .Run();
    }

    TFuture<void> RemoveSnapshot(const TOperationId& operationId)
    {
        return BIND(&TImpl::DoRemoveSnapshot, MakeStrong(this), operationId)
            .AsyncVia(CancelableControlInvoker)
            .Run();
    }

    void AddGlobalWatcherRequester(TWatcherRequester requester)
    {
        GlobalWatcherRequesters.push_back(requester);
    }

    void AddGlobalWatcherHandler(TWatcherHandler handler)
    {
        GlobalWatcherHandlers.push_back(handler);
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

    void AttachJobContext(
        const TYPath& path,
        const TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(chunkId);

        try {
            TJobFile file{
                jobId,
                path,
                chunkId,
                "input_context"
            };
            SaveJobFiles(operationId, { file });
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error saving input context for job %v into %v", jobId, path)
                << ex;
        }
    }

    void UpdateConfig(const TSchedulerConfigPtr& config)
    {
        Config = config;
        OnConfigUpdated();
    }

    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);

    DEFINE_SIGNAL(void(TOperationPtr operation), UserTransactionAborted);
    DEFINE_SIGNAL(void(TOperationPtr operation), SchedulerTransactionAborted);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* const Bootstrap;

    NHiveClient::TClusterDirectoryPtr ClusterDirectory;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;

    bool Connected = false;

    ITransactionPtr LockTransaction;

    TPeriodicExecutorPtr TransactionRefreshExecutor;
    TPeriodicExecutorPtr OperationNodesUpdateExecutor;
    TPeriodicExecutorPtr WatchersExecutor;
    TPeriodicExecutorPtr SnapshotExecutor;
    TPeriodicExecutorPtr AlertsExecutor;
    TClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer;

    std::vector<TWatcherRequester> GlobalWatcherRequesters;
    std::vector<TWatcherHandler>   GlobalWatcherHandlers;

    TEnumIndexedVector<TError, EAlertType> Alerts;

    struct TLivePreviewRequest
    {
        TChunkListId TableId;
        TChunkTreeId ChildId;
    };

    struct TUpdateList
    {
        explicit TUpdateList(TOperationPtr operation)
            : Operation(operation)
        { }

        TOperationPtr Operation;
        TTransactionId TransactionId;
        std::vector<TCreateJobNodeRequest> JobRequests;
        std::vector<TLivePreviewRequest> LivePreviewRequests;
        TFuture<void> LastUpdateFuture = VoidFuture;
    };

    yhash<TOperationId, TUpdateList> UpdateLists;

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

    void OnConfigUpdated()
    {
        ScheduleTestingDisconnection();
    }

    void ScheduleTestingDisconnection()
    {
        if (Config->TestingOptions->EnableRandomMasterDisconnection) {
            TDelayedExecutor::Submit(
                BIND(&TImpl::Disconnect, MakeStrong(this))
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
        Connected = true;

        CancelableContext = New<TCancelableContext>();
        CancelableControlInvoker = CancelableContext->CreateInvoker(Bootstrap->GetControlInvoker());

        const auto& result = resultOrError.Value();
        for (auto operationReport : result.OperationReports) {
            CreateUpdateList(operationReport.Operation);
        }

        LockTransaction->SubscribeAborted(
            BIND(&TImpl::OnLockTransactionAborted, MakeWeak(this))
                .Via(CancelableControlInvoker));

        StartPeriodicActivities();

        MasterConnected_.Fire(result);

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
            UpdateClusterDirectory();
            ListOperations();
            RequestOperationAttributes();
            return Result;
        }

    private:
        const TIntrusivePtr<TImpl> Owner;

        const TAddressMap ServiceAddresses;
        std::vector<TOperationId> OperationIds;
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

        void UpdateClusterDirectory()
        {
            WaitFor(Owner->ClusterDirectorySynchronizer->Sync())
                .ThrowOnError();
        }

        // - Request operations and their states.
        void ListOperations()
        {
            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);
            {
                auto req = TYPathProxy::List("//sys/operations");
                std::vector<TString> attributeKeys{
                    "state"
                };
                ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                batchReq->AddRequest(req, "list_operations");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>("list_operations").Value();
                auto operationsListNode = ConvertToNode(TYsonString(rsp->value()));
                auto operationsList = operationsListNode->AsList();
                LOG_INFO("Operations list received, %v operations total",
                    operationsList->GetChildCount());
                OperationIds.clear();
                for (auto operationNode : operationsList->GetChildren()) {
                    auto id = TOperationId::FromString(operationNode->GetValue<TString>());
                    auto state = operationNode->Attributes().Get<EOperationState>("state");
                    if (IsOperationInProgress(state)) {
                        OperationIds.push_back(id);
                    }
                }
            }
        }

        // - Request attributes for unfinished operations.
        // - Recreate operation instance from fetched data.
        void RequestOperationAttributes()
        {
            auto batchReq = Owner->StartObjectBatchRequest(EMasterChannelKind::Follower);
            {
                LOG_INFO("Fetching attributes and secure vaults for %v unfinished operations",
                    OperationIds.size());
                for (const auto& operationId : OperationIds) {
                    // Keep stuff below in sync with CreateOperationFromAttributes.
                    // Retrieve operation attributes.
                    {
                        auto req = TYPathProxy::Get(GetOperationPath(operationId) + "/@");
                        std::vector<TString> attributeKeys{
                            "operation_type",
                            "mutation_id",
                            "user_transaction_id",
                            "sync_scheduler_transaction_id",
                            "async_scheduler_transaction_id",
                            "input_transaction_id",
                            "output_transaction_id",
                            "debug_output_transaction_id",
                            "spec",
                            "authenticated_user",
                            "start_time",
                            "state",
                            "suspended",
                            "events",
                            "slot_index"
                        };
                        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                        batchReq->AddRequest(req, "get_op_attr");
                    }
                    // Retrieve secure vault.
                    {
                        auto req = TYPathProxy::Get(GetSecureVaultPath(operationId));
                        batchReq->AddRequest(req, "get_op_secure_vault");
                    }
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError, "get_op_attr"));
            const auto& batchRsp = batchRspOrError.Value();

            {
                auto attributesRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_op_attr");
                YCHECK(attributesRsps.size() == OperationIds.size());

                auto secureVaultRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_op_secure_vault");
                YCHECK(secureVaultRsps.size() == OperationIds.size());

                for (int index = 0; index < static_cast<int>(OperationIds.size()); ++index) {
                    const auto& operationId = OperationIds[index];
                    auto attributesRsp = attributesRsps[index].Value();
                    auto attributesNode = ConvertToAttributes(TYsonString(attributesRsp->value()));
                    auto secureVaultRspOrError = secureVaultRsps[index];
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
                        THROW_ERROR_EXCEPTION("Error while attempting to fetch the secure vault of operation %v", operationId)
                            << secureVaultRspOrError;
                    }
                    auto operationReport = Owner->CreateOperationFromAttributes(operationId, *attributesNode, std::move(secureVault));
                    if (operationReport.Operation) {
                        Result.OperationReports.push_back(operationReport);
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

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            auto watcherResponses = batchRspOrError.ValueOrThrow();

            for (auto handler : Owner->GlobalWatcherHandlers) {
                handler.Run(watcherResponses);
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

    TChunkServiceProxy::TReqExecuteBatchPtr StartChunkBatchRequest(TCellTag cellTag = PrimaryMasterCellTag)
    {
        TChunkServiceProxy proxy(Bootstrap
            ->GetMasterClient()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag));
        return proxy.ExecuteBatch();
    }


    void DoDisconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Connected)
            return;

        LOG_WARNING("Master disconnected");

        Connected = false;

        LockTransaction.Reset();

        ClearUpdateLists();
        ClearWatcherLists();

        StopPeriodicActivities();

        CancelableContext->Cancel();

        MasterDisconnected_.Fire();

        StartConnecting();
    }


    TOperationReport CreateOperationFromAttributes(
        const TOperationId& operationId,
        const IAttributeDictionary& attributes,
        IMapNodePtr secureVault)
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
                auto connection = GetConnectionOrThrow(CellTagFromId(transactionId));
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

        result.ControllerTransactions = New<TControllerTransactions>();
        result.ControllerTransactions->Sync = attachTransaction(
            attributes.Get<TTransactionId>("sync_scheduler_transaction_id"),
            true,
            "sync transaction");
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

        result.IsCommitted = attributes.Get<bool>("committed", false);

        // COMPAT(ermolovd). We use NullTransactionId as default value for the transition period.
        // Once all clusters are updated to version that creates debug_output transaction
        // this default value can be removed as in other transactions above.
        result.ControllerTransactions->DebugOutput = attachTransaction(
            attributes.Get<TTransactionId>("debug_output_transaction_id", NullTransactionId),
            true,
            "debug output transaction");

        auto spec = attributes.Get<INodePtr>("spec")->AsMap();
        TOperationSpecBasePtr operationSpec;
        try {
            operationSpec = ConvertTo<TOperationSpecBasePtr>(spec);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing operation spec (OperationId: %v)",
                operationId);
            return TOperationReport();
        }

        result.Operation = New<TOperation>(
            operationId,
            attributes.Get<EOperationType>("operation_type"),
            attributes.Get<TMutationId>("mutation_id"),
            userTransaction,
            spec,
            attributes.Get<TString>("authenticated_user"),
            operationSpec->Owners,
            attributes.Get<TInstant>("start_time"),
            attributes.Get<EOperationState>("state"),
            attributes.Get<bool>("suspended"),
            attributes.Get<std::vector<TOperationEvent>>("events", {}),
            attributes.Get<int>("slot_index", -1));

        result.Operation->SetSecureVault(std::move(secureVault));

        result.UserTransactionAborted = !userTransaction && userTransactionId;

        return result;
    }


    void StartPeriodicActivities()
    {
        TransactionRefreshExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::RefreshTransactions, MakeWeak(this)),
            Config->TransactionsRefreshPeriod,
            EPeriodicExecutorMode::Automatic);
        TransactionRefreshExecutor->Start();

        OperationNodesUpdateExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateOperationNodes, MakeWeak(this)),
            Config->OperationsUpdatePeriod,
            EPeriodicExecutorMode::Automatic);
        OperationNodesUpdateExecutor->Start();

        WatchersExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateWatchers, MakeWeak(this)),
            Config->WatchersUpdatePeriod,
            EPeriodicExecutorMode::Automatic);
        WatchersExecutor->Start();

        ClusterDirectorySynchronizer->Start();

        SnapshotExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::BuildSnapshot, MakeWeak(this)),
            Config->SnapshotPeriod,
            EPeriodicExecutorMode::Automatic);
        SnapshotExecutor->Start();

        AlertsExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateAlerts, MakeWeak(this)),
            Config->AlertsUpdatePeriod,
            EPeriodicExecutorMode::Automatic);
        AlertsExecutor->Start();
    }

    void StopPeriodicActivities()
    {
        if (TransactionRefreshExecutor) {
            TransactionRefreshExecutor->Stop();
            TransactionRefreshExecutor.Reset();
        }

        if (OperationNodesUpdateExecutor) {
            OperationNodesUpdateExecutor->Stop();
            OperationNodesUpdateExecutor.Reset();
        }

        if (WatchersExecutor) {
            WatchersExecutor->Stop();
            WatchersExecutor.Reset();
        }

        ClusterDirectorySynchronizer->Stop();

        if (SnapshotExecutor) {
            SnapshotExecutor->Stop();
            SnapshotExecutor.Reset();
        }

        if (AlertsExecutor) {
            AlertsExecutor->Stop();
            AlertsExecutor.Reset();
        }
    }


    void RefreshTransactions()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        // Collect all transactions that are used by currently running operations.
        yhash_set<TTransactionId> watchSet;
        auto watchTransaction = [&] (ITransactionPtr transaction) {
            if (transaction) {
                watchSet.insert(transaction->GetId());
            }
        };

        auto operations = Bootstrap->GetScheduler()->GetOperations();
        for (auto operation : operations) {
            auto controller = operation->GetController();
            if (controller) {
                for (const auto& transaction : controller->GetTransactions()) {
                    watchTransaction(transaction);
                }
            }
            watchTransaction(operation->GetUserTransaction());
        }

        yhash<TCellTag, TObjectServiceProxy::TReqExecuteBatchPtr> batchReqs;

        for (const auto& id : watchSet) {
            auto cellTag = CellTagFromId(id);
            if (batchReqs.find(cellTag) == batchReqs.end()) {
                auto connection = FindConnection(cellTag);
                if (!connection) {
                    continue;
                }
                auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
                auto authenticatedChannel = CreateAuthenticatedChannel(channel, SchedulerUserName);
                TObjectServiceProxy proxy(authenticatedChannel);
                batchReqs[cellTag] = proxy.ExecuteBatch();
            }

            auto checkReq = TObjectYPathProxy::GetBasicAttributes(FromObjectId(id));
            batchReqs[cellTag]->AddRequest(checkReq, "check_tx_" + ToString(id));
        }

        LOG_INFO("Refreshing transactions");

        yhash<TCellTag, NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> batchRsps;

        for (const auto& pair : batchReqs) {
            auto cellTag = pair.first;
            const auto& batchReq = pair.second;
            auto batchRspOrError = WaitFor(batchReq->Invoke(), CancelableControlInvoker);
            if (batchRspOrError.IsOK()) {
                batchRsps[cellTag] = batchRspOrError.Value();
            } else {
                LOG_ERROR(batchRspOrError, "Error refreshing transactions (CellTag: %v)",
                    cellTag);
            }
        }

        yhash_set<TTransactionId> deadTransactionIds;

        for (const auto& id : watchSet) {
            auto cellTag = CellTagFromId(id);
            auto it = batchRsps.find(cellTag);
            if (it != batchRsps.end()) {
                const auto& batchRsp = it->second;
                auto rspOrError = batchRsp->GetResponse("check_tx_" + ToString(id));
                if (!rspOrError.IsOK()) {
                    deadTransactionIds.insert(id);
                }
            }
        }

        LOG_INFO("Transactions refreshed");

        auto isTransactionAlive = [&] (TOperationPtr operation, ITransactionPtr transaction) -> bool {
            if (!transaction) {
                return true;
            }

            if (deadTransactionIds.find(transaction->GetId()) == deadTransactionIds.end()) {
                return true;
            }

            return false;
        };

        auto isUserTransactionAlive = [&] (TOperationPtr operation, ITransactionPtr transaction) -> bool {
            if (isTransactionAlive(operation, transaction)) {
                return true;
            }

            LOG_INFO("Expired user transaction found (OperationId: %v, TransactionId: %v)",
                operation->GetId(),
                transaction->GetId());
            return false;
        };

        auto isSchedulerTransactionAlive = [&] (TOperationPtr operation, ITransactionPtr transaction) -> bool {
            if (isTransactionAlive(operation, transaction)) {
                return true;
            }

            LOG_INFO("Expired scheduler transaction found (OperationId: %v, TransactionId: %v)",
                operation->GetId(),
                transaction->GetId());
            return false;
        };

        // Check every operation's transactions and raise appropriate notifications.
        for (auto operation : operations) {
            if (!isUserTransactionAlive(operation, operation->GetUserTransaction())) {
                UserTransactionAborted_.Fire(operation);
                continue;
            }

            auto controller = operation->GetController();
            if (controller) {
                for (const auto& transaction : controller->GetTransactions()) {
                    if (!isSchedulerTransactionAlive(operation, transaction)) {
                        SchedulerTransactionAborted_.Fire(operation);
                        break;
                    }
                }
            }
        }
    }

    TUpdateList* CreateUpdateList(TOperationPtr operation)
    {
        LOG_DEBUG("Operation update list registered (OperationId: %v)",
            operation->GetId());
        auto pair = UpdateLists.insert(std::make_pair(
            operation->GetId(),
            TUpdateList(operation)));
        YCHECK(pair.second);
        return &pair.first->second;
    }

    TUpdateList* FindUpdateList(const TOperationId& operationId)
    {
        auto it = UpdateLists.find(operationId);
        return it == UpdateLists.end() ? nullptr : &it->second;
    }

    TUpdateList* GetUpdateList(const TOperationId& operationId)
    {
        auto* result = FindUpdateList(operationId);
        YCHECK(result);
        return result;
    }

    void RemoveUpdateList(TOperationPtr operation)
    {
        LOG_DEBUG("Operation update list unregistered (OperationId: %v)",
            operation->GetId());
        YCHECK(UpdateLists.erase(operation->GetId()));
    }

    void ClearUpdateLists()
    {
        UpdateLists.clear();
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

    void UpdateOperationNodes()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_INFO("Updating nodes for %v operations",
            UpdateLists.size());

        // Issue updates for active operations.
        std::vector<TOperationPtr> finishedOperations;
        std::vector<TFuture<void>> asyncResults;
        for (auto& pair : UpdateLists) {
            auto& list = pair.second;
            auto operation = list.Operation;
            if (operation->IsFinishedState()) {
                finishedOperations.push_back(operation);
            } else {
                LOG_DEBUG("Updating operation node (OperationId: %v)",
                    operation->GetId());

                asyncResults.push_back(UpdateOperationNode(&list).Apply(
                    BIND(&TImpl::OnOperationNodeUpdated, MakeStrong(this), operation)
                        .AsyncVia(CancelableControlInvoker)));
            }
        }

        // Cleanup finished operations.
        for (auto operation : finishedOperations) {
            RemoveUpdateList(operation);
        }

        auto result = WaitFor(Combine(asyncResults));
        if (!result.IsOK()) {
            LOG_ERROR(result, "Error updating operation nodes");
            Disconnect();
            return;
        }

        LOG_INFO("Operation nodes updated");
    }

    void OnOperationNodeUpdated(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Operation node updated (OperationId: %v)",
            operation->GetId());
    }


    void UpdateOperationNodeAttributes(TOperationPtr operation)
    {
        auto batchReq = StartObjectBatchRequest();
        auto operationPath = GetOperationPath(operation->GetId());

        GenerateMutationId(batchReq);

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

        if (operation->HasControllerProgress())
        {
            auto controller = operation->GetController();

            // Set progress.
            {
                auto progress = controller->GetProgress();
                YCHECK(progress);

                auto req = TYPathProxy::Set(operationPath + "/@progress");
                req->set_value(progress.GetData());
                batchReq->AddRequest(req, "update_op_node");
            }
            // Set brief progress.
            {
                auto progress = controller->GetBriefProgress();
                YCHECK(progress);

                auto req = TYPathProxy::Set(operationPath + "/@brief_progress");
                req->set_value(progress.GetData());
                batchReq->AddRequest(req, "update_op_node");
            }
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

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }

    TOperationSnapshot DoDownloadSnapshot(const TOperationId& operationId)
    {
        auto snapshotPath = GetSnapshotPath(operationId);

        auto batchReq = StartObjectBatchRequest();
        auto req = TYPathProxy::Get(snapshotPath + "/@version");
        batchReq->AddRequest(req, "get_version");

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        const auto& batchRsp = batchRspOrError.ValueOrThrow();

        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_version");
        // Check for missing snapshots.
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION("Snapshot does not exist");
        }
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting snapshot version");

        const auto& rsp = rspOrError.Value();
        int version = ConvertTo<int>(TYsonString(rsp->value()));

        LOG_INFO("Snapshot found (OperationId: %v, Version: %v)",
            operationId,
            version);

        if (!ValidateSnapshotVersion(version)) {
            THROW_ERROR_EXCEPTION("Snapshot version validation failed");
        }

        TOperationSnapshot snapshot;
        snapshot.Version = version;
        try {
            auto downloader = New<TSnapshotDownloader>(
                Config,
                Bootstrap,
                operationId);
            snapshot.Data = downloader->Run();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error downloading snapshot") << ex;
        }
        return snapshot;
    }

    void DoRemoveSnapshot(const TOperationId& operationId)
    {
        auto batchReq = StartObjectBatchRequest();
        auto req = TYPathProxy::Remove(GetSnapshotPath(operationId));
        req->set_force(true);
        GenerateMutationId(req);
        batchReq->AddRequest(req, "remove_snapshot");

        auto batchRspOrError = WaitFor(batchReq->Invoke());

        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
    }

    void CreateJobNodes(
        TOperationPtr operation,
        const std::vector<TCreateJobNodeRequest>& jobRequests)
    {
        auto batchReq = StartObjectBatchRequest();

        for (const auto& request : jobRequests) {
            const auto& jobId = request.JobId;
            auto jobPath = GetJobPath(operation->GetId(), jobId);
            TYsonString inputPaths;
            if (request.InputPathsFuture) {
                auto inputPathsOrError = WaitFor(request.InputPathsFuture);
                if (!inputPathsOrError.IsOK()) {
                    LOG_WARNING(
                        inputPathsOrError,
                        "Error obtaining input paths for failed job (JobId: %v)",
                        jobId);
                } else {
                    inputPaths = inputPathsOrError.Value();
                }
            }

            auto attributes = ConvertToAttributes(request.Attributes);
            if (inputPaths) {
                attributes->SetYson("input_paths", inputPaths);
            }

            auto req = TCypressYPathProxy::Create(jobPath);
            req->set_type(static_cast<int>(EObjectType::MapNode));
            ToProto(req->mutable_node_attributes(), *attributes);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "create");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        auto error = GetCumulativeError(batchRspOrError);
        if (!error.IsOK()) {
            if (error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded)) {
                LOG_ERROR(error, "Account limit exceeded while creating job nodes");
            } else {
                THROW_ERROR_EXCEPTION("Failed to create job nodes") << error;
            }
        }
    }

    struct TJobFile
    {
        TJobId JobId;
        TYPath Path;
        TChunkId ChunkId;
        TString DescriptionType;
    };

    void SaveJobFiles(const TOperationId& operationId, const std::vector<TJobFile>& files)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto client = Bootstrap->GetMasterClient();
        auto connection = client->GetNativeConnection();

        ITransactionPtr transaction;
        {
            NApi::TTransactionStartOptions options;
            options.PrerequisiteTransactionIds = {LockTransaction->GetId()};
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Format("Saving job files of operation %v", operationId));
            options.Attributes = std::move(attributes);

            transaction = WaitFor(client->StartTransaction(ETransactionType::Master, options))
                .ValueOrThrow();
        }

        const auto& transactionId = transaction->GetId();

        yhash<TCellTag, std::vector<TJobFile>> cellTagToFiles;
        for (const auto& file : files) {
            cellTagToFiles[CellTagFromId(file.ChunkId)].push_back(file);
        }

        for (const auto& pair : cellTagToFiles) {
            auto cellTag = pair.first;
            const auto& perCellFiles = pair.second;

            struct TJobFileInfo
            {
                TTransactionId UploadTransactionId;
                TNodeId NodeId;
                TChunkListId ChunkListId;
                NChunkClient::NProto::TDataStatistics Statistics;
            };

            std::vector<TJobFileInfo> infos;

            {
                auto batchReq = StartObjectBatchRequest();

                for (const auto& file : perCellFiles) {
                    {
                        auto req = TCypressYPathProxy::Create(file.Path);
                        req->set_recursive(true);
                        req->set_type(static_cast<int>(EObjectType::File));

                        auto attributes = CreateEphemeralAttributes();
                        if (cellTag == connection->GetPrimaryMasterCellTag()) {
                            attributes->Set("external", false);
                        } else {
                            attributes->Set("external_cell_tag", cellTag);
                        }
                        attributes->Set("vital", false);
                        attributes->Set("replication_factor", 1);
                        attributes->Set(
                            "description", BuildYsonStringFluently()
                                .BeginMap()
                                    .Item("type").Value(file.DescriptionType)
                                    .Item("job_id").Value(file.JobId)
                                .EndMap());
                        ToProto(req->mutable_node_attributes(), *attributes);

                        SetTransactionId(req, transactionId);
                        GenerateMutationId(req);
                        batchReq->AddRequest(req, "create");
                    }
                    {
                        auto req = TFileYPathProxy::BeginUpload(file.Path);
                        req->set_update_mode(static_cast<int>(EUpdateMode::Overwrite));
                        req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));
                        req->set_upload_transaction_title(Format("Saving files of job %v of operation %v",
                            file.JobId,
                            operationId));
                        GenerateMutationId(req);
                        SetTransactionId(req, transactionId);
                        batchReq->AddRequest(req, "begin_upload");
                    }
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
                const auto& batchRsp = batchRspOrError.Value();

                auto createRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create");
                auto beginUploadRsps = batchRsp->GetResponses<TFileYPathProxy::TRspBeginUpload>("begin_upload");
                for (int index = 0; index < perCellFiles.size(); ++index) {
                    infos.push_back(TJobFileInfo());
                    auto& info = infos.back();

                    {
                        const auto& rsp = createRsps[index].Value();
                        info.NodeId = FromProto<TNodeId>(rsp->node_id());
                    }
                    {
                        const auto& rsp = beginUploadRsps[index].Value();
                        info.UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
                    }
                }
            }

            {
                auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower, cellTag);

                for (const auto& info : infos) {
                    auto req = TFileYPathProxy::GetUploadParams(FromObjectId(info.NodeId));
                    SetTransactionId(req, info.UploadTransactionId);
                    batchReq->AddRequest(req, "get_upload_params");
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
                const auto& batchRsp = batchRspOrError.Value();

                auto getUploadParamsRsps = batchRsp->GetResponses<TFileYPathProxy::TRspGetUploadParams>("get_upload_params");
                for (int index = 0; index < getUploadParamsRsps.size(); ++index) {
                    const auto& rsp = getUploadParamsRsps[index].Value();
                    auto& info = infos[index];
                    info.ChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
                }
            }

            {
                auto batchReq = StartChunkBatchRequest(cellTag);
                GenerateMutationId(batchReq);
                batchReq->set_suppress_upstream_sync(true);

                for (int index = 0; index < perCellFiles.size(); ++index) {
                    const auto& file = perCellFiles[index];
                    const auto& info = infos[index];
                    auto* req = batchReq->add_attach_chunk_trees_subrequests();
                    ToProto(req->mutable_parent_id(), info.ChunkListId);
                    ToProto(req->add_child_ids(), file.ChunkId);
                    req->set_request_statistics(true);
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
                const auto& batchRsp = batchRspOrError.Value();

                for (int index = 0; index < perCellFiles.size(); ++index) {
                    auto& info = infos[index];
                    const auto& rsp = batchRsp->attach_chunk_trees_subresponses(index);
                    info.Statistics = rsp.statistics();
                }
            }

            {
                auto batchReq = StartObjectBatchRequest();

                for (int index = 0; index < perCellFiles.size(); ++index) {
                    const auto& info = infos[index];
                    auto req = TFileYPathProxy::EndUpload(FromObjectId(info.NodeId));
                    *req->mutable_statistics() = info.Statistics;
                    SetTransactionId(req, info.UploadTransactionId);
                    GenerateMutationId(req);
                    batchReq->AddRequest(req);
                }

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            }
        }

        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    void AttachLivePreviewChunks(
        TOperationPtr operation,
        const TTransactionId& transactionId,
        const std::vector<TLivePreviewRequest>& livePreviewRequests)
    {
        struct TTableInfo
        {
            TNodeId TableId;
            TCellTag CellTag;
            std::vector<TChunkId> ChildIds;
            TTransactionId UploadTransactionId;
            TChunkListId UploadChunkListId;
            NChunkClient::NProto::TDataStatistics Statistics;
        };

        yhash<TNodeId, TTableInfo> tableIdToInfo;
        for (const auto& request : livePreviewRequests) {
            auto& tableInfo = tableIdToInfo[request.TableId];
            tableInfo.TableId = request.TableId;
            tableInfo.ChildIds.push_back(request.ChildId);
        }

        if (tableIdToInfo.empty()) {
            return;
        }

        for (const auto& pair : tableIdToInfo) {
            const auto& tableInfo = pair.second;
            LOG_DEBUG("Appending live preview chunk trees (OperationId: %v, TableId: %v, ChildCount: %v)",
                operation->GetId(),
                tableInfo.TableId,
                tableInfo.ChildIds.size());
        }

        // BeginUpload
        {
            auto batchReq = StartObjectBatchRequest();

            for (const auto& pair : tableIdToInfo) {
                const auto& tableId = pair.first;
                {
                    auto req = TTableYPathProxy::BeginUpload(FromObjectId(tableId));
                    req->set_update_mode(static_cast<int>(EUpdateMode::Append));
                    req->set_lock_mode(static_cast<int>(ELockMode::Shared));
                    req->set_upload_transaction_title(Format("Attaching live preview chunks of operation %v",
                        operation->GetId()));
                    SetTransactionId(req, transactionId);
                    GenerateMutationId(req);
                    batchReq->AddRequest(req, "begin_upload");
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            auto rsps = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspBeginUpload>("begin_upload");
            int rspIndex = 0;
            for (auto& pair : tableIdToInfo) {
                auto& tableInfo = pair.second;
                const auto& rsp = rsps[rspIndex++].Value();
                tableInfo.CellTag = rsp->cell_tag();
                tableInfo.UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
            }
        }

        yhash<TCellTag, std::vector<TTableInfo*>> cellTagToInfos;
        for (auto& pair : tableIdToInfo) {
            auto& tableInfo  = pair.second;
            cellTagToInfos[tableInfo.CellTag].push_back(&tableInfo);
        }

        // GetUploadParams
        for (auto& pair : cellTagToInfos) {
            auto cellTag = pair.first;
            auto& tableInfos = pair.second;

            auto batchReq = StartObjectBatchRequest(EMasterChannelKind::Follower, cellTag);
            for (const auto* tableInfo : tableInfos) {
                auto req = TTableYPathProxy::GetUploadParams(FromObjectId(tableInfo->TableId));
                SetTransactionId(req, tableInfo->UploadTransactionId);
                batchReq->AddRequest(req, "get_upload_params");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            auto rsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetUploadParams>("get_upload_params");
            int rspIndex = 0;
            for (auto* tableInfo : tableInfos) {
                const auto& rsp = rsps[rspIndex++].Value();
                tableInfo->UploadChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            }
        }

        // Attach
        for (auto& pair : cellTagToInfos) {
            auto cellTag = pair.first;
            auto& tableInfos = pair.second;

            auto batchReq = StartChunkBatchRequest(cellTag);
            GenerateMutationId(batchReq);
            batchReq->set_suppress_upstream_sync(true);

            std::vector<int> tableIndexToRspIndex;
            for (const auto* tableInfo : tableInfos) {
                size_t beginIndex = 0;
                const auto& childIds = tableInfo->ChildIds;
                while (beginIndex < childIds.size()) {
                    auto lastIndex = std::min(beginIndex + Config->MaxChildrenPerAttachRequest, childIds.size());
                    bool isFinal = (lastIndex == childIds.size());
                    if (isFinal) {
                        tableIndexToRspIndex.push_back(batchReq->attach_chunk_trees_subrequests_size());
                    }
                    auto* req = batchReq->add_attach_chunk_trees_subrequests();
                    ToProto(req->mutable_parent_id(), tableInfo->UploadChunkListId);
                    for (auto index = beginIndex; index < lastIndex; ++index) {
                        ToProto(req->add_child_ids(), childIds[index]);
                    }
                    req->set_request_statistics(isFinal);
                    beginIndex = lastIndex;
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
            const auto& batchRsp = batchRspOrError.Value();

            const auto& rsps = batchRsp->attach_chunk_trees_subresponses();
            for (int tableIndex = 0; tableIndex < tableInfos.size(); ++tableIndex) {
                auto* tableInfo = tableInfos[tableIndex];
                const auto& rsp = rsps.Get(tableIndexToRspIndex[tableIndex]);
                tableInfo->Statistics = rsp.statistics();
            }
        }

        // EndUpload
        {
            auto batchReq = StartObjectBatchRequest();

            for (const auto& pair : tableIdToInfo) {
                const auto& tableId = pair.first;
                const auto& tableInfo = pair.second;
                {
                    auto req = TTableYPathProxy::EndUpload(FromObjectId(tableId));
                    *req->mutable_statistics() = tableInfo.Statistics;
                    SetTransactionId(req, tableInfo.UploadTransactionId);
                    GenerateMutationId(req);
                    batchReq->AddRequest(req, "end_upload");
                }
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));
        }
    }

    void DoUpdateOperationNode(
        TOperationPtr operation,
        const TTransactionId& transactionId,
        const std::vector<TCreateJobNodeRequest>& jobRequests,
        const std::vector<TLivePreviewRequest>& livePreviewRequests)
    {
        try {
            CreateJobNodes(operation, jobRequests);
        } catch (const std::exception& ex) {
            auto error = TError("Error creating job nodes for operation %v",
                operation->GetId())
                << ex;
            if (error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded)) {
                LOG_DEBUG(error);
                return;
            } else {
                THROW_ERROR error;
            }
        }

        try {
            std::vector<TJobFile> files;
            for (const auto& request : jobRequests) {
                if (request.StderrChunkId) {
                    files.push_back({
                        request.JobId,
                        GetStderrPath(operation->GetId(), request.JobId),
                        request.StderrChunkId,
                        "stderr"
                    });
                }
                if (request.FailContextChunkId) {
                    files.push_back({
                        request.JobId,
                        GetFailContextPath(operation->GetId(), request.JobId),
                        request.FailContextChunkId,
                        "fail_context"
                    });
                }
            }
            SaveJobFiles(operation->GetId(), files);
        } catch (const std::exception& ex) {
            // NB: Don' treat this as a critical error.
            // Some of these chunks could go missing for a number of reasons.
            LOG_WARNING(ex, "Error saving job files (OperationId: %v)",
                operation->GetId());
        }

        try {
            AttachLivePreviewChunks(operation, transactionId, livePreviewRequests);
        } catch (const std::exception& ex) {
            // NB: Don' treat this as a critical error.
            // Some of these chunks could go missing for a number of reasons.
            LOG_WARNING(ex, "Error attaching live preview chunks (OperationId: %v)",
                operation->GetId());
        }

        try {
            // NB: Update operation attributes after updating all job nodes.
            // Tests assume, that all job files are present, when operation
            // is in one of the terminal states.
            UpdateOperationNodeAttributes(operation);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating operation node %v",
                operation->GetId())
                << ex;
        }
    }

    TFuture<void> UpdateOperationNode(TUpdateList* list)
    {
        auto operation = list->Operation;

        auto lastUpdateFuture = list->LastUpdateFuture.Apply(
            BIND(
                &TImpl::DoUpdateOperationNode,
                MakeStrong(this),
                operation,
                list->TransactionId,
                std::move(list->JobRequests),
                std::move(list->LivePreviewRequests))
            .AsyncVia(CancelableControlInvoker));

        list->LastUpdateFuture = lastUpdateFuture;
        return lastUpdateFuture;
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

        CreateUpdateList(operation);

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

    void OnOperationNodeFlushed(
        TOperationPtr operation,
        const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto operationId = operation->GetId();

        if (!error.IsOK()) {
            LOG_ERROR(error);
            Disconnect();
            return;
        }

        LOG_INFO("Operation node flushed (OperationId: %v)",
            operationId);
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
        for (auto alertType : TEnumTraits<EAlertType>::GetDomainValues()) {
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

    void BuildSnapshot()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Config->EnableSnapshotBuilding)
            return;

        auto builder = New<TSnapshotBuilder>(
            Config,
            Bootstrap->GetScheduler(),
            Bootstrap->GetMasterClient());

        // NB: Result is logged in the builder.
        auto error = WaitFor(builder->Run());
        if (error.IsOK()) {
            LOG_INFO("Snapshot builder finished");
        } else {
            LOG_ERROR(error, "Error building snapshots");
        }
    }

    void DoAttachToLivePreview(
        const TOperationId& operationId,
        const TTransactionId& transactionId,
        const TNodeId& tableId,
        const std::vector<TChunkTreeId>& childIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto* list = FindUpdateList(operationId);
        if (!list) {
            LOG_DEBUG("Operation node is not registered, omitting live preview attach (OperationId: %v)",
                operationId);
            return;
        }

        if (!list->TransactionId) {
            list->TransactionId = transactionId;
        } else {
            // NB: Controller must attach all live preview chunks under the same transaction.
            YCHECK(list->TransactionId == transactionId);
        }

        LOG_TRACE("Attaching live preview chunk trees (OperationId: %v, TableId: %v, ChildCount: %v)",
            operationId,
            tableId,
            childIds.size());

        for (const auto& childId : childIds) {
            list->LivePreviewRequests.push_back(TLivePreviewRequest{tableId, childId});
        }
    }


    INativeConnectionPtr FindConnection(TCellTag cellTag)
    {
        auto localConnection = Bootstrap->GetMasterClient()->GetNativeConnection();
        return cellTag == localConnection->GetCellTag()
            ? localConnection
            : ClusterDirectory->FindConnection(cellTag);
    }

    INativeConnectionPtr GetConnectionOrThrow(TCellTag cellTag)
    {
        auto localConnection = Bootstrap->GetMasterClient()->GetNativeConnection();
        return cellTag == localConnection->GetCellTag()
            ? localConnection
            : ClusterDirectory->GetConnectionOrThrow(cellTag);
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

TFuture<TOperationSnapshot> TMasterConnector::DownloadSnapshot(const TOperationId& operationId)
{
    return Impl->DownloadSnapshot(operationId);
}

TFuture<void> TMasterConnector::RemoveSnapshot(const TOperationId& operationId)
{
    return Impl->RemoveSnapshot(operationId);
}

void TMasterConnector::CreateJobNode(const TCreateJobNodeRequest& createJobNodeRequest)
{
    return Impl->CreateJobNode(createJobNodeRequest);
}

void TMasterConnector::RegisterAlert(EAlertType alertType, const TError& alert)
{
    Impl->RegisterAlert(alertType, alert);
}

void TMasterConnector::UnregisterAlert(EAlertType alertType)
{
    Impl->UnregisterAlert(alertType);
}

void TMasterConnector::AttachJobContext(
    const TYPath& path,
    const TChunkId& chunkId,
    const TOperationId& operationId,
    const TJobId& jobId)
{
    return Impl->AttachJobContext(path, chunkId, operationId, jobId);
}

void TMasterConnector::UpdateConfig(const TSchedulerConfigPtr& config)
{
    Impl->UpdateConfig(config);
}

TFuture<void> TMasterConnector::AttachToLivePreview(
    const TOperationId& operationId,
    const TTransactionId& transactionId,
    const TNodeId& tableId,
    const std::vector<TChunkTreeId>& childIds)
{
    return Impl->AttachToLivePreview(operationId, transactionId, tableId, childIds);
}

void TMasterConnector::AddGlobalWatcherRequester(TWatcherRequester requester)
{
    Impl->AddGlobalWatcherRequester(requester);
}

void TMasterConnector::AddGlobalWatcherHandler(TWatcherHandler handler)
{
    Impl->AddGlobalWatcherHandler(handler);
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
DELEGATE_SIGNAL(TMasterConnector, void(TOperationPtr operation), UserTransactionAborted, *Impl)
DELEGATE_SIGNAL(TMasterConnector, void(TOperationPtr operation), SchedulerTransactionAborted, *Impl)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

