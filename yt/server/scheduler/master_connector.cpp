#include "stdafx.h"
#include "master_connector.h"
#include "scheduler.h"
#include "private.h"
#include "helpers.h"
#include "snapshot_builder.h"
#include "snapshot_downloader.h"
#include "serialize.h"
#include "scheduler_strategy.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/misc/address.h>

#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/scheduler.h>

#include <core/rpc/serialized_channel.h>
#include <core/rpc/helpers.h>

#include <core/yson/consumer.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/fluent.h>
#include <core/ytree/node.h>

#include <core/ypath/token.h>

#include <ytlib/api/config.h>
#include <ytlib/api/connection.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/helpers.h>
#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/scheduler/helpers.h>

#include <ytlib/security_client/public.h>

#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/hive/cluster_directory.h>

#include <server/cell_scheduler/bootstrap.h>
#include <server/cell_scheduler/config.h>

#include <server/object_server/object_manager.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NRpc;
using namespace NApi;
using namespace NSecurityClient;
using namespace NTransactionClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////

class TMasterConnector::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap)
        : Config(config)
        , Bootstrap(bootstrap)
        , Proxy(Bootstrap->GetMasterClient()->GetMasterChannel())
        , ClusterDirectory(Bootstrap->GetClusterDirectory())
        , Connected(false)
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


    TAsyncError CreateOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto id = operation->GetId();
        LOG_INFO("Creating operation node (OperationId: %v)",
            id);

        auto* list = CreateUpdateList(operation);
        auto strategy = Bootstrap->GetScheduler()->GetStrategy();

        auto batchReq = StartBatchRequest(list);
        {
            auto req = TYPathProxy::Set(GetOperationPath(id));
            req->set_value(BuildYsonStringFluently()
                .BeginAttributes()
                    .Do(BIND(&ISchedulerStrategy::BuildOperationAttributes, strategy, operation))
                    .Do(BIND(&BuildInitializingOperationAttributes, operation))
                    .Item("brief_spec").BeginMap()
                        .Do(BIND(&IOperationController::BuildBriefSpec, operation->GetController()))
                        .Do(BIND(&ISchedulerStrategy::BuildBriefSpec, strategy, operation))
                    .EndMap()
                    .Item("progress").BeginMap().EndMap()
                    .Item("brief_progress").BeginMap().EndMap()
                    .Item("opaque").Value("true")
                .EndAttributes()
                .BeginMap()
                    .Item("jobs").BeginAttributes()
                        .Item("opaque").Value("true")
                    .EndAttributes()
                    .BeginMap().EndMap()
                .EndMap()
                .Data());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        return batchReq->Invoke().Apply(
            BIND(
                &TImpl::OnOperationNodeCreated,
                MakeStrong(this),
                operation)
            .AsyncVia(Bootstrap->GetControlInvoker()));
    }

    TAsyncError ResetRevivingOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);
        YCHECK(operation->GetState() == EOperationState::Reviving);

        auto id = operation->GetId();
        LOG_INFO("Resetting reviving operation node (OperationId: %v)",
            id);

        auto* list = GetUpdateList(operation);
        auto batchReq = StartBatchRequest(list);

        auto attributes = ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Do(BIND(&BuildRunningOperationAttributes, operation))
                .Item("progress").BeginMap().EndMap()
                .Item("brief_progress").BeginMap().EndMap()
            .EndMap());

        for (const auto& key : attributes->List()) {
            auto req = TYPathProxy::Set(GetOperationPath(id) + "/@" + ToYPathLiteral(key));
            req->set_value(attributes->GetYson(key).Data());
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        return batchReq->Invoke().Apply(
            BIND(
                &TImpl::OnRevivingOperationNodeReset,
                MakeStrong(this),
                operation)
            .AsyncVia(Bootstrap->GetControlInvoker()));
    }

    TFuture<void> FlushOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto id = operation->GetId();
        LOG_INFO("Flushing operation node (OperationId: %v)",
            id);

        auto* list = FindUpdateList(operation);
        if (!list) {
            LOG_INFO("Operation node is not registered, omitting flush (OperationId: %v)",
                id);
            return VoidFuture;
        }

        // Create a batch update for this particular operation.
        auto batchReq = StartBatchRequest(list);
        PrepareOperationUpdate(list, batchReq);

        return batchReq->Invoke().Apply(
            BIND(&TImpl::OnOperationNodeFlushed, MakeStrong(this), operation)
                .Via(CancelableControlInvoker));
    }


    void CreateJobNode(TJobPtr job,
        const NChunkClient::TChunkId& stderrChunkId,
        const std::vector<TChunkId>& failContextChunkIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Creating job node (OperationId: %v, JobId: %v, StdErrChunkId: %v, FailContextChunkIds: %v)",
            job->GetOperation()->GetId(),
            job->GetId(),
            stderrChunkId,
            JoinToString(failContextChunkIds));

        auto* list = GetUpdateList(job->GetOperation());
        TJobRequest request;
        request.Job = job;
        request.StderrChunkId = stderrChunkId;
        request.FailContextChunkIds = failContextChunkIds;
        list->JobRequests.push_back(request);
    }

    void AttachToLivePreview(
        TOperationPtr operation,
        const TChunkListId& chunkListId,
        const TChunkTreeId& childId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Attaching live preview chunk tree (OperationId: %v, ChunkListId: %v, ChildId: %v)",
            operation->GetId(),
            chunkListId,
            childId);

        auto* list = GetUpdateList(operation);
        TLivePreviewRequest request;
        request.ChunkListId = chunkListId;
        request.ChildId = childId;
        list->LivePreviewRequests.push_back(request);
    }

    void AttachToLivePreview(
        TOperationPtr operation,
        const TChunkListId& chunkListId,
        const std::vector<TChunkTreeId>& childrenIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Attaching live preview chunk trees (OperationId: %v, ChunkListId: %v, ChildrenCount: %v)",
            operation->GetId(),
            chunkListId,
            static_cast<int>(childrenIds.size()));

        auto* list = GetUpdateList(operation);
        for (const auto& childId : childrenIds) {
            TLivePreviewRequest request;
            request.ChunkListId = chunkListId;
            request.ChildId = childId;
            list->LivePreviewRequests.push_back(request);
        }
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


    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);

    DEFINE_SIGNAL(void(TOperationPtr operation), UserTransactionAborted);
    DEFINE_SIGNAL(void(TOperationPtr operation), SchedulerTransactionAborted);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    TObjectServiceProxy Proxy;
    NHive::TClusterDirectoryPtr ClusterDirectory;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;

    bool Connected;

    NTransactionClient::TTransactionPtr LockTransaction;

    TPeriodicExecutorPtr TransactionRefreshExecutor;
    TPeriodicExecutorPtr OperationNodesUpdateExecutor;
    TPeriodicExecutorPtr WatchersExecutor;
    TPeriodicExecutorPtr SnapshotExecutor;
    TPeriodicExecutorPtr ClusterDirectoryExecutor;

    std::vector<TWatcherRequester> GlobalWatcherRequesters;
    std::vector<TWatcherHandler>   GlobalWatcherHandlers;

    struct TJobRequest
    {
        TJobPtr Job;
        TChunkId StderrChunkId;
        std::vector<TChunkId> FailContextChunkIds;
    };

    struct TLivePreviewRequest
    {
        TChunkListId ChunkListId;
        TChunkTreeId ChildId;
    };

    struct TUpdateList
    {
        TUpdateList(IChannelPtr masterChannel, TOperationPtr operation)
            : Operation(operation)
            , Proxy(CreateSerializedChannel(masterChannel))
        { }

        TOperationPtr Operation;
        std::vector<TJobRequest> JobRequests;
        std::vector<TLivePreviewRequest> LivePreviewRequests;
        TObjectServiceProxy Proxy;
    };

    yhash_map<TOperationId, TUpdateList> UpdateLists;

    struct TWatcherList
    {
        explicit TWatcherList(TOperationPtr operation)
            : Operation(operation)
        { }

        TOperationPtr Operation;
        std::vector<TWatcherRequester> WatcherRequesters;
        std::vector<TWatcherHandler>   WatcherHandlers;
    };

    yhash_map<TOperationId, TWatcherList> WatcherLists;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


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

    void OnConnected(TErrorOr<TMasterHandshakeResult> resultOrError)
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
        for (auto operation : result.Operations) {
            CreateUpdateList(operation);
        }
        for (auto handler : GlobalWatcherHandlers) {
            handler.Run(result.WatcherResponses);
        }

        LockTransaction->SubscribeAborted(
            BIND(&TImpl::OnLockTransactionAborted, MakeWeak(this))
                .Via(CancelableControlInvoker));

        StartRefresh();
        StartSnapshots();

        MasterConnected_.Fire(result);
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
        {
            auto localHostName = TAddressResolver::Get()->GetLocalHostName();
            int port = Owner->Bootstrap->GetConfig()->RpcPort;
            ServiceAddress = BuildServiceAddress(localHostName, port);
        }

        TErrorOr<TMasterHandshakeResult> Run()
        {
            try {
                RegisterInstance();
                StartLockTransaction();
                TakeLock();
                AssumeControl();
                UpdateClusterDirectory();
                ListOperations();
                AbortAbortingOperations();
                RequestOperationAttributes();
                CheckOperationTransactions();
                DownloadSnapshots();
                AbortTransactions();
                RemoveSnapshots();
                InvokeWatchers();
                return Result;
            } catch (const std::exception& ex) {
                return TErrorOr<TMasterHandshakeResult>(ex);
            }
        }

    private:
        TIntrusivePtr<TImpl> Owner;
        Stroka ServiceAddress;
        std::vector<TOperationId> OperationIds;
        std::vector<TOperationId> AbortingOperationIds;
        TMasterHandshakeResult Result;

        // - Register scheduler instance.
        void RegisterInstance()
        {
            auto batchReq = Owner->StartBatchRequest(false);
            auto path = "//sys/scheduler/instances/" + ToYPathLiteral(ServiceAddress);
            {
                auto req = TCypressYPathProxy::Create(path);
                req->set_ignore_existing(true);
                req->set_type(EObjectType::MapNode);
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }
            {
                auto req = TCypressYPathProxy::Create(path + "/orchid");
                req->set_ignore_existing(true);
                req->set_type(EObjectType::Orchid);
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("remote_address", ServiceAddress);
                ToProto(req->mutable_node_attributes(), *attributes);
                GenerateMutationId(req);
                batchReq->AddRequest(req);
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError());
        }

        // - Start lock transaction.
        void StartLockTransaction()
        {
            auto batchReq = Owner->StartBatchRequest(false);
            {
                auto req = TMasterYPathProxy::CreateObjects();
                req->set_type(EObjectType::Transaction);

                auto* reqExt = req->MutableExtension(TReqStartTransactionExt::create_transaction_ext);
                reqExt->set_timeout(Owner->Config->LockTransactionTimeout.MilliSeconds());

                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Scheduler lock at %v", ServiceAddress));
                ToProto(req->mutable_object_attributes(), *attributes);

                GenerateMutationId(req);
                batchReq->AddRequest(req, "start_lock_tx");
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);

            {
                auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_lock_tx");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting lock transaction");
                auto transactionId = FromProto<TTransactionId>(rsp->object_ids(0));

                TTransactionAttachOptions options(transactionId);
                options.AutoAbort = true;
                auto transactionManager = Owner->Bootstrap->GetMasterClient()->GetTransactionManager();
                Owner->LockTransaction = transactionManager->Attach(options);

                LOG_INFO("Lock transaction is %v", transactionId);
            }
        }

        // - Take lock.
        void TakeLock()
        {
            auto batchReq = Owner->StartBatchRequest();
            {
                auto req = TCypressYPathProxy::Lock("//sys/scheduler/lock");
                SetTransactionId(req, Owner->LockTransaction);
                req->set_mode(ELockMode::Exclusive);
                GenerateMutationId(req);
                batchReq->AddRequest(req, "take_lock");
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError());
        }

        // - Publish scheduler address.
        // - Update orchid address.
        void AssumeControl()
        {
            auto batchReq = Owner->StartBatchRequest();
            auto schedulerAddress = Owner->Bootstrap->GetLocalAddress();
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@address");
                req->set_value(ConvertToYsonString(schedulerAddress).Data());
                GenerateMutationId(req);
                batchReq->AddRequest(req, "set_scheduler_address");
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/orchid/@remote_address");
                req->set_value(ConvertToYsonString(schedulerAddress).Data());
                GenerateMutationId(req);
                batchReq->AddRequest(req, "set_orchid_address");
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError());
        }

        void UpdateClusterDirectory()
        {
            Owner->Bootstrap->GetClusterDirectory()->UpdateSelf();
            Owner->OnGotClusters(WaitFor(Owner->GetClusters()));
        }

        // - Request operations and their states.
        void ListOperations()
        {
            auto batchReq = Owner->StartBatchRequest();
            {
                auto req = TYPathProxy::List("//sys/operations");
                auto* attributeFilter = req->mutable_attribute_filter();
                attributeFilter->set_mode(EAttributeFilterMode::MatchingOnly);
                attributeFilter->add_keys("state");
                batchReq->AddRequest(req, "list_operations");
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError());

            {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>("list_operations");
                auto operationsListNode = ConvertToNode(TYsonString(rsp->keys()));
                auto operationsList = operationsListNode->AsList();
                LOG_INFO("Operations list received, %v operations total",
                    static_cast<int>(operationsList->GetChildCount()));
                OperationIds.clear();
                for (auto operationNode : operationsList->GetChildren()) {
                    auto id = TOperationId::FromString(operationNode->GetValue<Stroka>());
                    auto state = operationNode->Attributes().Get<EOperationState>("state");
                    if (IsOperationInProgress(state)) {
                        OperationIds.push_back(id);
                    } else if (state == EOperationState::Aborting) {
                        AbortingOperationIds.push_back(id);
                    }
                }
            }
        }

        // - Set abort state for aborting operations.
        void AbortAbortingOperations()
        {
            auto batchReq = Owner->StartBatchRequest();

            for (const auto& id : AbortingOperationIds) {
                auto req = TYPathProxy::Set(GetOperationPath(id) + "/@state");
                req->set_value(ConvertToYsonString(EOperationState::Aborted).Data());
                GenerateMutationId(req);
                batchReq->AddRequest(req, "abort_operation");
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError());
        }

        // - Request attributes for unfinished operations.
        // - Recreate operation instance from fetched data.
        void RequestOperationAttributes()
        {
            auto batchReq = Owner->StartBatchRequest();
            {
                LOG_INFO("Fetching attributes for %v unfinished operations",
                    static_cast<int>(OperationIds.size()));
                for (const auto& operationId : OperationIds) {
                    auto req = TYPathProxy::Get(GetOperationPath(operationId));
                    // Keep in sync with CreateOperationFromAttributes.
                    auto* attributeFilter = req->mutable_attribute_filter();
                    attributeFilter->set_mode(EAttributeFilterMode::MatchingOnly);
                    attributeFilter->add_keys("operation_type");
                    attributeFilter->add_keys("mutation_id");
                    attributeFilter->add_keys("user_transaction_id");
                    attributeFilter->add_keys("sync_scheduler_transaction_id");
                    attributeFilter->add_keys("async_scheduler_transaction_id");
                    attributeFilter->add_keys("input_transaction_id");
                    attributeFilter->add_keys("output_transaction_id");
                    attributeFilter->add_keys("spec");
                    attributeFilter->add_keys("authenticated_user");
                    attributeFilter->add_keys("start_time");
                    attributeFilter->add_keys("state");
                    attributeFilter->add_keys("suspended");
                    batchReq->AddRequest(req, "get_op_attr");
                }
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError());

            {
                auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_op_attr");
                YCHECK(rsps.size() == OperationIds.size());

                for (int index = 0; index < static_cast<int>(rsps.size()); ++index) {
                    const auto& operationId = OperationIds[index];
                    auto rsp = rsps[index];
                    auto operationNode = ConvertToNode(TYsonString(rsp->value()));
                    auto operation = Owner->CreateOperationFromAttributes(operationId, operationNode->Attributes());
                    Result.Operations.push_back(operation);
                }
            }
        }

        // - Try to ping the previous incarnations of scheduler transactions.
        void CheckOperationTransactions()
        {
            auto awaiter = New<TParallelAwaiter>(GetCurrentInvoker());

            for (auto operation : Result.Operations) {
                operation->SetState(EOperationState::Reviving);

                auto checkTransaction = [&] (TOperationPtr operation, TTransactionPtr transaction) {
                    if (!transaction)
                        return;

                    awaiter->Await(
                        transaction->Ping(),
                        BIND([=] (const TError& error) {
                            if (!error.IsOK() && !operation->GetCleanStart()) {
                                operation->SetCleanStart(true);
                                LOG_INFO("Error renewing operation transaction, will use clean start (OperationId: %v, TransactionId: %v)",
                                    operation->GetId(),
                                    transaction->GetId());
                            }
                        }));
                };

                // NB: Async transaction is not checked.
                checkTransaction(operation, operation->GetUserTransaction());
                checkTransaction(operation, operation->GetSyncSchedulerTransaction());
                checkTransaction(operation, operation->GetInputTransaction());
                checkTransaction(operation, operation->GetOutputTransaction());
            }

            WaitFor(awaiter->Complete());
        }

        // - Check snapshots for existence and validate versions.
        void DownloadSnapshots()
        {
            for (auto operation : Result.Operations) {
                if (!operation->GetCleanStart()) {
                    if (!DownloadSnapshot(operation)) {
                        operation->SetCleanStart(true);
                    }
                }
            }
        }

        bool DownloadSnapshot(TOperationPtr operation)
        {
            const auto& operationId = operation->GetId();
            auto snapshotPath = GetSnapshotPath(operationId);

            auto batchReq = Owner->StartBatchRequest();
            auto req = TYPathProxy::Get(snapshotPath + "/@version");
            batchReq->AddRequest(req, "get_version");

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);

            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_version");

            // Check for missing snapshots.
            if (rsp->GetError().FindMatching(NYTree::EErrorCode::ResolveError)) {
                LOG_INFO("Snapshot does not exist, will use clean start (OperationId: %v)",
                    operationId);
                return false;
            }
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting snapshot version");

            int version = ConvertTo<int>(TYsonString(rsp->value()));

            LOG_INFO("Snapshot found (OperationId: %v, Version: %v)",
                operationId,
                version);

            if (!ValidateSnapshotVersion(version)) {
                LOG_INFO("Snapshot version validation failed, will use clean start (OperationId: %v)",
                    operationId);
                return false;
            }

            if (!Owner->Config->EnableSnapshotLoading) {
                LOG_INFO("Snapshot loading is disabled in configuration (OperationId: %v)",
                    operationId);
                return false;
            }

            try {
                auto downloader = New<TSnapshotDownloader>(
                    Owner->Config,
                    Owner->Bootstrap,
                    operation);
                downloader->Run();
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error downloading snapshot (OperationId: %s)",
                    ~ToString(operationId));
                return false;
            }

            // Everything seems OK.
            LOG_INFO("Operation state will be recovered from snapshot (OperationId: %v)",
                operationId);
            return true;
        }

        // - Abort orphaned transactions.
        void AbortTransactions()
        {
            auto awaiter = New<TParallelAwaiter>(GetCurrentInvoker());

            for (auto operation : Result.Operations) {
                auto scheduleAbort = [=] (TTransactionPtr transaction) {
                    if (!transaction)
                        return;

                    awaiter->Await(transaction->Abort());
                };

                // NB: Async transaction is always aborted.
                {
                    scheduleAbort(operation->GetAsyncSchedulerTransaction());
                    operation->SetAsyncSchedulerTransaction(nullptr);
                }

                if (operation->GetCleanStart()) {
                    LOG_INFO("Aborting operation transactions (OperationId: %v)",
                        operation->GetId());

                    // NB: Don't touch user transaction.
                    scheduleAbort(operation->GetSyncSchedulerTransaction());
                    operation->SetSyncSchedulerTransaction(nullptr);

                    scheduleAbort(operation->GetInputTransaction());
                    operation->SetInputTransaction(nullptr);

                    scheduleAbort(operation->GetOutputTransaction());
                    operation->SetOutputTransaction(nullptr);
                } else {
                    LOG_INFO("Reusing operation transactions (OperationId: %v)",
                        operation->GetId());
                }
            }

            WaitFor(awaiter->Complete());
        }

        // - Remove unneeded snapshots.
        void RemoveSnapshots()
        {
            auto batchReq = Owner->StartBatchRequest();

            for (auto operation : Result.Operations) {
                if (operation->GetCleanStart()) {
                    auto req = TYPathProxy::Remove(GetSnapshotPath(operation->GetId()));
                    req->set_force(true);
                    batchReq->AddRequest(req, "remove_snapshot");
                }
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);

            {
                auto rsps = batchRsp->GetResponses<TYPathProxy::TRspRemove>("remove_snapshot");
                for (auto rsp : rsps) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error removing snapshot");
                }
            }
        }

        // - Send watcher requests.
        void InvokeWatchers()
        {
            auto batchReq = Owner->StartBatchRequest();
            for (auto requester : Owner->GlobalWatcherRequesters) {
                requester.Run(batchReq);
            }

            auto batchRsp = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);
            Result.WatcherResponses = batchRsp;
        }

    };


    TObjectServiceProxy::TReqExecuteBatchPtr StartBatchRequest(bool requireTransaction = true)
    {
        return DoStartBatchRequest(&Proxy, requireTransaction);
    }

    TObjectServiceProxy::TReqExecuteBatchPtr StartBatchRequest(TUpdateList* list, bool requireTransaction = true)
    {
        return DoStartBatchRequest(&list->Proxy, requireTransaction);
    }

    TObjectServiceProxy::TReqExecuteBatchPtr DoStartBatchRequest(TObjectServiceProxy* proxy, bool requireTransaction = true)
    {
        auto req = proxy->ExecuteBatch();
        if (requireTransaction) {
            YCHECK(LockTransaction);
            req->PrerequisiteTransactions().push_back(TObjectServiceProxy::TPrerequisiteTransaction(LockTransaction->GetId()));
        }
        return req;
    }


    void Disconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Connected)
            return;

        LOG_WARNING("Master disconnected");

        Connected = false;

        LockTransaction.Reset();

        ClearUpdateLists();
        ClearWatcherLists();

        StopRefresh();
        StopSnapshots();

        CancelableContext->Cancel();

        MasterDisconnected_.Fire();

        StartConnecting();
    }


    TOperationPtr CreateOperationFromAttributes(const TOperationId& operationId, const IAttributeDictionary& attributes)
    {
        auto getTransaction = [&] (const TTransactionId& id, bool ping) -> TTransactionPtr {
            if (id == NullTransactionId) {
                return nullptr;
            }
            auto clusterDirectory = Bootstrap->GetClusterDirectory();
            auto connection = clusterDirectory->GetConnection(CellTagFromId(id));
            auto client = connection->CreateClient(GetRootClientOptions());
            auto transactionManager = client->GetTransactionManager();
            TTransactionAttachOptions options(id);
            options.AutoAbort = false;
            options.Ping = ping;
            options.PingAncestors = false;
            return transactionManager->Attach(options);
        };

        auto userTransaction = getTransaction(
            attributes.Get<TTransactionId>("user_transaction_id"),
            false);

        auto syncTransaction = getTransaction(
            attributes.Get<TTransactionId>("sync_scheduler_transaction_id"),
            true);

        auto asyncTransaction = getTransaction(
            attributes.Get<TTransactionId>("async_scheduler_transaction_id"),
            true);

        auto inputTransaction = getTransaction(
            attributes.Get<TTransactionId>("input_transaction_id"),
            true);

        auto outputTransaction = getTransaction(
            attributes.Get<TTransactionId>("output_transaction_id"),
            true);

        auto operation = New<TOperation>(
            operationId,
            attributes.Get<EOperationType>("operation_type"),
            attributes.Get<TMutationId>("mutation_id"),
            userTransaction,
            attributes.Get<INodePtr>("spec")->AsMap(),
            attributes.Get<Stroka>("authenticated_user"),
            attributes.Get<TInstant>("start_time"),
            attributes.Get<EOperationState>("state"),
            attributes.Get<bool>("suspended"));

        operation->SetSyncSchedulerTransaction(syncTransaction);
        operation->SetAsyncSchedulerTransaction(asyncTransaction);
        operation->SetInputTransaction(inputTransaction);
        operation->SetOutputTransaction(outputTransaction);

        return operation;
    }


    void StartRefresh()
    {
        TransactionRefreshExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::RefreshTransactions, MakeWeak(this)),
            Config->TransactionsRefreshPeriod,
            EPeriodicExecutorMode::Manual);
        TransactionRefreshExecutor->Start();

        OperationNodesUpdateExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateOperationNodes, MakeWeak(this)),
            Config->OperationsUpdatePeriod,
            EPeriodicExecutorMode::Manual);
        OperationNodesUpdateExecutor->Start();

        WatchersExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateWatchers, MakeWeak(this)),
            Config->WatchersUpdatePeriod,
            EPeriodicExecutorMode::Manual);
        WatchersExecutor->Start();

        ClusterDirectoryExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateClusterDirectory, MakeWeak(this)),
            Config->ClusterDirectoryUpdatePeriod,
            EPeriodicExecutorMode::Manual);
        ClusterDirectoryExecutor->Start();
    }

    void StopRefresh()
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

        if (ClusterDirectoryExecutor) {
            ClusterDirectoryExecutor->Stop();
            ClusterDirectoryExecutor.Reset();
        }
    }


    void StartSnapshots()
    {
        SnapshotExecutor = New<TPeriodicExecutor>(
            CancelableControlInvoker,
            BIND(&TImpl::BuildSnapshot, MakeWeak(this)),
            Config->SnapshotPeriod,
            EPeriodicExecutorMode::Manual);
        SnapshotExecutor->Start();
    }

    void StopSnapshots()
    {
        if (SnapshotExecutor) {
            SnapshotExecutor->Stop();
            SnapshotExecutor.Reset();
        }
    }


    void RefreshTransactions()
    {
        // TODO(ignat): uncomment and remove MultiCellBatchRequest
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        // Collect all transactions that are used by currently running operations.
        yhash_set<TTransactionId> watchSet;
        auto watchTransaction = [&] (TTransactionPtr transaction) {
            if (transaction) {
                watchSet.insert(transaction->GetId());
            }
        };

        auto operations = Bootstrap->GetScheduler()->GetOperations();
        for (auto operation : operations) {
            if (operation->GetState() != EOperationState::Running)
                continue;

            watchTransaction(operation->GetUserTransaction());
            watchTransaction(operation->GetSyncSchedulerTransaction());
            watchTransaction(operation->GetAsyncSchedulerTransaction());
            watchTransaction(operation->GetInputTransaction());
            watchTransaction(operation->GetOutputTransaction());
        }

        yhash_map<TCellTag, TObjectServiceProxy::TReqExecuteBatchPtr> batchRequests;

        for (const auto& id : watchSet) {
            auto cellTag = CellTagFromId(id);
            if (batchRequests.find(cellTag) == batchRequests.end()) {
                auto connection = ClusterDirectory->GetConnection(cellTag);
                auto objectServiceProxy = TObjectServiceProxy(connection->GetMasterChannel());
                batchRequests[cellTag] = objectServiceProxy.ExecuteBatch();
            }

            auto checkReq = TObjectYPathProxy::GetBasicAttributes(FromObjectId(id));
            batchRequests[cellTag]->AddRequest(checkReq, "check_tx_" + ToString(id));
        }

        LOG_INFO("Refreshing transactions");
        TransactionRefreshExecutor->ScheduleNext();


        yhash_map<TCellTag, NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> batchResponses;

        for (const auto& pair : batchRequests) {
            auto cellTag = pair.first;
            const auto& request = pair.second;
            auto response = WaitFor(request->Invoke(), CancelableControlInvoker);
            if (!response->IsOK()) {
                LOG_ERROR(*response, "Error refreshing transactions");
            }
            batchResponses[cellTag] = response;
        }

        yhash_set<TTransactionId> deadTransactionIds;

        for (const auto& id : watchSet) {
            auto cellTag = CellTagFromId(id);
            if (!batchResponses[cellTag]->GetResponse("check_tx_" + ToString(id))->IsOK()) {
                deadTransactionIds.insert(id);
            }
        }


        LOG_INFO("Transactions refreshed");

        auto isTransactionAlive = [&] (TOperationPtr operation, TTransactionPtr transaction) -> bool {
            if (!transaction) {
                return true;
            }

            if (deadTransactionIds.find(transaction->GetId()) == deadTransactionIds.end()) {
                return true;
            }

            return false;
        };

        auto isUserTransactionAlive = [&] (TOperationPtr operation, TTransactionPtr transaction) -> bool {
            if (isTransactionAlive(operation, transaction)) {
                return true;
            }

            LOG_INFO("Expired user transaction found (OperationId: %v, TransactionId: %v)",
                operation->GetId(),
                transaction->GetId());
            return false;
        };

        auto isSchedulerTransactionAlive = [&] (TOperationPtr operation, TTransactionPtr transaction) -> bool {
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
            if (operation->GetState() != EOperationState::Running)
                continue;

            if (!isUserTransactionAlive(operation, operation->GetUserTransaction())) {
                UserTransactionAborted_.Fire(operation);
            }

            if (!isSchedulerTransactionAlive(operation, operation->GetSyncSchedulerTransaction()) ||
                !isSchedulerTransactionAlive(operation, operation->GetAsyncSchedulerTransaction()) ||
                !isSchedulerTransactionAlive(operation, operation->GetInputTransaction()) ||
                !isSchedulerTransactionAlive(operation, operation->GetOutputTransaction()))
            {
                SchedulerTransactionAborted_.Fire(operation);
            }
        }
    }

    TUpdateList* CreateUpdateList(TOperationPtr operation)
    {
        LOG_DEBUG("Operation update list registered (OperationId: %v)",
            operation->GetId());
        TUpdateList list(Bootstrap->GetMasterClient()->GetMasterChannel(), operation);
        auto pair = UpdateLists.insert(std::make_pair(operation->GetId(), list));
        YCHECK(pair.second);
        return &pair.first->second;
    }

    TUpdateList* FindUpdateList(TOperationPtr operation)
    {
        auto it = UpdateLists.find(operation->GetId());
        return it == UpdateLists.end() ? nullptr : &it->second;
    }

    TUpdateList* GetUpdateList(TOperationPtr operation)
    {
        auto* result = FindUpdateList(operation);
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
            static_cast<int>(UpdateLists.size()));

        // Issue updates for active operations.
        std::vector<TOperationPtr> finishedOperations;
        auto awaiter = New<TParallelAwaiter>(CancelableControlInvoker);
        for (auto& pair : UpdateLists) {
            auto& list = pair.second;
            auto operation = list.Operation;
            if (operation->IsFinishedState()) {
                finishedOperations.push_back(operation);
            } else {
                LOG_DEBUG("Updating operation node (OperationId: %v)",
                    operation->GetId());

                auto batchReq = StartBatchRequest(&list);
                PrepareOperationUpdate(&list, batchReq);

                awaiter->Await(
                    batchReq->Invoke(),
                    BIND(&TImpl::OnOperationNodeUpdated, MakeStrong(this), operation));
            }
        }

        awaiter->Complete(BIND(&TImpl::OnOperationNodesUpdated, MakeStrong(this)));

        // Cleanup finished operations.
        for (auto operation : finishedOperations) {
            RemoveUpdateList(operation);
        }
    }

    void OnOperationNodeUpdated(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto error = GetOperationNodeUpdateError(operation, batchRsp);
        if (!error.IsOK()) {
            LOG_ERROR(error);
            Disconnect();
            return;
        }

        LOG_DEBUG("Operation node updated (OperationId: %v)",
            operation->GetId());
    }

    void OnOperationNodesUpdated()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_INFO("Operation nodes updated");

        OperationNodesUpdateExecutor->ScheduleNext();
    }


    void PrepareOperationUpdate(
        TOperationPtr operation,
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        auto state = operation->GetState();
        auto operationPath = GetOperationPath(operation->GetId());
        auto controller = operation->GetController();

        // Set state.
        {
            auto req = TYPathProxy::Set(operationPath + "/@state");
            req->set_value(ConvertToYsonString(operation->GetState()).Data());
            batchReq->AddRequest(req, "update_op_node");
        }

        // Set suspended flag.
        {
            auto req = TYPathProxy::Set(operationPath + "/@suspended");
            req->set_value(ConvertToYsonString(operation->GetSuspended()).Data());
            batchReq->AddRequest(req, "update_op_node");
        }

        if ((state == EOperationState::Running || IsOperationFinished(state)) && controller) {
            // Set progress.
            {
                auto req = TYPathProxy::Set(operationPath + "/@progress");
                req->set_value(BuildYsonStringFluently()
                    .BeginMap()
                        .Do(BIND(&IOperationController::BuildProgress, controller))
                    .EndMap().Data());
                batchReq->AddRequest(req, "update_op_node");

            }
            // Set brief progress.
            {
                auto req = TYPathProxy::Set(operationPath + "/@brief_progress");
                req->set_value(BuildYsonStringFluently()
                    .BeginMap()
                        .Do(BIND(&IOperationController::BuildBriefProgress, controller))
                    .EndMap().Data());
                batchReq->AddRequest(req, "update_op_node");
            }
        }

        // Set result.
        if (operation->IsFinishedState() && controller) {
            auto req = TYPathProxy::Set(operationPath + "/@result");
            req->set_value(ConvertToYsonString(BIND(
                &IOperationController::BuildResult,
                controller)).Data());
            batchReq->AddRequest(req, "update_op_node");
        }

        // Set end time, if given.
        if (operation->GetFinishTime()) {
            auto req = TYPathProxy::Set(operationPath + "/@finish_time");
            req->set_value(ConvertToYsonString(operation->GetFinishTime().Get()).Data());
            batchReq->AddRequest(req, "update_op_node");
        }
    }

    void PrepareOperationUpdate(
        TUpdateList* list,
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        auto operation = list->Operation;

        PrepareOperationUpdate(operation, batchReq);

        // Create jobs.
        {
            auto& requests = list->JobRequests;
            for (const auto& request : requests) {
                auto job = request.Job;
                auto operation = job->GetOperation();
                auto jobPath = GetJobPath(operation->GetId(), job->GetId());

                {
                    auto req = TYPathProxy::Set(jobPath);
                    req->set_value(BuildYsonStringFluently()
                        .BeginAttributes()
                        .Do(BIND(&BuildJobAttributes, job))
                        .EndAttributes()
                        .BeginMap()
                        .EndMap()
                        .Data());
                    batchReq->AddRequest(req, "update_op_node");
                }

                if (request.StderrChunkId != NullChunkId) {
                    auto stderrPath = GetStderrPath(operation->GetId(), job->GetId());

                    auto req = TCypressYPathProxy::Create(stderrPath);
                    GenerateMutationId(req);
                    req->set_type(EObjectType::File);

                    auto attributes = CreateEphemeralAttributes();
                    attributes->Set("vital", false);
                    attributes->Set("replication_factor", 1);
                    attributes->Set("account", TmpAccountName);
                    ToProto(req->mutable_node_attributes(), *attributes);

                    auto* reqExt = req->MutableExtension(NFileClient::NProto::TReqCreateFileExt::create_file_ext);
                    ToProto(reqExt->mutable_chunk_id(), request.StderrChunkId);

                    batchReq->AddRequest(req, "create_stderr");
                }

                bool existNotNullFailContext = false;
                for (const auto& failContextChunkId : request.FailContextChunkIds) {
                    if (failContextChunkId != NChunkServer::NullChunkId) {
                        existNotNullFailContext = true;
                    }
                }

                if (existNotNullFailContext) {
                    {
                        auto failContextRootPath = GetFailContextRootPath(operation->GetId(), job->GetId());
                        auto req = TYPathProxy::Set(failContextRootPath);
                        req->set_value(BuildYsonStringFluently()
                            .BeginMap()
                            .EndMap()
                            .Data());
                        batchReq->AddRequest(req, "update_op_node");
                    }

                    size_t index = 0;
                    for (const auto& failContextChunkId : request.FailContextChunkIds) {
                        if (failContextChunkId != NChunkServer::NullChunkId) {
                            auto failContextPath = GetFailContextPath(operation->GetId(), job->GetId(), index);

                            auto req = TCypressYPathProxy::Create(failContextPath);
                            GenerateMutationId(req);
                            req->set_type(EObjectType::File);

                            auto attributes = CreateEphemeralAttributes();
                            attributes->Set("vital", false);
                            attributes->Set("replication_factor", 1);
                            attributes->Set("account", TmpAccountName);
                            ToProto(req->mutable_node_attributes(), *attributes);

                            auto* reqExt = req->MutableExtension(NFileClient::NProto::TReqCreateFileExt::create_file_ext);
                            ToProto(reqExt->mutable_chunk_id(), failContextChunkId);

                            batchReq->AddRequest(req, "create_fail_context");
                        }
                        ++index;
                    }
                }
            }
            requests.clear();
        }

        // Attach live preview chunks.
        {
            auto& requests = list->LivePreviewRequests;

            // Sort by chunk list.
            std::sort(
                requests.begin(),
                requests.end(),
                [] (const TLivePreviewRequest& lhs, const TLivePreviewRequest& rhs) {
                    return lhs.ChunkListId < rhs.ChunkListId;
                });

            // Group by chunk list.
            int rangeBegin = 0;
            while (rangeBegin < static_cast<int>(requests.size())) {
                int rangeEnd = rangeBegin; // non-inclusive
                while (rangeEnd < static_cast<int>(requests.size()) &&
                       requests[rangeBegin].ChunkListId == requests[rangeEnd].ChunkListId)
                {
                    ++rangeEnd;
                }

                auto req = TChunkListYPathProxy::Attach(FromObjectId(requests[rangeBegin].ChunkListId));
                GenerateMutationId(req);
                for (int index = rangeBegin; index < rangeEnd; ++index) {
                    ToProto(req->add_children_ids(), requests[index].ChildId);
                }
                batchReq->AddRequest(req, "update_live_preview");

                rangeBegin = rangeEnd;
            }
            requests.clear();
        }
    }

    TError GetOperationNodeUpdateError(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto operationId = operation->GetId();

        if (!batchRsp->IsOK()) {
            return TError(
                "Error updating operation node (OperationId: %v)",
                operationId)
                << batchRsp->GetError();
        }

        {
            auto rsps = batchRsp->GetResponses("update_op_node");
            for (auto rsp : rsps) {
                if (!rsp->IsOK()) {
                    return TError(
                        "Error updating operation node (OperationId: %v)",
                        operationId)
                        << rsp->GetError();
                }
            }
        }

        // NB: Here we silently ignore (but still log down) create_stderr and update_live_preview failures.
        // These requests may fail due to user transaction being aborted.
        {
            auto rsps = batchRsp->GetResponses("create_stderr");
            for (auto rsp : rsps) {
                if (!rsp->IsOK()) {
                    LOG_WARNING(
                        rsp->GetError(),
                        "Error creating stderr node (OperationId: %v)",
                        operationId);
                }
            }
        }

        {
            auto rsps = batchRsp->GetResponses("create_fail_context");
            for (const auto& rsp: rsps) {
                if (!rsp->IsOK()) {
                    LOG_WARNING(
                        rsp->GetError(),
                        "Error creating fail context node (OperationId: %v)",
                        operationId);
                }
            }
        }

        {
            auto rsps = batchRsp->GetResponses("update_live_preview");
            for (auto rsp : rsps) {
                if (!rsp->IsOK()) {
                    LOG_WARNING(
                        rsp->GetError(),
                        "Error updating live preview (OperationId: %v)",
                        operationId);
                }
            }
        }

        return TError();
    }

    TError OnOperationNodeCreated(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();
        auto error = batchRsp->GetCumulativeError();

        if (!error.IsOK()) {
            auto wrappedError = TError("Error creating operation node (OperationId: %v)",
                operationId)
                << error;
            LOG_WARNING(wrappedError);
            return wrappedError;
        }

        LOG_INFO("Operation node created (OperationId: %v)",
            operationId);

        return TError();
    }

    TError OnRevivingOperationNodeReset(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto operationId = operation->GetId();

        auto error = batchRsp->GetCumulativeError();

        if (!error.IsOK()) {
            auto wrappedError = TError("Error resetting reviving operation node (OperationId: %v)",
                operationId)
                << error;
            LOG_ERROR(wrappedError);
            return wrappedError;
        }

        LOG_INFO("Reviving operation node reset (OperationId: %v)",
            operationId);

        return TError();
    }

    void OnOperationNodeFlushed(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto operationId = operation->GetId();

        auto error = GetOperationNodeUpdateError(operation, batchRsp);
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
            auto batchReq = StartBatchRequest();
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

            auto batchReq = StartBatchRequest();
            for (auto requester : list.WatcherRequesters) {
                requester.Run(batchReq);
            }
            batchReq->Invoke().Subscribe(
                BIND(&TImpl::OnOperationWatchersUpdated, MakeStrong(this), operation)
                    .Via(CancelableControlInvoker));
        }

        WatchersExecutor->ScheduleNext();
    }

    void OnGlobalWatchersUpdated(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        if (!batchRsp->IsOK()) {
            LOG_ERROR(*batchRsp, "Error updating global watchers");
            return;
        }

        for (auto handler : GlobalWatcherHandlers) {
            handler.Run(batchRsp);
        }

        LOG_INFO("Global watchers updated");
    }

    void OnOperationWatchersUpdated(TOperationPtr operation, TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        if (!batchRsp->IsOK()) {
            LOG_ERROR(*batchRsp, "Error updating operation watchers (OperationId: %v)",
                operation->GetId());
            return;
        }

        if (operation->GetState() != EOperationState::Running)
            return;

        auto* list = FindWatcherList(operation);
        if (!list)
            return;

        for (auto handler : list->WatcherHandlers) {
            handler.Run(batchRsp);
        }

        LOG_INFO("Operation watchers updated (OperationId: %v)",
            operation->GetId());
    }


    void BuildSnapshot()
    {
        if (!Config->EnableSnapshotBuilding)
            return;

        auto builder = New<TSnapshotBuilder>(
            Config,
            Bootstrap->GetScheduler(),
            Bootstrap->GetMasterClient());
        builder->Run().Subscribe(BIND(&TImpl::OnSnapshotBuilt, MakeWeak(this))
            .Via(CancelableControlInvoker));
    }

    void OnSnapshotBuilt(TError error)
    {
        SnapshotExecutor->ScheduleNext();
    }

    void UpdateClusterDirectory()
    {
        auto this_ = MakeStrong(this);
        GetClusters()
            .Subscribe(BIND([this, this_] (TYPathProxy::TRspGetPtr rsp) {
                OnGotClusters(rsp);
                OnClusterDirectoryUpdated();
            }).Via(CancelableControlInvoker));
    }

    void OnClusterDirectoryUpdated()
    {
        ClusterDirectoryExecutor->ScheduleNext();
    }

    TFuture<TYPathProxy::TRspGetPtr> GetClusters()
    {
        return Proxy.Execute(TYPathProxy::Get("//sys/clusters"));
    }

    void OnGotClusters(TYPathProxy::TRspGetPtr rsp)
    {
        if (rsp->IsOK()) {
            try {
                auto clustersNode = ConvertToNode(TYsonString(rsp->value()))->AsMap();

                for (auto name : ClusterDirectory->GetClusterNames()) {
                    if (!clustersNode->FindChild(name)) {
                        ClusterDirectory->RemoveCluster(name);
                    }
                }

                for (const auto& pair : clustersNode->GetChildren()) {
                    const auto& clusterName = pair.first;
                    auto clusterAttributes = ConvertToAttributes(pair.second);

                    auto cellTag = clusterAttributes->Get<TCellTag>("cell_tag");
                    auto defaultNetwork = clusterAttributes->Find<Stroka>("default_network");
                    auto connectionConfig = clusterAttributes->Get<NApi::TConnectionConfigPtr>("connection");

                    ClusterDirectory->UpdateCluster(clusterName, connectionConfig, cellTag, defaultNetwork);
                }
                LOG_DEBUG("Cell directory updated successfully");
            } catch (const std::exception& ex) {
                LOG_ERROR(TError("Cannot update cell directory") << ex);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    TSchedulerConfigPtr config,
    NCellScheduler::TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TMasterConnector::~TMasterConnector()
{ }

void TMasterConnector::Start()
{
    Impl->Start();
}

bool TMasterConnector::IsConnected() const
{
    return Impl->IsConnected();
}

TAsyncError TMasterConnector::CreateOperationNode(TOperationPtr operation)
{
    return Impl->CreateOperationNode(operation);
}

TAsyncError TMasterConnector::ResetRevivingOperationNode(TOperationPtr operation)
{
    return Impl->ResetRevivingOperationNode(operation);
}

TFuture<void> TMasterConnector::FlushOperationNode(TOperationPtr operation)
{
    return Impl->FlushOperationNode(operation);
}

void TMasterConnector::CreateJobNode(
    TJobPtr job,
    const TChunkId& stderrChunkId,
    const std::vector<TChunkId>& failContextChunkIds)
{
    return Impl->CreateJobNode(job, stderrChunkId, failContextChunkIds);
}

void TMasterConnector::AttachToLivePreview(
    TOperationPtr operation,
    const TChunkListId& chunkListId,
    const TChunkTreeId& childId)
{
    Impl->AttachToLivePreview(operation, chunkListId, childId);
}

void TMasterConnector::AttachToLivePreview(
    TOperationPtr operation,
    const TChunkListId& chunkListId,
    const std::vector<TChunkTreeId>& childrenIds)
{
    Impl->AttachToLivePreview(operation, chunkListId, childrenIds);
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

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

