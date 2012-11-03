#include "stdafx.h"
#include "master_connector.h"
#include "scheduler.h"
#include "private.h"
#include "helpers.h"

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/actions/async_pipeline.h>
#include <ytlib/actions/parallel_awaiter.h>

#include <ytlib/rpc/serialized_channel.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/node.h>

#include <ytlib/scheduler/helpers.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NMetaState;
using namespace NRpc;

using NChunkClient::NullChunkId;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = SchedulerLogger;

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

        auto id = operation->GetOperationId();
        LOG_INFO("Creating operation node (OperationId: %s)",
            ~ToString(id));

        auto* list = CreateUpdateList(operation);

        TObjectServiceProxy proxy(list->SerializedChannel);

        auto setReq = TYPathProxy::Set(GetOperationPath(id));
        setReq->set_value(BuildOperationYson(operation).Data());

        return proxy.Execute(setReq).Apply(
            BIND(&TImpl::OnOperationNodeCreated, MakeStrong(this), operation)
                .AsyncVia(Bootstrap->GetControlInvoker()));
    }

    TFuture<void> FlushOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto id = operation->GetOperationId();
        LOG_INFO("Flushing operation node (OperationId: %s)",
            ~ToString(id));

        auto* list = GetUpdateList(operation);
        YCHECK(list->State == EUpdateListState::Active);
        list->State = EUpdateListState::Flushing;

        // Create a batch update for this particular operation.
        TObjectServiceProxy proxy(list->SerializedChannel);
        auto batchReq = proxy.ExecuteBatch();
        PrepareOperationUpdate(list, batchReq);

        batchReq->Invoke().Apply(
            BIND(&TImpl::OnOperationNodeFlushed, MakeStrong(this), operation)
                .Via(CancelableControlInvoker));

        return list->FlushedPromise;
    }

    TFuture<void> FinalizeOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto id = operation->GetOperationId();
        LOG_INFO("Finalizing operation node (OperationId: %s)",
            ~ToString(id));

        auto* list = GetUpdateList(operation);
        YCHECK(list->State == EUpdateListState::Flushed);
        list->State = EUpdateListState::Finalizing;

        // Create a batch update for this particular operation.
        TObjectServiceProxy proxy(list->SerializedChannel);
        auto batchReq = proxy.ExecuteBatch();
        PrepareOperationUpdate(list, batchReq);

        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnOperationNodeFinalized, MakeStrong(this), operation)
                .Via(CancelableControlInvoker));

        return list->FinalizedPromise;
    }


    void CreateJobNode(TJobPtr job, const NChunkClient::TChunkId& chunkId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Creating job node (OperationId: %s, JobId: %s, StdErrChunkId: %s)",
            ~job->GetOperation()->GetOperationId().ToString(),
            ~job->GetId().ToString(),
            ~chunkId.ToString());

        auto* list = GetUpdateList(job->GetOperation());
        YCHECK(list->State == EUpdateListState::Active);
        list->PendingJobs.insert(std::make_pair(job, chunkId));
    }

    DEFINE_SIGNAL(void(TObjectServiceProxy::TReqExecuteBatchPtr), WatcherRequest);
    DEFINE_SIGNAL(void(TObjectServiceProxy::TRspExecuteBatchPtr), WatcherResponse);
    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);
    DEFINE_SIGNAL(void(TOperationPtr operation), PrimaryTransactionAborted);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;

    bool Connected;

    NTransactionClient::ITransactionPtr LockTransaction;

    TPeriodicInvokerPtr TransactionRefreshInvoker;
    TPeriodicInvokerPtr ExecNodesRefreshInvoker;
    TPeriodicInvokerPtr OperationNodesUpdateInvoker;
    TPeriodicInvokerPtr WatchersInvoker;

    DECLARE_ENUM(EUpdateListState,
        (Active)
        (Flushing)
        (Flushed)
        (Finalizing)
        (Finalized)
    );

    struct TUpdateList
    {
        TUpdateList(IChannelPtr masterChannel, TOperationPtr operation)
            : Operation(operation)
            , State(EUpdateListState::Active)
            , FlushedPromise(NewPromise<void>())
            , FinalizedPromise(NewPromise<void>())
            , SerializedChannel(CreateSerializedChannel(masterChannel))
        { }

        TOperationPtr Operation;
        yhash_map<TJobPtr, TChunkId> PendingJobs;
        EUpdateListState State;
        TPromise<void> FlushedPromise;
        TPromise<void> FinalizedPromise;
        IChannelPtr SerializedChannel;
    };

    yhash_map<TOperationId, TUpdateList> UpdateLists;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void StartConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Connecting to master");

        New<TRegistrationPipeline>(this)
            ->Create()
            ->Run()
            .Subscribe(BIND(&TImpl::OnConnected, MakeStrong(this))
                .Via(Bootstrap->GetControlInvoker()));
    }

    void OnConnected(TValueOrError<TMasterHandshakeResult> resultOrError)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!resultOrError.IsOK()) {
            LOG_ERROR(resultOrError, "Error connecting to master");
            TDelayedInvoker::Submit(
                BIND(&TImpl::StartConnecting, MakeStrong(this))
                    .Via(Bootstrap->GetControlInvoker()),
                Config->ConnectRetryPeriod);
            return;
        }

        LOG_INFO("Master connected");

        YCHECK(!Connected);
        Connected = true;

        CancelableContext = New<TCancelableContext>();
        CancelableControlInvoker = CancelableContext->CreateInvoker(Bootstrap->GetControlInvoker());

        const auto& result = resultOrError.Value();
        CreateUpdateLists(result.Operations);
        WatcherResponse_.Fire(result.WatcherResponses);

        LockTransaction->SubscribeAborted(
            BIND(&TImpl::OnLockTransactionAborted, MakeWeak(this))
                .Via(CancelableControlInvoker));

        StartRefresh();

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
            , Proxy(owner->Bootstrap->GetMasterChannel())
        { }

        TAsyncPipeline<TMasterHandshakeResult>::TPtr Create()
        {
            return StartAsyncPipeline(Owner->Bootstrap->GetControlInvoker())
                ->Add(BIND(&TRegistrationPipeline::Round1, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round2, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round3, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round4, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round5, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round6, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round7, MakeStrong(this)));
        }

    private:
        TIntrusivePtr<TImpl> Owner;
        TObjectServiceProxy Proxy;
        std::vector<TOperationId> OperationIds;
        TMasterHandshakeResult Result;

        // Round 1:
        // - Start lock transaction.
        TObjectServiceProxy::TInvExecuteBatch Round1()
        {
            auto batchReq = Proxy.ExecuteBatch();
            {
                auto req = TTransactionYPathProxy::CreateObject(RootTransactionPath);
                req->set_type(EObjectType::Transaction);
                batchReq->AddRequest(req, "start_lock_tx");
            }
            return batchReq->Invoke();
        }

        // Round 2:
        // - Take lock.
        TObjectServiceProxy::TInvExecuteBatch Round2(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);
            {
                auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_lock_tx");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting lock transaction");
                auto transactionId = TTransactionId::FromProto(rsp->object_id());
                Owner->LockTransaction = Owner->Bootstrap->GetTransactionManager()->Attach(transactionId, true);
                LOG_INFO("Lock transaction is %s", ~ToString(transactionId));
            }

            auto batchReq = Proxy.ExecuteBatch();
            auto schedulerAddress = Owner->Bootstrap->GetPeerAddress();
            {
                auto req = TCypressYPathProxy::Lock("//sys/scheduler/lock");
                SetTransactionId(req, Owner->LockTransaction);
                req->set_mode(ELockMode::Exclusive);
                GenerateRpcMutationId(req);
                batchReq->AddRequest(req, "take_lock");
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@address");
                req->set_value(ConvertToYsonString(TRawString(schedulerAddress)).Data());
                batchReq->AddRequest(req, "set_scheduler_address");
            }
            {
                auto req = TYPathProxy::Set("//sys/scheduler/orchid&/@remote_address");
                req->set_value(ConvertToYsonString(TRawString(schedulerAddress)).Data());
                batchReq->AddRequest(req, "set_orchid_address");
            }
            return batchReq->Invoke();
        }

        // Round 3:
        // - Publish scheduler address.
        // - Update orchid address.
        // - Request operations and their states.
        TObjectServiceProxy::TInvExecuteBatch Round3(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);
            {
                auto rsp = batchRsp->GetResponse("take_lock");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error taking lock");
                LOG_INFO("Scheduler lock taken");
            }
            {
                auto rsp = batchRsp->GetResponse("set_scheduler_address");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error setting scheduler address");
                LOG_INFO("Scheduler address set");
            }
            {
                auto rsp = batchRsp->GetResponse("set_orchid_address");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error setting orchid address");
                LOG_INFO("Orchid address set");
            }

            auto batchReq = Proxy.ExecuteBatch();
            {
                auto req = TYPathProxy::List(GetOperationsPath());
                auto* attributeFilter = req->mutable_attribute_filter();
                attributeFilter->set_mode(EAttributeFilterMode::MatchingOnly);
                attributeFilter->add_keys("state");
                batchReq->AddRequest(req, "list_operations");
            }
            return batchReq->Invoke();
        }

        // Round 4:
        // - Request attributes for unfinished operations.
        TObjectServiceProxy::TInvExecuteBatch Round4(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);
            {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspList>("list_operations");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting operations list");
                auto operationsListNode = ConvertToNode(TYsonString(rsp->keys()));
                auto operationsList = operationsListNode->AsList();
                LOG_INFO("Operations list received, %d operations total",
                    static_cast<int>(operationsList->GetChildCount()));
                OperationIds.clear();
                FOREACH (auto operationNode, operationsList->GetChildren()) {
                    auto id = TOperationId::FromString(operationNode->GetValue<Stroka>());
                    auto state = operationNode->Attributes().Get<EOperationState>("state");
                    if (state == EOperationState::Initializing ||
                        state == EOperationState::Preparing ||
                        state == EOperationState::Reviving ||
                        state == EOperationState::Running)
                    {
                        OperationIds.push_back(id);
                    }
                }
            }

            auto batchReq = Proxy.ExecuteBatch();
            {
                LOG_INFO("Fetching attributes for %d unfinished operations",
                    static_cast<int>(OperationIds.size()));
                FOREACH (const auto& operationId, OperationIds) {
                    auto req = TYPathProxy::Get(GetOperationPath(operationId));
                    // Keep in sync with ParseOperationYson.
                    auto* attributeFilter = req->mutable_attribute_filter();
                    attributeFilter->set_mode(EAttributeFilterMode::MatchingOnly);
                    attributeFilter->add_keys("operation_type");
                    attributeFilter->add_keys("transaction_id");
                    attributeFilter->add_keys("spec");
                    attributeFilter->add_keys("start_time");
                    attributeFilter->add_keys("state");
                    batchReq->AddRequest(req, "get_op_attr");
                }
            }
            return batchReq->Invoke();
        }

        // Round 5:
        // - Reset operation nodes.
        TObjectServiceProxy::TInvExecuteBatch Round5(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);

            {
                auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_op_attr");
                YCHECK(rsps.size() == OperationIds.size());

                for (int index = 0; index < static_cast<int>(rsps.size()); ++index) {
                    const auto& operationId = OperationIds[index];
                    auto rsp = rsps[index];
                    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting operation attributes (OperationId: %s)",
                        ~ToString(operationId));
                    auto operationNode = ConvertToNode(TYsonString(rsp->value()));
                    auto operation = ParseOperationYson(operationId, operationNode->Attributes());
                    Result.Operations.push_back(operation);
                }
            }

            auto batchReq = Proxy.ExecuteBatch();
            FOREACH (auto operation, Result.Operations) {
                operation->SetState(EOperationState::Reviving);

                auto req = TYPathProxy::Set(GetOperationPath(operation->GetOperationId()));
                req->set_value(BuildOperationYson(operation).Data());
                batchReq->AddRequest(req, "reset_op");
            }

            return batchReq->Invoke();
        }

        // Round 6:
        // - Watcher requests.
        TObjectServiceProxy::TInvExecuteBatch Round6(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);

            auto batchReq = Proxy.ExecuteBatch();
            Owner->WatcherRequest_.Fire(batchReq);
            return batchReq->Invoke();
        }

        // Round 7:
        // - Relax :)
        TMasterHandshakeResult Round7(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
        {
            auto error = batchRsp->GetCumulativeError();
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
            Result.WatcherResponses = batchRsp;
            return Result;
        }

    };


    void Disconnect()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Connected)
            return;

        LOG_WARNING("Master disconnected");

        Connected = false;
        ClearUpdateLists();
        StopRefresh();
        CancelableContext->Cancel();

        MasterDisconnected_.Fire();

        StartConnecting();
    }


    static TYsonString BuildOperationYson(TOperationPtr operation)
    {
        return BuildYsonStringFluently()
            .BeginAttributes()
                .Do(BIND(&BuildOperationAttributes, operation))
                .Item("progress").BeginMap().EndMap()
                .Item("opaque").Scalar("true")
            .EndAttributes()
            .BeginMap()
                .Item("jobs").BeginAttributes()
                    .Item("opaque").Scalar("true")
                .EndAttributes()
                .BeginMap()
                .EndMap()
            .EndMap();
    }

    static TOperationPtr ParseOperationYson(const TOperationId& operationId, const IAttributeDictionary& attributes)
    {
        return New<TOperation>(
            operationId,
            attributes.Get<EOperationType>("operation_type"),
            attributes.Get<TTransactionId>("transaction_id"),
            attributes.Get<INodePtr>("spec")->AsMap(),
            attributes.Get<TInstant>("start_time"),
            attributes.Get<EOperationState>("state"));
    }

    static TYsonString BuildJobYson(TJobPtr job)
    {
        return BuildYsonStringFluently()
            .BeginAttributes()
                .Do(BIND(&BuildJobAttributes, job))
            .EndAttributes()
            .BeginMap()
            .EndMap();
    }

    static TYsonString BuildJobAttributesYson(TJobPtr job)
    {
        return BuildYsonStringFluently()
            .BeginMap()
                .Do(BIND(&BuildJobAttributes, job))
            .EndMap();
    }


    void StartRefresh()
    {
        TransactionRefreshInvoker = New<TPeriodicInvoker>(
            CancelableControlInvoker,
            BIND(&TImpl::RefreshTransactions, MakeWeak(this)),
            Config->TransactionsRefreshPeriod);
        TransactionRefreshInvoker->Start();

        OperationNodesUpdateInvoker = New<TPeriodicInvoker>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateOperationNodes, MakeWeak(this)),
            Config->OperationsUpdatePeriod);
        OperationNodesUpdateInvoker->Start();

        WatchersInvoker = New<TPeriodicInvoker>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateWatchers, MakeWeak(this)),
            Config->WatchersUpdatePeriod);
        WatchersInvoker->Start();
    }

    void StopRefresh()
    {
        if (TransactionRefreshInvoker) {
            TransactionRefreshInvoker->Stop();
            TransactionRefreshInvoker.Reset();
        }

        if (ExecNodesRefreshInvoker) {
            ExecNodesRefreshInvoker->Stop();
            ExecNodesRefreshInvoker.Reset();
        }

        if (OperationNodesUpdateInvoker) {
            OperationNodesUpdateInvoker->Stop();
            OperationNodesUpdateInvoker.Reset();
        }

        if (WatchersInvoker) {
            WatchersInvoker->Stop();
            WatchersInvoker.Reset();
        }
    }


    void RefreshTransactions()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        // Collect all transactions that are used by currently running operations.
        yhash_set<TTransactionId> transactionIdsSet;
        auto operations = Bootstrap->GetScheduler()->GetOperations();
        FOREACH (auto operation, operations) {
            auto transactionId = operation->GetTransactionId();
            if (transactionId != NullTransactionId) {
                transactionIdsSet.insert(transactionId);
            }
        }

        // Invoke GetId verbs for these transactions to see if they are alive.
        std::vector<TTransactionId> transactionIdsList;
        TObjectServiceProxy proxy(Bootstrap->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();
        FOREACH (const auto& id, transactionIdsSet) {
            auto checkReq = TObjectYPathProxy::GetId(FromObjectId(id));
            transactionIdsList.push_back(id);
            batchReq->AddRequest(checkReq, "check_tx");
        }

        LOG_INFO("Refreshing %d transactions",
            static_cast<int>(transactionIdsList.size()));

        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnTransactionsRefreshed, MakeStrong(this), transactionIdsList)
                .Via(CancelableControlInvoker));
    }

    void OnTransactionsRefreshed(
        const std::vector<TTransactionId>& transactionIds,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        TransactionRefreshInvoker->ScheduleNext();

        if (!batchRsp->IsOK()) {
            LOG_ERROR(*batchRsp, "Error refreshing transactions");
            Disconnect();
            return;
        }

        LOG_INFO("Transactions refreshed");

        // Collect the list of dead transactions.
        auto rsps = batchRsp->GetResponses("check_tx");
        yhash_set<TTransactionId> deadTransactionIds;
        for (int index = 0; index < static_cast<int>(rsps.size()); ++index) {
            if (!batchRsp->GetResponse(index)->IsOK()) {
                YCHECK(deadTransactionIds.insert(transactionIds[index]).second);
            }
        }

        // Collect the list of operations corresponding to dead transactions.
        auto operations = Bootstrap->GetScheduler()->GetOperations();
        FOREACH (auto operation, operations) {
            if (deadTransactionIds.find(operation->GetTransactionId()) != deadTransactionIds.end()) {
                PrimaryTransactionAborted_.Fire(operation);
            }
        }
    }

    TUpdateList* CreateUpdateList(TOperationPtr operation)
    {
        TUpdateList list(Bootstrap->GetMasterChannel(), operation);
        auto pair = UpdateLists.insert(std::make_pair(operation->GetOperationId(), list));
        YCHECK(pair.second);
        return &pair.first->second;
    }

    void CreateUpdateLists(const std::vector<TOperationPtr>& operations)
    {
        FOREACH (auto operation, operations) {
            CreateUpdateList(operation);
        }
    }

    TUpdateList* GetUpdateList(TOperationPtr operation)
    {
        auto it = UpdateLists.find(operation->GetOperationId());
        YCHECK(it != UpdateLists.end());
        return &it->second;
    }

    void RemoveUpdateList(TOperationPtr operation)
    {
        YCHECK(UpdateLists.erase(operation->GetOperationId()));
    }

    void ClearUpdateLists()
    {
        UpdateLists.clear();
    }


    void UpdateOperationNodes()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_INFO("Updating operation nodes");

        auto awaiter = New<TParallelAwaiter>(CancelableControlInvoker);

        FOREACH (auto& pair, UpdateLists) {
            auto& list = pair.second;
            auto operation = list.Operation;
            if (list.State == EUpdateListState::Active) {
                LOG_DEBUG("Updating operation node (OperationId: %s)",
                    ~ToString(operation->GetOperationId()));

                TObjectServiceProxy proxy(list.SerializedChannel);
                auto batchReq = proxy.ExecuteBatch();
                PrepareOperationUpdate(&list, batchReq);

                awaiter->Await(
                    batchReq->Invoke(),
                    BIND(&TImpl::OnOperationNodeUpdated, MakeStrong(this), operation));
            }
        }

        awaiter->Complete(BIND(&TImpl::OnOperationNodesUpdated, MakeStrong(this)));
    }

    void OnOperationNodeUpdated(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto error = batchRsp->GetCumulativeError();
        if (!error.IsOK()) {
            LOG_ERROR(error, "Error updating operation node (OperationId: %s)",
                ~ToString(operation->GetOperationId()));
            Disconnect();
            return;
        }

        LOG_DEBUG("Operation node updated (OperationId: %s)",
            ~ToString(operation->GetOperationId()));
    }

    void OnOperationNodesUpdated()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_INFO("Operation nodes updated");

        OperationNodesUpdateInvoker->ScheduleNext();
    }


    void PrepareOperationUpdate(
        TUpdateList* list,
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        auto operation = list->Operation;
        auto state = operation->GetState();
        auto operationPath = GetOperationPath(operation->GetOperationId());

        // Set state.
        {
            auto req = TYPathProxy::Set(operationPath + "/@state");
            req->set_value(ConvertToYsonString(operation->GetState()).Data());
            batchReq->AddRequest(req);
        }

        // Set progress.
        if (state == EOperationState::Running || operation->IsFinishedState()) {
            auto req = TYPathProxy::Set(operationPath + "/@progress");
            req->set_value(BuildYsonStringFluently()
                .BeginMap()
                    .Do(BIND(&IOperationController::BuildProgressYson, operation->GetController()))
                .EndMap().Data());
            batchReq->AddRequest(req);
        }

        // Set result.
        if (operation->IsFinishedState()) {
            auto req = TYPathProxy::Set(operationPath + "/@result");
            req->set_value(ConvertToYsonString(BIND(
                &IOperationController::BuildResultYson,
                operation->GetController())).Data());
            batchReq->AddRequest(req);
        }

        // Set end time, if given.
        if (operation->GetEndTime()) {
            auto req = TYPathProxy::Set(operationPath + "/@end_time");
            req->set_value(ConvertToYsonString(operation->GetEndTime().Get()).Data());
            batchReq->AddRequest(req);
        }

        // Create jobs.
        FOREACH (const auto& pair, list->PendingJobs) {
            auto job = pair.first;
            auto chunkId = pair.second;
            auto jobPath = GetJobPath(operation->GetOperationId(), job->GetId());
            auto req = TYPathProxy::Set(jobPath);
            req->set_value(BuildJobYson(job).Data());
            batchReq->AddRequest(req);

            if (chunkId != NullChunkId) {
                auto stdErrPath = GetStdErrPath(operation->GetOperationId(), job->GetId());
                auto req = TCypressYPathProxy::Create(stdErrPath);
                req->set_type(EObjectType::File);
                auto* reqExt = req->MutableExtension(NFileClient::NProto::TReqCreateFileExt::create_file);
                *reqExt->mutable_chunk_id() = chunkId.ToProto();
                GenerateRpcMutationId(req);
                batchReq->AddRequest(req);
            }
        }
        list->PendingJobs.clear();
    }


    TError OnOperationNodeCreated(
        TOperationPtr operation,
        TYPathProxy::TRspSetPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetOperationId();
        auto error = rsp->GetError();

        if (!error.IsOK()) {
            LOG_ERROR(error, "Error creating operation node (OperationId: %s)",
                ~operationId.ToString());
            RemoveUpdateList(operation);
            return error;
        }

        LOG_INFO("Operation node created (OperationId: %s)",
            ~operationId.ToString());

        return TError();
    }

    void OnOperationNodeFlushed(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetOperationId();

        auto error = batchRsp->GetCumulativeError();
        if (!error.IsOK()) {
            LOG_ERROR(error, "Error flushing operation node (OperationId: %s)",
                ~operationId.ToString());
            Disconnect();
            return;
        }

        LOG_INFO("Operation node flushed (OperationId: %s)",
            ~operationId.ToString());
       
        auto* list = GetUpdateList(operation);
        YCHECK(list->State == EUpdateListState::Flushing);
        list->State = EUpdateListState::Flushed;
        list->FlushedPromise.Set();
    }

    void OnOperationNodeFinalized(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetOperationId();

        auto error = batchRsp->GetCumulativeError();
        if (!error.IsOK()) {
            LOG_ERROR(error, "Error finalizing operation node (OperationId: %s)",
                ~operationId.ToString());
            Disconnect();
            return;
        }

        LOG_INFO("Operation node finalized (OperationId: %s)",
            ~operationId.ToString());

        auto* list = GetUpdateList(operation);
        YCHECK(list->State == EUpdateListState::Finalizing);
        list->State = EUpdateListState::Finalized; 
        list->FinalizedPromise.Set();

        RemoveUpdateList(operation);
    }


    void UpdateWatchers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_INFO("Updating watchers");

        // Create a batch update for all operations.
        TObjectServiceProxy proxy(Bootstrap->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();
        WatcherRequest_.Fire(batchReq);
        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnWatchersUpdated, MakeStrong(this))
                .Via(CancelableControlInvoker));
    }

    void OnWatchersUpdated(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        WatchersInvoker->ScheduleNext();

        auto error = batchRsp->GetCumulativeError();
        if (!error.IsOK()) {
            LOG_ERROR(error, "Error updating watchers");
            return;
        }

        WatcherResponse_.Fire(batchRsp);

        LOG_INFO("Watchers updated");
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

TFuture<void> TMasterConnector::FlushOperationNode(TOperationPtr operation)
{
    return Impl->FlushOperationNode(operation);
}

TFuture<void> TMasterConnector::FinalizeOperationNode(TOperationPtr operation)
{
    return Impl->FinalizeOperationNode(operation);
}

void TMasterConnector::CreateJobNode(TJobPtr job, const NChunkClient::TChunkId& chunkId)
{
    return Impl->CreateJobNode(job, chunkId);
}

DELEGATE_SIGNAL(TMasterConnector, void(TObjectServiceProxy::TReqExecuteBatchPtr), WatcherRequest, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(TObjectServiceProxy::TRspExecuteBatchPtr), WatcherResponse, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(const TMasterHandshakeResult& result), MasterConnected, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterDisconnected, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(TOperationPtr operation), PrimaryTransactionAborted, *Impl)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

