#include "stdafx.h"
#include "master_connector.h"
#include "scheduler.h"
#include "private.h"
#include "helpers.h"

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/actions/async_pipeline.h>

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
        , ObjectProxy(bootstrap->GetMasterChannel())
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

        CreateUpdateList(operation);

        auto setReq = TYPathProxy::Set(GetOperationPath(id));
        setReq->set_value(BuildOperationYson(operation).Data());
        return ObjectProxy.Execute(setReq).Apply(
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

        // Create a batch update for this particular operation.
        auto batchReq = ObjectProxy.ExecuteBatch();
        PrepareOperationUpdate(list, batchReq);

        batchReq->Invoke().Apply(
            BIND(&TImpl::OnOperationNodeFlushed, MakeStrong(this), operation)
                .Via(CancelableControlInvoker));

        return list->Flushed;
    }

    TFuture<void> FinalizeOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        auto id = operation->GetOperationId();
        LOG_INFO("Finalizing operation node (OperationId: %s)",
            ~ToString(id));

        auto* list = GetUpdateList(operation);
        YCHECK(!list->FinalizationPending);
        list->FinalizationPending = true;

        // Create a batch update for this particular operation.
        auto batchReq = ObjectProxy.ExecuteBatch();
        PrepareOperationUpdate(list, batchReq);

        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnOperationNodeFinalized, MakeStrong(this), operation)
                .Via(CancelableControlInvoker));

        return list->Finalized;
    }


    void CreateJobNode(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Creating job node (OperationId: %s, JobId: %s)",
            ~job->GetOperation()->GetOperationId().ToString(),
            ~job->GetId().ToString());

        auto* list = GetUpdateList(job->GetOperation());
        YCHECK(!list->FinalizationPending);
        YCHECK(list->PendingJobCreations.insert(job).second);
    }

    void UpdateJobNode(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        LOG_DEBUG("Updating job node (OperationId: %s, JobId: %s)",
            ~job->GetOperation()->GetOperationId().ToString(),
            ~job->GetId().ToString());

        auto* list = GetUpdateList(job->GetOperation());
        YCHECK(!list->FinalizationPending);
        list->PendingJobUpdates.insert(job);
    }

    void SetJobStdErr(TJobPtr job, const NChunkClient::TChunkId& chunkId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);
        YCHECK(chunkId != NullChunkId);

        LOG_DEBUG("Setting job stderr (OperationId: %s, JobId: %s, StdErrChunkId: %s)",
            ~job->GetOperation()->GetOperationId().ToString(),
            ~job->GetId().ToString(),
            ~chunkId.ToString());

        auto* list = GetUpdateList(job->GetOperation());
        YCHECK(!list->FinalizationPending);
        list->PendingStdErrChunkIds.insert(std::make_pair(job, chunkId));
    }


    DEFINE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DEFINE_SIGNAL(void(), MasterDisconnected);
    DEFINE_SIGNAL(void(TOperationPtr operation), PrimaryTransactionAborted);
    DEFINE_SIGNAL(void(const Stroka& address), NodeOnline);
    DEFINE_SIGNAL(void(const Stroka& address), NodeOffline);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;

    bool Connected;

    NObjectClient::TObjectServiceProxy ObjectProxy;

    NTransactionClient::ITransactionPtr LockTransaction;

    TPeriodicInvokerPtr TransactionRefreshInvoker;
    TPeriodicInvokerPtr ExecNodesRefreshInvoker;
    TPeriodicInvokerPtr OperationNodesUpdateInvoker;

    struct TOperationUpdateList
    {
        explicit TOperationUpdateList(TOperationPtr operation)
            : Operation(operation)
            , FinalizationPending(false)
            , Flushed(NewPromise<void>())
            , Finalized(NewPromise<void>())
        { }

        TOperationPtr Operation;
        yhash_set<TJobPtr> PendingJobCreations;
        yhash_set<TJobPtr> PendingJobUpdates;
        yhash_map<TJobPtr, TChunkId> PendingStdErrChunkIds;
        bool FinalizationPending;
        TPromise<void> Flushed;
        TPromise<void> Finalized;
    };

    yhash_map<TOperationId, TOperationUpdateList> UpdateLists;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void StartConnecting()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Connecting to master");

        New<TRegistrationPipeline>(this)
            ->Create()
            ->Run()
            .Subscribe(BIND(&TImpl::OnConnected, MakeStrong(this)));
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
        UpdateExecNodes(result.ExecNodeAddresses);
        CreateUpdateLists(result.Operations);

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
        { }

        TAsyncPipeline<TMasterHandshakeResult>::TPtr Create()
        {
            return StartAsyncPipeline(Owner->Bootstrap->GetControlInvoker())
                ->Add(BIND(&TRegistrationPipeline::Round1, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round2, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round3, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round4, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round5, MakeStrong(this)))
                ->Add(BIND(&TRegistrationPipeline::Round6, MakeStrong(this)));
        }

    private:
        TIntrusivePtr<TImpl> Owner;
        std::vector<TOperationId> OperationIds;
        TMasterHandshakeResult Result;

        // Round 1:
        // - Start lock transaction.
        TObjectServiceProxy::TInvExecuteBatch Round1()
        {
            auto batchReq = Owner->ObjectProxy.ExecuteBatch();
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

            auto batchReq = Owner->ObjectProxy.ExecuteBatch();
            auto schedulerAddress = Owner->Bootstrap->GetPeerAddress();
            {
                auto req = TCypressYPathProxy::Lock(WithTransaction(
                    "//sys/scheduler/lock",
                    Owner->LockTransaction->GetId()));
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
        // - Request online node addresses.
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

            auto batchReq = Owner->ObjectProxy.ExecuteBatch();
            {
                auto req = TYPathProxy::List(GetOperationsPath());
                req->add_attributes("state");
                batchReq->AddRequest(req, "list_operations");
            }
            {
                auto req = TYPathProxy::Get("//sys/holders/@online");
                batchReq->AddRequest(req, "get_online_nodes");
                
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
            {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_online_nodes");
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting online nodes");
                Result.ExecNodeAddresses = ConvertTo< std::vector<Stroka> >(TYsonString(rsp->value()));
                LOG_INFO("Exec nodes received, %d nodes found",
                    static_cast<int>(Result.ExecNodeAddresses.size()));
            }

            auto batchReq = Owner->ObjectProxy.ExecuteBatch();
            {
                LOG_INFO("Fetching attributes for %d unfinished operations",
                    static_cast<int>(OperationIds.size()));
                FOREACH (const auto& operationId, OperationIds) {
                    auto req = TYPathProxy::Get(GetOperationPath(operationId));
                    // Keep in sync with ParseOperationYson.
                    req->add_attributes("operation_type");
                    req->add_attributes("transaction_id");
                    req->add_attributes("spec");
                    req->add_attributes("start_time");
                    req->add_attributes("state");
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

            auto batchReq = Owner->ObjectProxy.ExecuteBatch();
            FOREACH (auto operation, Result.Operations) {
                operation->SetState(EOperationState::Reviving);

                auto req = TYPathProxy::Set(GetOperationPath(operation->GetOperationId()));
                req->set_value(BuildOperationYson(operation).Data());
                batchReq->AddRequest(req, "reset_op");
            }

            return batchReq->Invoke();
        }

        // Round 6:
        // - Relax :)
        TMasterHandshakeResult Round6(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp);

            {
                auto rsps = batchRsp->GetResponses("reset_op");
                YCHECK(rsps.size() == OperationIds.size());
                for (int index = 0; index < static_cast<int>(rsps.size()); ++index) {
                    const auto& operationId = OperationIds[index];
                    auto rsp = rsps[index];
                    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error resetting operation node (OperationId: %s)",
                        ~ToString(operationId));
                }
            }

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
        return
            BuildYsonFluently()
                .BeginAttributes()
                    .Do(BIND(&BuildOperationAttributes, operation))
                    .Item("opaque").Scalar("true")
                .EndAttributes()
                .BeginMap()
                    .Item("jobs").BeginAttributes()
                        .Item("opaque").Scalar("true")
                    .EndAttributes()
                    .BeginMap()
                    .EndMap()
                .EndMap().GetYsonString();
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
        return
            BuildYsonFluently()
                .BeginAttributes()
                    .Do(BIND(&BuildJobAttributes, job))
                .EndAttributes()
                .BeginMap()
                .EndMap().GetYsonString();
    }

    static TYsonString BuildJobAttributesYson(TJobPtr job)
    {
        return
            BuildYsonFluently()
                .BeginMap()
                    .Do(BIND(&BuildJobAttributes, job))
                .EndMap().GetYsonString();
    }


    void StartRefresh()
    {
        TransactionRefreshInvoker = New<TPeriodicInvoker>(
            CancelableControlInvoker,
            BIND(&TImpl::RefreshTransactions, MakeWeak(this)),
            Config->TransactionsRefreshPeriod);
        TransactionRefreshInvoker->Start();

        ExecNodesRefreshInvoker = New<TPeriodicInvoker>(
            CancelableControlInvoker,
            BIND(&TImpl::RefreshExecNodes, MakeWeak(this)),
            Config->NodesRefreshPeriod);
        ExecNodesRefreshInvoker->Start();

        OperationNodesUpdateInvoker = New<TPeriodicInvoker>(
            CancelableControlInvoker,
            BIND(&TImpl::UpdateOperationNodes, MakeWeak(this)),
            Config->OperationsUpdatePeriod);
        OperationNodesUpdateInvoker->Start();
    }

    void StopRefresh()
    {
        TransactionRefreshInvoker->Stop();
        ExecNodesRefreshInvoker->Stop();
        OperationNodesUpdateInvoker->Stop();
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
        auto batchReq = ObjectProxy.ExecuteBatch();
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
        NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
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


    void RefreshExecNodes()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        // Get the list of online nodes from the master.
        LOG_INFO("Refreshing exec nodes");
        auto req = TYPathProxy::Get("//sys/holders/@online");
        ObjectProxy.Execute(req).Subscribe(
            BIND(&TImpl::OnExecNodesRefreshed, MakeStrong(this))
                .Via(CancelableControlInvoker));
    }

    void OnExecNodesRefreshed(NYTree::TYPathProxy::TRspGetPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        ExecNodesRefreshInvoker->ScheduleNext();

        if (!rsp->IsOK()) {
            LOG_ERROR(*rsp, "Error refreshing exec nodes");
            Disconnect();
            return;
        }

        auto onlineAddresses = ConvertTo< std::vector<Stroka> >(TYsonString(rsp->value()));
        LOG_INFO("Exec nodes refreshed, %d nodes found",
            static_cast<int>(onlineAddresses.size()));

        UpdateExecNodes(onlineAddresses);
    }

    void UpdateExecNodes(const std::vector<Stroka>& onlineAddresses)
    {
        // Examine the list of nodes returned by master and figure out the difference.

        yhash_set<Stroka> nodeAddresses;
        auto nodes = Bootstrap->GetScheduler()->GetExecNodes();
        FOREACH (auto node, nodes) {
            YCHECK(nodeAddresses.insert(node->GetAddress()).second);
        }

        FOREACH (const auto& address, onlineAddresses) {
            auto it = nodeAddresses.find(address);
            if (it == nodeAddresses.end()) {
                LOG_INFO("Node %s is online", ~address.Quote());
                NodeOnline_.Fire(address);
            } else {
                nodeAddresses.erase(it);
            }
        }

        FOREACH (const auto& address, nodeAddresses) {
            LOG_INFO("Node %s is offline", ~address);
            NodeOffline_.Fire(address);
        }
    }


    void CreateUpdateList(TOperationPtr operation)
    {
        YCHECK(UpdateLists.insert(std::make_pair(
            operation->GetOperationId(),
            TOperationUpdateList(operation))).second);
    }

    void CreateUpdateLists(const std::vector<TOperationPtr>& operations)
    {
        FOREACH (auto operation, operations) {
            CreateUpdateList(operation);
        }
    }

    TOperationUpdateList* GetUpdateList(TOperationPtr operation)
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

        // Create a batch update for all operations.
        auto batchReq = ObjectProxy.ExecuteBatch();
        FOREACH (auto& pair, UpdateLists) {
            auto* list = &pair.second;
            PrepareOperationUpdate(list, batchReq);
        }

        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnOperationNodesUpdated, MakeStrong(this))
                .Via(CancelableControlInvoker));
    }

    void PrepareOperationUpdate(
        TOperationUpdateList* list,
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
        if (state == EOperationState::Running || operation->IsFinished()) {
            auto req = TYPathProxy::Set(operationPath + "/@progress");
            req->set_value(
                ConvertToYsonString(
                    BIND(
                        &IOperationController::BuildProgressYson,
                        operation->GetController()))
                .Data()
            );
            batchReq->AddRequest(req);
        }

        // Set result.
        if (operation->IsFinished()) {
            auto req = TYPathProxy::Set(operationPath + "/@result");
            req->set_value(
                ConvertToYsonString(
                    BIND(
                        &IOperationController::BuildResultYson,
                        operation->GetController()))
                .Data()
            );
            batchReq->AddRequest(req);
        }

        // Set end time, if given.
        if (operation->GetEndTime()) {
            auto req = TYPathProxy::Set(operationPath + "/@end_time");
            req->set_value(ConvertToYsonString(operation->GetEndTime().Get()).Data());
            batchReq->AddRequest(req);
        }

        // Create jobs.
        FOREACH (auto job, list->PendingJobCreations) {
            auto jobPath = GetJobPath(operation->GetOperationId(), job->GetId());
            auto req = TYPathProxy::Set(jobPath);
            req->set_value(BuildJobYson(job).Data());
            batchReq->AddRequest(req);
        }
        list->PendingJobCreations.clear();
        
        // Update jobs.
        FOREACH (auto job, list->PendingJobUpdates) {
            auto jobPath = GetJobPath(operation->GetOperationId(), job->GetId());
            auto req = TYPathProxy::Set(jobPath + "/@");
            req->set_value(BuildJobAttributesYson(job).Data());
            batchReq->AddRequest(req);
        }
        list->PendingJobUpdates.clear();

        // Set stderrs.
        FOREACH (const auto& pair, list->PendingStdErrChunkIds) {
            auto job = pair.first;
            auto chunkId = pair.second;
            auto stdErrPath = GetStdErrPath(operation->GetOperationId(), job->GetId());
            auto req = TCypressYPathProxy::Create(stdErrPath);
            req->set_type(EObjectType::File);
            auto* reqExt = req->MutableExtension(NFileClient::NProto::TReqCreateFileExt::create_file);
            *reqExt->mutable_chunk_id() = chunkId.ToProto();
            GenerateRpcMutationId(req);
            batchReq->AddRequest(req);
        }
        list->PendingStdErrChunkIds.clear();
    }


    void OnOperationNodesUpdated(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(Connected);

        OperationNodesUpdateInvoker->ScheduleNext();

        auto error = batchRsp->GetCumulativeError();
        if (!error.IsOK()) {
            LOG_ERROR(error, "Error updating operation nodes");
            Disconnect();
            return;
        }

        LOG_INFO("Operation nodes updated");
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
        list->Flushed.Set();
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
        list->Finalized.Set();

        RemoveUpdateList(operation);
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

TFuture<void> TMasterConnector::FlushOperationNode( TOperationPtr operation )
{
    return Impl->FlushOperationNode(operation);
}

TFuture<void> TMasterConnector::FinalizeOperationNode(TOperationPtr operation)
{
    return Impl->FinalizeOperationNode(operation);
}

void TMasterConnector::CreateJobNode(TJobPtr job)
{
    return Impl->CreateJobNode(job);
}

void TMasterConnector::UpdateJobNode(TJobPtr job)
{
    return Impl->UpdateJobNode(job);
}

void TMasterConnector::SetJobStdErr(TJobPtr job, const TChunkId& chunkId)
{
    Impl->SetJobStdErr(job, chunkId);
}

DELEGATE_SIGNAL(TMasterConnector, void(const TMasterHandshakeResult& result), MasterConnected, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(), MasterDisconnected, *Impl);
DELEGATE_SIGNAL(TMasterConnector, void(TOperationPtr operation), PrimaryTransactionAborted, *Impl)
DELEGATE_SIGNAL(TMasterConnector, void(const Stroka& address), NodeOnline, *Impl)
DELEGATE_SIGNAL(TMasterConnector, void(const Stroka& address), NodeOffline, *Impl)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

