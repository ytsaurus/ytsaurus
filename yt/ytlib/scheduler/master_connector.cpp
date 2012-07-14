#include "stdafx.h"
#include "master_connector.h"
#include "helpers.h"
#include "scheduler.h"
#include "private.h"

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/cell_scheduler/bootstrap.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ytree.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NCypress;
using namespace NObjectServer;
using namespace NChunkServer;

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
        , ObjectProxy(bootstrap->GetMasterChannel())
    { }

    void Start()
    {
        while (true) {
            try {
                TryRegister();
                // Registration was successful, bail out.
                break;
            } catch (const std::exception& ex) {
                LOG_WARNING("Registration failed, will retry in %s\n%s",
                    ~ToString(Config->StartupRetryPeriod),
                    ex.what());
                Sleep(Config->StartupRetryPeriod);
            }
        }

        StartRefresh();
    }


    std::vector<TOperationPtr> LoadOperations()
    {
        std::vector<TOperationPtr> operations;

        LOG_INFO("Requesting operations list");
        std::vector<TOperationId> operationIds;
        {
            auto req = TYPathProxy::List(GetOperationsPath());
            auto rsp = ObjectProxy.Execute(req).Get();
            if (!rsp->IsOK()) {
                ythrow yexception() << Sprintf("Failed to get operations list\n%s",
                    ~rsp->GetError().ToString());
            }
            LOG_INFO("Found %d operations", rsp->keys_size());
            FOREACH (const auto& key, rsp->keys()) {
                operationIds.push_back(TOperationId::FromString(key));
            }
        }

        LOG_INFO("Requesting operations attributes");
        {
            auto batchReq = ObjectProxy.ExecuteBatch();
            FOREACH (const auto& operationId, operationIds) {
                auto req = TYPathProxy::Get(GetOperationPath(operationId) + "/@");
                batchReq->AddRequest(req);
            }
            auto batchRsp = batchReq->Invoke().Get();
            if (!batchRsp->IsOK()) {
                ythrow yexception() << Sprintf("Failed to get operations attributes\n%s",
                    ~batchRsp->GetError().ToString());
            }

            for (int index = 0; index < batchRsp->GetSize(); ++index) {
                const auto& operationId = operationIds[index];
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(index);
                if (!rsp->IsOK()) {
                    ythrow yexception() << Sprintf("Failed to get attributes for operation %s\n%s",
                        ~operationId.ToString(),
                        ~rsp->GetError().ToString());
                }

                auto operation = ParseOperationYson(operationId, TYsonString(rsp->value()));
                operations.push_back(operation);
            }
        }

        LOG_INFO("Operations loaded successfully");

        return operations;
    }


    TAsyncError CreateOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto id = operation->GetOperationId();
        LOG_INFO("Creating operation node (OperationId: %s)", ~id.ToString());

        CreateOperationUpdateList(operation);

        auto setReq = TYPathProxy::Set(GetOperationPath(id));
        setReq->set_value(BuildOperationYson(operation).Data());
        return ObjectProxy
            .Execute(setReq)
            .Apply(
                BIND(&TImpl::OnOperationNodeCreated, MakeStrong(this), operation)
                .AsyncVia(Bootstrap->GetControlInvoker()));
    }

    void ReviveOperationNodes(const std::vector<TOperationPtr> operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto batchReq = ObjectProxy.ExecuteBatch();
        FOREACH (auto operation, operations) {
            LOG_INFO("Reviving operation node (OperationId: %s)", ~operation->GetOperationId().ToString());
            
            CreateOperationUpdateList(operation);

            YCHECK(operation->GetState() == EOperationState::Reviving);
            
            auto req = TYPathProxy::Set(GetOperationPath(operation->GetOperationId()));
            req->set_value(BuildOperationYson(operation).Data());
            batchReq->AddRequest(req);
        }

        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnOperationNodesRevived, MakeStrong(this))
            .Via(Bootstrap->GetControlInvoker()));
    }
    
    void RemoveOperationNode(TOperationPtr operation)
    {
        auto id = operation->GetOperationId();
        LOG_INFO("Removing operation node (OperationId: %s)", ~id.ToString());

        auto removeReq = TYPathProxy::Remove(GetOperationPath(id));
        ObjectProxy.Execute(removeReq).Subscribe(
            BIND(&TImpl::OnOperationNodeRemoved, MakeStrong(this), operation)
            .Via(Bootstrap->GetControlInvoker()));
    }

    TAsyncError FlushOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Flushing operation node (OperationId: %s)", ~operation->GetOperationId().ToString());

        auto* list = GetOperationUpdateList(operation);

        // Create a batch update for this particular operation.
        auto batchReq = ObjectProxy.ExecuteBatch();
        PrepareOperationUpdate(list, batchReq);

        return batchReq->Invoke().Apply(
            BIND(&TImpl::OnOperationNodeFlushed, MakeStrong(this), operation)
            .AsyncVia(Bootstrap->GetControlInvoker()));
    }

    TAsyncError FinalizeOperationNode(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Finalizing operation node (OperationId: %s)", ~operation->GetOperationId().ToString());

        auto* list = GetOperationUpdateList(operation);
        YCHECK(!list->FinalizationPending);
        list->FinalizationPending = true;

        // Create a batch update for this particular operation.
        auto batchReq = ObjectProxy.ExecuteBatch();
        PrepareOperationUpdate(list, batchReq);

        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnOperationNodeFinalized, MakeStrong(this), operation)
            .Via(Bootstrap->GetControlInvoker()));

        return list->Finalized;
    }


    void CreateJobNode(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Creating job node (OperationId: %s, JobId: %s)",
            ~job->GetOperation()->GetOperationId().ToString(),
            ~job->GetId().ToString());

        auto* list = GetOperationUpdateList(job->GetOperation());
        YCHECK(!list->FinalizationPending);
        YCHECK(list->PendingJobCreations.insert(job).second);
    }

    void UpdateJobNode(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Updating job node (OperationId: %s, JobId: %s)",
            ~job->GetOperation()->GetOperationId().ToString(),
            ~job->GetId().ToString());

        auto* list = GetOperationUpdateList(job->GetOperation());
        YCHECK(!list->FinalizationPending);
        list->PendingJobUpdates.insert(job);
    }

    void SetJobStdErr(TJobPtr job, const NChunkServer::TChunkId& chunkId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YASSERT(chunkId != NullChunkId);

        LOG_DEBUG("Setting job stderr (OperationId: %s, JobId: %s, StdErrChunkId: %s)",
            ~job->GetOperation()->GetOperationId().ToString(),
            ~job->GetId().ToString(),
            ~chunkId.ToString());

        auto* list = GetOperationUpdateList(job->GetOperation());
        YCHECK(!list->FinalizationPending);
        list->PendingStdErrChunkIds.insert(std::make_pair(job, chunkId));
    }


    DEFINE_SIGNAL(void(TOperationPtr operation), PrimaryTransactionAborted);
    DEFINE_SIGNAL(void(const Stroka& address), NodeOnline);
    DEFINE_SIGNAL(void(const Stroka& address), NodeOffline);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    NObjectServer::TObjectServiceProxy ObjectProxy;

    NTransactionClient::ITransactionPtr BootstrapTransaction;

    TPeriodicInvoker::TPtr TransactionRefreshInvoker;
    TPeriodicInvoker::TPtr ExecNodesRefreshInvoker;
    TPeriodicInvoker::TPtr OperationNodesUpdateInvoker;

    struct TOperationUpdateList
    {
        explicit TOperationUpdateList(TOperationPtr operation)
            : Operation(operation)
            , FinalizationPending(false)
            , Finalized(NewPromise<TError>())
        { }

        TOperationPtr Operation;
        yhash_set<TJobPtr> PendingJobCreations;
        yhash_set<TJobPtr> PendingJobUpdates;
        yhash_map<TJobPtr, TChunkId> PendingStdErrChunkIds;
        bool FinalizationPending;
        TPromise<TError> Finalized;
    };

    yhash_map<TOperationId, TOperationUpdateList> OperationUpdateLists;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void TryRegister()
    {
        // Take a lock to prevent multiple instances of scheduler from running simultaneously.
        // To this aim, create an auxiliary transaction that takes care of this lock.
        // We never commit this transaction, so it gets aborted (and the lock gets released)
        // when the scheduler dies.

        try {
            BootstrapTransaction = Bootstrap->GetTransactionManager()->Start();
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Failed to start bootstrap transaction\n%s", ex.what());
        }

        try {
            LOG_INFO("Taking lock");
            {
                auto req = TCypressYPathProxy::Lock(WithTransaction(
                    "//sys/scheduler/lock",
                    BootstrapTransaction->GetId()));
                req->set_mode(ELockMode::Exclusive);
                auto rsp = ObjectProxy.Execute(req).Get();
                if (!rsp->IsOK()) {
                    ythrow yexception() << Sprintf("Failed to take scheduler lock, check for another running scheduler instances\n%s",
                        ~rsp->GetError().ToString());
                }
            }
            LOG_INFO("Lock taken");

            LOG_INFO("Publishing scheduler address");
            {
                auto req = TYPathProxy::Set("//sys/scheduler/@address");
                req->set_value(YsonizeString(Bootstrap->GetPeerAddress(), EYsonFormat::Binary));
                auto rsp = ObjectProxy.Execute(req).Get();
                if (!rsp->IsOK()) {
                    ythrow yexception() << Sprintf("Failed to publish scheduler address\n%s",
                        ~rsp->GetError().ToString());
                }
            }
            LOG_INFO("Scheduler address published");

            LOG_INFO("Registering at orchid");
            {
                auto req = TYPathProxy::Set("//sys/scheduler/orchid&/@remote_address");
                req->set_value(YsonizeString(Bootstrap->GetPeerAddress(), EYsonFormat::Binary));
                auto rsp = ObjectProxy.Execute(req).Get();
                if (!rsp->IsOK()) {
                    ythrow yexception() << Sprintf("Failed to register at orchid\n%s",
                        ~rsp->GetError().ToString());
                }
            }
            LOG_INFO("Registered at orchid");
        } catch (...) {
            // Abort the bootstrap transaction (will need a new one anyway).
            BootstrapTransaction->Abort();
            BootstrapTransaction.Reset();
            throw;
        }
    }


    TYsonString BuildOperationYson(TOperationPtr operation)
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

    TYsonString BuildJobYson(TJobPtr job)
    {
        return
            BuildYsonFluently()
                .BeginAttributes()
                    .Do(BIND(&BuildJobAttributes, job))
                .EndAttributes()
                .BeginMap()
                .EndMap().GetYsonString();
    }

    TYsonString BuildJobAttributesYson(TJobPtr job)
    {
        return
            BuildYsonFluently()
                .BeginMap()
                    .Do(BIND(&BuildJobAttributes, job))
                .EndMap().GetYsonString();
    }

    TOperationPtr ParseOperationYson(const TOperationId& operationId, const TYsonString& yson)
    {
        auto attributes = ConvertToAttributes(yson);
        return New<TOperation>(
            operationId,
            attributes->Get<EOperationType>("operation_type"),
            attributes->Get<TTransactionId>("transaction_id"),
            attributes->Get<INodePtr>("spec")->AsMap(),
            attributes->Get<TInstant>("start_time"),
            attributes->Get<EOperationState>("state"));
    }


    void StartRefresh()
    {
        TransactionRefreshInvoker = New<TPeriodicInvoker>(
            Bootstrap->GetControlInvoker(),
            BIND(&TImpl::RefreshTransactions, MakeWeak(this)),
            Config->TransactionsRefreshPeriod);
        TransactionRefreshInvoker->Start();

        ExecNodesRefreshInvoker = New<TPeriodicInvoker>(
            Bootstrap->GetControlInvoker(),
            BIND(&TImpl::RefreshExecNodes, MakeWeak(this)),
            Config->NodesRefreshPeriod);
        ExecNodesRefreshInvoker->Start();

        OperationNodesUpdateInvoker = New<TPeriodicInvoker>(
            Bootstrap->GetControlInvoker(),
            BIND(&TImpl::UpdateOperationNodes, MakeWeak(this)),
            Config->OperationsUpdatePeriod);
        OperationNodesUpdateInvoker->Start();
    }

    void RefreshTransactions()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Collect all transactions that are used by currently running operations.
        yhash_set<TTransactionId> transactionIds;
        auto operations = Bootstrap->GetScheduler()->GetOperations();
        FOREACH (auto operation, operations) {
            auto transactionId = operation->GetTransactionId();
            if (transactionId != NullTransactionId) {
                transactionIds.insert(transactionId);
            }
        }

        // Invoke GetId verbs for these transactions to see if they are alive.
        std::vector<TTransactionId> transactionIdsList;
        auto batchReq = ObjectProxy.ExecuteBatch();
        FOREACH (const auto& id, transactionIds) {
            auto checkReq = TObjectYPathProxy::GetId(FromObjectId(id));
            transactionIdsList.push_back(id);
            batchReq->AddRequest(checkReq);
        }

        LOG_INFO("Refreshing %d transactions", batchReq->GetSize());
        batchReq->Invoke().Subscribe(BIND(
            &TImpl::OnTransactionsRefreshed,
            MakeStrong(this),
            Passed(MoveRV(transactionIdsList)))
            .Via(Bootstrap->GetControlInvoker()));
    }

    void OnTransactionsRefreshed(
        const std::vector<TTransactionId>& transactionIds,
        NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TransactionRefreshInvoker->ScheduleNext();

        if (!rsp->IsOK()) {
            LOG_ERROR("Error refreshing transactions\n%s", ~rsp->GetError().ToString());
            return;
        }

        LOG_INFO("Transactions refreshed successfully");

        // Collect the list of dead transactions.
        yhash_set<TTransactionId> deadTransactionIds;
        for (int index = 0; index < rsp->GetSize(); ++index) {
            if (!rsp->GetResponse(index)->IsOK()) {
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

        // Get the list of online nodes from the master.
        LOG_INFO("Refreshing exec nodes");
        auto req = TYPathProxy::Get("//sys/holders/@online");
        ObjectProxy.Execute(req).Subscribe(
            BIND(&TImpl::OnExecNodesRefreshed, MakeStrong(this))
            .Via(Bootstrap->GetControlInvoker()));
    }

    void OnExecNodesRefreshed(NYTree::TYPathProxy::TRspGetPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ExecNodesRefreshInvoker->ScheduleNext();

        if (!rsp->IsOK()) {
            LOG_ERROR("Error refreshing exec nodes\n%s", ~rsp->GetError().ToString());
            return;
        }

        auto onlineAddresses = ConvertTo< std::vector<Stroka> >(TYsonString(rsp->value()));
        LOG_INFO("Exec nodes refreshed successfully, %d nodes found",
            static_cast<int>(onlineAddresses.size()));

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


    void CreateOperationUpdateList(TOperationPtr operation)
    {
        YCHECK(OperationUpdateLists.insert(std::make_pair(
            operation->GetOperationId(),
            TOperationUpdateList(operation))).second);
    }

    TOperationUpdateList* GetOperationUpdateList(TOperationPtr operation)
    {
        auto it = OperationUpdateLists.find(operation->GetOperationId());
        YCHECK(it != OperationUpdateLists.end());
        return &it->second;
    }

    void RemoveOperationUpdateList(TOperationPtr operation)
    {
        YCHECK(OperationUpdateLists.erase(operation->GetOperationId()));
    }


    void UpdateOperationNodes()
    {
        LOG_INFO("Updating operation nodes");

        // Create a batch update for all operations.
        auto batchReq = ObjectProxy.ExecuteBatch();
        FOREACH (auto& pair, OperationUpdateLists) {
            auto* list = &pair.second;
            PrepareOperationUpdate(list, batchReq);
        }

        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnOperationNodesUpdated, MakeStrong(this))
            .Via(Bootstrap->GetControlInvoker()));
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
            auto* reqExt = req->MutableExtension(NFileServer::NProto::TReqCreateFileExt::create_file);
            *reqExt->mutable_chunk_id() = chunkId.ToProto();
            batchReq->AddRequest(req);
        }
        list->PendingStdErrChunkIds.clear();
    }


    void OnOperationNodesRevived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Just check every response and log the errors.
        // TODO(babenko): retry?
        if (!batchRsp->IsOK()) {
            LOG_ERROR("Error reviving operation nodes\n%s", ~batchRsp->GetError().ToString());
            return;
        }

        FOREACH (auto rsp, batchRsp->GetResponses()) {
            if (!rsp->IsOK()) {
                LOG_ERROR("Error reviving operation node\n%s", ~rsp->GetError().ToString());
            }
        }

        LOG_INFO("Operation nodes revived successfully");
    }

    void OnOperationNodesUpdated(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        OperationNodesUpdateInvoker->ScheduleNext();

        // Just check every response and log the errors.
        // TODO(babenko): retry?
        if (!batchRsp->IsOK()) {
            LOG_ERROR("Error updating operation nodes\n%s", ~batchRsp->GetError().ToString());
            return;
        }

        FOREACH (auto rsp, batchRsp->GetResponses()) {
            if (!rsp->IsOK()) {
                LOG_ERROR("Error updating operation nodes\n%s", ~rsp->GetError().ToString());
            }
        }

        LOG_INFO("Operation nodes updated successfully");
    }

    TError OnOperationNodeCreated(
        TOperationPtr operation,
        TYPathProxy::TRspSetPtr rsp)
    {
        auto operationId = operation->GetOperationId();
        auto error = rsp->GetError();

        if (!error.IsOK()) {
            LOG_ERROR("Error creating operation node (OperationId: %s)\n%s",
                ~operationId.ToString(),
                ~error.ToString());
            return rsp->GetError();
        }

        LOG_ERROR("Operation node created successfully (OperationId: %s)",
            ~operationId.ToString());

        return TError();
    }

    void OnOperationNodeRemoved(
        TOperationPtr operation,
        TYPathProxy::TRspRemovePtr rsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // TODO(babenko): retry failed attempts

        if (!rsp->IsOK()) {
            LOG_WARNING("Error removing operation node (OperationId: %s)\n%s",
                ~operation->GetOperationId().ToString(),
                ~rsp->GetError().ToString());
            return;
        }

        LOG_INFO("Operation node removed successfully (OperationId: %s)",
            ~operation->GetOperationId().ToString());
    }

    TError OnOperationNodeFlushed(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetOperationId();

        // Just check every response and log the errors.
        auto error = GetBatchError(batchRsp);
        if (error.IsOK()) {
            LOG_INFO("Operation node flushed successfully (OperationId: %s)",
                ~operationId.ToString());
        } else {
            LOG_ERROR("Error flushing operation node (OperationId: %s)\n%s",
                ~operationId.ToString(),
                ~error.ToString());
        }

        return error;
    }

    void OnOperationNodeFinalized(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetOperationId();
        auto* list = GetOperationUpdateList(operation);

        // Just check every response and log the errors.
        // TODO(babenko): retry?
        auto error = GetBatchError(batchRsp);
        if (error.IsOK()) {
            LOG_INFO("Operation node finalized successfully (OperationId: %s)",
                ~operationId.ToString());
            list->Finalized.Set(TError());
        } else {
            LOG_ERROR("Error finalizing operation node (OperationId: %s)\n%s",
                ~operationId.ToString(),
                ~error.ToString());
            // Wrap the error.
            list->Finalized.Set(TError(Sprintf("Error finalizing operation node\n%s",
                ~error.ToString())));
        }

        RemoveOperationUpdateList(operation);
    }

    static TError GetBatchError(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        TError error;
        if (!batchRsp->IsOK()) {
            return batchRsp->GetError();
        }

        FOREACH (auto rsp, batchRsp->GetResponses()) {
            auto error = rsp->GetError();
            if (!error.IsOK()) {
                return error;
            }
        }
        
        return TError();
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

std::vector<TOperationPtr> TMasterConnector::LoadOperations()
{
    return Impl->LoadOperations();
}

TAsyncError TMasterConnector::CreateOperationNode(TOperationPtr operation)
{
    return Impl->CreateOperationNode(operation);
}

void TMasterConnector::ReviveOperationNodes(const std::vector<TOperationPtr> operations)
{
    return Impl->ReviveOperationNodes(operations);
}

void TMasterConnector::RemoveOperationNode(TOperationPtr operation)
{
    Impl->RemoveOperationNode(operation);
}

TAsyncError TMasterConnector::FlushOperationNode( TOperationPtr operation )
{
    return Impl->FlushOperationNode(operation);
}

TAsyncError TMasterConnector::FinalizeOperationNode(TOperationPtr operation)
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

DELEGATE_SIGNAL(TMasterConnector, void(TOperationPtr operation), PrimaryTransactionAborted, *Impl)
DELEGATE_SIGNAL(TMasterConnector, void(const Stroka& address), NodeOnline, *Impl)
DELEGATE_SIGNAL(TMasterConnector, void(const Stroka& address), NodeOffline, *Impl)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

