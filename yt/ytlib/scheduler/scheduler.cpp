#include "stdafx.h"
#include "scheduler.h"
#include "private.h"
#include "config.h"
#include "exec_node.h"
#include "operation.h"
#include "job.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/string.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/cypress/cypress_service_proxy.h>

#include <ytlib/cell_scheduler/config.h>
#include <ytlib/cell_scheduler/bootstrap.h>

#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/cypress/id.h>

#include <ytlib/object_server/object_ypath_proxy.h>

#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NCellScheduler;
using namespace NTransactionClient;
using namespace NCypress;
using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////

NLog::TLogger& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap)
        : Config(config)
        , Bootstrap(bootstrap)
        , CypressProxy(bootstrap->GetMasterChannel())
    {
        YASSERT(config);
        YASSERT(bootstrap);
        VERIFY_INVOKER_AFFINITY(bootstrap->GetControlInvoker(), ControlThread);
    }

    void Start()
    {
        RegisterAtMaster();
        StartRefresh();
    }

    TFuture< TValueOrError<TOperationPtr> >::TPtr StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const NYTree::IMapNodePtr spec)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Generate operation id.
        auto operationId = TOperationId::Create();

        // Create operation object.
        auto operation = New<TOperation>(
            operationId,
            type,
            transactionId,
            spec,
            TInstant::Now());

        LOG_INFO("Starting operation (Type: %s, OperationId: %s, TransactionId: %s)",
            ~type.ToString(),
            ~operationId.ToString(),
            ~transactionId.ToString());

        // Create a node in Cypress that will represent the operation.
        auto setReq = TYPathProxy::Set(GetOperationPath(operationId));
        setReq->set_value(BuildYsonFluently()
            .WithAttributes().BeginMap()
            .EndMap()
            .BeginAttributes()
            .Item("type").Scalar(CamelCaseToUnderscoreCase(type.ToString()))
            .Item("transaction_id").Scalar(transactionId.ToString())
            .Item("spec").Node(spec)
            .EndAttributes());

        return CypressProxy.Execute(setReq)->Apply(
            FromMethod(
            &TImpl::OnOperationNodeCreated,
            this,
            operation)
            ->AsyncVia(Bootstrap->GetControlInvoker()));
    }

    void AbortOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Aborting operation (OperationId: %s)",
            ~operation->GetOperationId().ToString());

        UnregisterOperation(operation);
    }


    TExecNodePtr FindNode(const Stroka& address)
    {
        auto it = Nodes.find(address);
        return it == Nodes.end() ? NULL : it->second;
    }

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    NCypress::TCypressServiceProxy CypressProxy;

    NTransactionClient::ITransaction::TPtr BootstrapTransaction;

    TPeriodicInvoker::TPtr TransactionRefreshInvoker;
    TPeriodicInvoker::TPtr NodesRefreshInvoker;

    yhash_map<Stroka, TExecNodePtr> Nodes;
    yhash_map<TOperationId, TOperationPtr> Operations;
    yhash_map<TJobId, TJobId> Jobs;

    TValueOrError<TOperationPtr> OnOperationNodeCreated(
        NYTree::TYPathProxy::TRspSet::TPtr rsp,
        TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!rsp->IsOK()) {
            auto error = rsp->GetError();
            LOG_ERROR("Error creating operation node\n%s", ~error.ToString());
            return error;
        }

        RegisterOperation(operation);
        return operation;
    }


    void RegisterNode(TExecNodePtr node)
    {
        YVERIFY(Nodes.insert(MakePair(node->GetAddress(), node)).second);    
    }

    void UnregisterNode(TExecNodePtr node)
    {
        YVERIFY(Nodes.erase(node->GetAddress()) == 1);
    }

    
    void RegisterOperation(TOperationPtr operation)
    {
        YVERIFY(Operations.insert(MakePair(operation->GetOperationId(), operation)).second);
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        YVERIFY(Operations.erase(operation->GetOperationId()) == 1);
    }


    void RegisterAtMaster()
    {
        // TODO(babenko): Currently we use succeed-or-die strategy. Add retries later.

        // Take the lock to prevent multiple instances of scheduler from running simultaneously.
        // To this aim, we create an auxiliary transaction that takes care of this lock.
        // We never commit or commit this transaction, so it gets aborted (and the lock gets released)
        // when the scheduler dies.
        try {
            BootstrapTransaction = Bootstrap->GetTransactionManager()->Start();
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Failed to start bootstrap transaction\n%s", ex.what());
        }

        LOG_INFO("Taking lock");
        {
            auto req = TCypressYPathProxy::Lock(WithTransaction(
                "/sys/scheduler/lock",
                BootstrapTransaction->GetId()));
            req->set_mode(ELockMode::Exclusive);
            auto rsp = CypressProxy.Execute(req)->Get();
            if (!rsp->IsOK()) {
                ythrow yexception() << Sprintf("Failed to take scheduler lock, check for another running scheduler instances\n%s",
                    ~rsp->GetError().ToString());
            }
        }
        LOG_INFO("Lock taken");

        LOG_INFO("Publishing scheduler address");
        {
            auto req = TYPathProxy::Set("/sys/scheduler/runtime@address");
            req->set_value(SerializeToYson(Bootstrap->GetPeerAddress()));
            auto rsp = CypressProxy.Execute(req)->Get();
            if (!rsp->IsOK()) {
                ythrow yexception() << Sprintf("Failed to publish scheduler address\n%s",
                    ~rsp->GetError().ToString());
            }
        }
        LOG_INFO("Scheduler address published");
    }


    void StartRefresh()
    {
        TransactionRefreshInvoker = New<TPeriodicInvoker>(
            FromMethod(&TImpl::RefreshTransactions, this)
            ->Via(Bootstrap->GetControlInvoker()),
            Config->TransactionsRefreshPeriod);
        TransactionRefreshInvoker->Start();

        NodesRefreshInvoker = New<TPeriodicInvoker>(
            FromMethod(&TImpl::RefreshNodes, this)
            ->Via(Bootstrap->GetControlInvoker()),
            Config->NodesRefreshPeriod);
        NodesRefreshInvoker->Start();
    }


    void RefreshTransactions()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Check if any operations are running.
        if (Operations.empty())
            return;

        // Collect all transactions that are used by currently running operations.
        yhash_set<TTransactionId> transactionIds;
        FOREACH (const auto& pair, Operations) {
            transactionIds.insert(pair.second->GetTransactionId());
        }

        // Invoke GetId verbs for these transactions to see if they are alive.
        std::vector<TTransactionId> transactionIdsList;
        auto batchReq = CypressProxy.ExecuteBatch();
        FOREACH (const auto& id, transactionIds) {
            auto checkReq = TObjectYPathProxy::GetId(FromObjectId(id));
            transactionIdsList.push_back(id);
            batchReq->AddRequest(checkReq);
        }

        LOG_INFO("Refreshing %d transactions", batchReq->GetSize());
        batchReq->Invoke()->Subscribe(
            FromMethod(&TImpl::OnTransactionsRefreshed, this, transactionIdsList)
            ->Via(Bootstrap->GetControlInvoker()));
    }

    void OnTransactionsRefreshed(
        NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr rsp,
        std::vector<TTransactionId> transactionIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!rsp->IsOK()) {
            LOG_ERROR("Error refreshing transactions\n%s", rsp->GetError().ToString());
            return;
        }

        LOG_INFO("Transactions refreshed successfully");

        // Collect the list of dead transactions.
        yhash_set<TTransactionId> deadTransactionIds;
        for (int index = 0; index < rsp->GetSize(); ++index) {
            if (!rsp->GetResponse(index)->IsOK()) {
                YVERIFY(deadTransactionIds.insert(transactionIds[index]).second);
            }
        }

        // Collect the list of operations corresponding to dead transactions.
        std::vector<TOperationPtr> deadOperations;
        FOREACH (const auto& pair, Operations) {
            auto operation = pair.second;
            if (deadTransactionIds.find(operation->GetTransactionId()) != deadTransactionIds.end()) {
                deadOperations.push_back(operation);
            }
        }

        // Abort dead operations.
        FOREACH (auto operation, deadOperations) {
            LOG_INFO("Operation corresponds to a dead transaction, aborting (OperationId: %s, TransactionId: %s)",
                ~operation->GetOperationId().ToString(),
                ~operation->GetTransactionId().ToString());
            AbortOperation(operation);
        }
    }


    void RefreshNodes()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Get the list of online nodes from the master.
        LOG_INFO("Refreshing nodes");
        auto req = TYPathProxy::Get("/sys/holders@online");
        CypressProxy.Execute(req)->Subscribe(
            FromMethod(&TImpl::OnNodesRefreshed, this)
            ->Via(Bootstrap->GetControlInvoker()));
    }

    void OnNodesRefreshed(NYTree::TYPathProxy::TRspGet::TPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!rsp->IsOK()) {
            LOG_ERROR("Error refreshing nodes\n%s", rsp->GetError().ToString());
            return;
        }

        LOG_INFO("Nodes refreshed successfully");

        // Examine the list of nodes returned by master and figure out the difference.

        yhash_set<TExecNodePtr> deadNodes;
        FOREACH (const auto& pair, Nodes) {
            YVERIFY(deadNodes.insert(pair.second).second);
        }

        auto onlineAddresses = DeserializeFromYson< yvector<Stroka> >(rsp->value());
        
        FOREACH (const auto& address, onlineAddresses) {
            auto node = FindNode(address);
            if (node) {
                YVERIFY(deadNodes.erase(node) == 1);
            } else {
                LOG_INFO("Node %s has become online", ~address.Quote());
                auto node = New<TExecNode>(address);
                RegisterNode(node);
            }
        }

        FOREACH (auto node, deadNodes) {
            LOG_INFO("Node %s has become offline", ~node->GetAddress().Quote());
            UnregisterNode(node);
        }
    }


    static NYTree::TYPath GetOperationPath(const TOperationId& id)
    {
        return CombineYPaths("/sys/scheduler/operations", id.ToString());
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

TScheduler::TScheduler(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl(new TImpl(config, bootstrap))
{ }

void TScheduler::Start()
{
    Impl->Start();
}

TFuture< TValueOrError<TOperationPtr> >::TPtr TScheduler::StartOperation(
    EOperationType type,
    const TTransactionId& transactionId,
    const NYTree::IMapNodePtr spec)
{
    return  Impl->StartOperation(
        type,
        transactionId,
        spec);
}

void TScheduler::AbortOperation(TOperationPtr operation)
{
    Impl->AbortOperation(operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

