#include "stdafx.h"
#include "scheduler.h"
#include "private.h"
#include "config.h"

#include <ytlib/misc/string.h>

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

TOperation::TOperation(
    const TOperationId& operationId,
    EOperationType type,
    const TTransactionId& transactionId,
    IMapNodePtr spec)
    : OperationId(operationId)
    , Type(type)
    , TransactionId(transactionId)
    , Spec(spec)
{ }

TOperationId TOperation::GetOperationId()
{
    return OperationId;
}

EOperationType TOperation::GetType() const
{
    return Type;
}

TTransactionId TOperation::GetTransactionId() const
{
    return TransactionId;
}

IMapNodePtr TOperation::GetSpec() const
{
    return Spec;
}

////////////////////////////////////////////////////////////////////

TScheduler::TScheduler(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , CypressProxy(bootstrap->GetMasterChannel())
{
    YASSERT(config);
    YASSERT(bootstrap);
    VERIFY_INVOKER_AFFINITY(bootstrap->GetControlInvoker(), ControlThread);
}

void TScheduler::Start()
{
    RegisterAtMaster();
    StartRefresh();
}

void TScheduler::RegisterAtMaster()
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

void TScheduler::StartRefresh()
{
    TransactionRefreshInvoker = New<TPeriodicInvoker>(
        FromMethod(&TScheduler::RefreshTransactions, MakeWeak(this))
        ->Via(Bootstrap->GetControlInvoker()),
        Config->TransactionsRefreshPeriod);
    TransactionRefreshInvoker->Start();

    NodesRefreshInvoker = New<TPeriodicInvoker>(
        FromMethod(&TScheduler::RefreshNodes, MakeWeak(this))
        ->Via(Bootstrap->GetControlInvoker()),
        Config->NodesRefreshPeriod);
    NodesRefreshInvoker->Start();
}

TFuture< TValueOrError<TOperationPtr> >::TPtr TScheduler::StartOperation(
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
        spec);

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
            &TScheduler::OnOperationNodeCreated,
            MakeStrong(this),
            operation)
        ->AsyncVia(Bootstrap->GetControlInvoker()));
}

TValueOrError<TOperationPtr> TScheduler::OnOperationNodeCreated(
    TYPathProxy::TRspSet::TPtr rsp,
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

void TScheduler::AbortOperation(TOperationPtr operation)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Aborting operation (OperationId: %s)",
        ~operation->GetOperationId().ToString());

    UnregisterOperation(operation);
}

TYPath TScheduler::GetOperationPath(const TOperationId& id)
{
    return CombineYPaths("/sys/scheduler/operations", id.ToString());
}

void TScheduler::RefreshNodes()
{
}

void TScheduler::OnNodesRefreshed(TYPathProxy::TRspGet::TPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

}

void TScheduler::RefreshTransactions()
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
        FromMethod(&TScheduler::OnTransactionsRefreshed, MakeStrong(this), transactionIdsList)
        ->Via(Bootstrap->GetControlInvoker()));

}

void TScheduler::OnTransactionsRefreshed(
    TCypressServiceProxy::TRspExecuteBatch::TPtr rsp,
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

void TScheduler::RegisterOperation(TOperationPtr operation)
{
    YVERIFY(Operations.insert(MakePair(operation->GetOperationId(), operation)).second);
}

void TScheduler::UnregisterOperation(TOperationPtr operation)
{
    YVERIFY(Operations.erase(operation->GetOperationId()) == 1);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

