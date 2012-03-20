#include "stdafx.h"
#include "scheduler.h"
#include "private.h"

#include <ytlib/cell_scheduler/config.h>
#include <ytlib/cell_scheduler/bootstrap.h>

#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/cypress/id.h>

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
    TCellSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , TransactionManager(New<TTransactionManager>(
        config->TransactionManager,
        bootstrap->GetMasterChannel()))
    , CypressProxy(bootstrap->GetMasterChannel())
{
    YASSERT(config);
    YASSERT(bootstrap);
    VERIFY_INVOKER_AFFINITY(bootstrap->GetControlInvoker(), ControlThread);
}

void TScheduler::Start()
{
    Register();
}

void TScheduler::Register()
{
    // TODO(babenko): Currently we use succeed-or-die strategy. Add retries later.

    // Take the lock to prevent multiple instances of scheduler from running simultaneously.
    // To this aim, we create an auxiliary transaction that takes care of this lock.
    // We never commit or commit this transaction, so it gets aborted (and the lock gets released)
    // when the scheduler dies.
    try {
        BootstrapTransaction = TransactionManager->Start();
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

    LOG_INFO("Starting %s operation (OperationId: %s, TransactionId: %s)",
        ~type.ToString(),
        ~operationId.ToString(),
        ~transactionId.ToString());

    // Create a node in Cypress that will represent the operation.
    auto setReq = TYPathProxy::Set(GetOperationPath(operationId));
    setReq->set_value(BuildYsonFluently()
        .BeginMap()
            .Item("type").Scalar(type.ToString())
            .Item("transaction_id").Scalar(transactionId.ToString())
        .EndMap());

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

    return operation;
}

TYPath TScheduler::GetOperationPath(const TOperationId& id)
{
    return CombineYPaths("/sys/scheduler/operations", id.ToString());
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

