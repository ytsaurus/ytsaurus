#include "stdafx.h"
#include "scheduler_commands.h"

#include <ytlib/misc/configurable.h>

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/config.h>

#include <ytlib/cypress/cypress_ypath_proxy.h>

#include <ytlib/job_proxy/config.h>

#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NCypress;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerCommandBase::TSchedulerCommandBase(ICommandHost* host)
    : TUntypedCommandBase(host)
{ }

void TSchedulerCommandBase::StartOperation(
    TTransactedRequestPtr request,
    EOperationType type,
    const NYTree::TYson& spec)
{
    auto transaction = Host->GetTransaction(request, true);

    TSchedulerServiceProxy proxy(Host->GetSchedulerChannel());

    TOperationId operationId;
    {
        auto startOpReq = proxy.StartOperation();
        startOpReq->set_type(type);
        *startOpReq->mutable_transaction_id() = transaction->GetId().ToProto();
        startOpReq->set_spec(spec);

        auto startOpRsp = startOpReq->Invoke().Get();
        if (!startOpRsp->IsOK()) {
            ythrow yexception() << startOpRsp->GetError().ToString();
        }

        operationId = TOperationId::FromProto(startOpRsp->operation_id());
    }

    Host->ReplySuccess(BuildYsonFluently().Scalar(operationId.ToString()));

    // TODO: move the rest to the console wrapper
    WaitForOperation(operationId);
    DumpOperationResult(operationId);
}

void TSchedulerCommandBase::WaitForOperation(const TOperationId& operationId)
{
    auto config = Host->GetConfig();

    TSchedulerServiceProxy proxy(Host->GetSchedulerChannel());

    while (true)  {
        auto waitOpReq = proxy.WaitForOperation();
        *waitOpReq->mutable_operation_id() = operationId.ToProto();
        waitOpReq->set_timeout(config->OperationWaitTimeout.GetValue());

        // Override default timeout.
        waitOpReq->SetTimeout(config->OperationWaitTimeout * 2);
        auto waitOpRsp = waitOpReq->Invoke().Get();

        if (!waitOpRsp->IsOK()) {
            ythrow yexception() << waitOpRsp->GetError().ToString();
        }

        if (waitOpRsp->finished())
            break;

        DumpOperationProgress(operationId);
    } 
}

void TSchedulerCommandBase::AbortOperation(const NScheduler::TOperationId& operationId)
{
    TSchedulerServiceProxy proxy(Host->GetSchedulerChannel());
    auto abortOpReq = proxy.AbortOperation();
    *abortOpReq->mutable_operation_id() = operationId.ToProto();
    abortOpReq->Invoke().Get();
}


// TODO(babenko): refactor
static NYTree::TYPath GetOperationPath(const TOperationId& id)
{
    return "//sys/operations/" + EscapeYPathToken(id.ToString());
}

// TODO(babenko): refactor
// TODO(babenko): YPath and RPC responses currently share no base class.
template <class TResponse>
static void CheckResponse(TResponse response, const Stroka& failureMessage) 
{
    if (response->IsOK())
        return;

    ythrow yexception() << failureMessage + "\n" + response->GetError().ToString();
}

void TSchedulerCommandBase::DumpOperationProgress(const TOperationId& operationId)
{
    auto operationPath = GetOperationPath(operationId);
    
    TCypressServiceProxy proxy(Host->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(operationPath + "/@state");
        batchReq->AddRequest(req, "get_state");
    }

    {
        auto req = TYPathProxy::Get(operationPath + "/@progress");
        batchReq->AddRequest(req, "get_progress");
    }

    auto batchRsp = batchReq->Invoke().Get();
    CheckResponse(batchRsp, "Error getting operation progress");

    EOperationState state;
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_state");
        CheckResponse(rsp, "Error getting operation state");
        state = DeserializeFromYson<EOperationState>(rsp->value());
    }

    TYson progress;
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_progress");
        CheckResponse(rsp, "Error getting operation progress");
        progress = rsp->value();
    }

    if (state == EOperationState::Running) {
        i64 jobsTotal = DeserializeFromYson<i64>(progress, "/jobs/total");
        i64 jobsCompleted = DeserializeFromYson<i64>(progress, "/jobs/completed");
        int donePercentage  = (jobsCompleted * 100) / jobsTotal;
        printf("%s: %3d%% jobs done (%" PRId64 " of %" PRId64 ")\n",
            ~state.ToString(),
            donePercentage,
            jobsCompleted,
            jobsTotal);
    } else {
        printf("%s\n", ~state.ToString());
    }
}

void TSchedulerCommandBase::DumpOperationResult(const TOperationId& operationId)
{
    auto operationPath = GetOperationPath(operationId);

    TCypressServiceProxy proxy(Host->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(operationPath + "/@result");
        batchReq->AddRequest(req, "get_result");
    }

    auto batchRsp = batchReq->Invoke().Get();
    CheckResponse(batchRsp, "Error getting operation result");

    TError error;

    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_result");
        CheckResponse(rsp, "Error getting operation result");
        // TODO(babenko): refactor!
        auto errorNode = DeserializeFromYson<INodePtr>(rsp->value(), "/error");
        error = TError::FromYson(errorNode);
    }

    // TODO(babenko): refactor!
    printf("%s\n", ~error.ToString());
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand(ICommandHost* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

void TMapCommand::DoExecute(TMapRequestPtr request)
{
    StartOperation(
        request,
        EOperationType::Map,
        SerializeToYson(request->Spec));
    // TODO(babenko): dump stderrs
}

////////////////////////////////////////////////////////////////////////////////

TMergeCommand::TMergeCommand(ICommandHost* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

void TMergeCommand::DoExecute(TMergeRequestPtr request)
{
    StartOperation(
        request,
        EOperationType::Merge,
        SerializeToYson(request->Spec));
}

////////////////////////////////////////////////////////////////////////////////

TAbortOperationCommand::TAbortOperationCommand(ICommandHost* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

void TAbortOperationCommand::DoExecute(TAbortOperationRequestPtr request)
{
    AbortOperation(request->OperationId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
