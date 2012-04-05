#include "stdafx.h"
#include "scheduler_commands.h"

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/config.h>

#include <ytlib/job_proxy/config.h>

#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerCommandBase::TSchedulerCommandBase(ICommandHost* host)
    : TUntypedCommandBase(host)
{ }

void TSchedulerCommandBase::RunOperation(
    TTransactedRequestPtr request,
    NScheduler::EOperationType type,
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

        auto startOpRsp = startOpReq->Invoke()->Get();
        if (!startOpRsp->IsOK()) {
            Host->ReplyError(startOpRsp->GetError());
            return;
        }

        operationId = TOperationId::FromProto(startOpRsp->operation_id());
    }

    Host->ReplySuccess(BuildYsonFluently()
        .BeginMap()
            .Item("operation_id").Scalar(operationId.ToString())
        .EndMap());

    {
        auto waitOpReq = proxy.WaitForOperation();
        *waitOpReq->mutable_operation_id() = operationId.ToProto();

        // Operation can run for a while, override the default timeout.
        waitOpReq->SetTimeout(Null);
        auto waitOpRsp = waitOpReq->Invoke()->Get();

        if (!waitOpRsp->IsOK()) {
            Host->ReplyError(waitOpRsp->GetError());
            return;
        }

        auto error = TError::FromProto(waitOpRsp->result().error());
        if (error.IsOK()) {
            Host->ReplySuccess();
        } else {
            Host->ReplyError(error);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand(ICommandHost* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

void TMapCommand::DoExecute(TMapRequestPtr request)
{
    PreprocessYPaths(&request->Spec->InputTablePaths);
    PreprocessYPaths(&request->Spec->OutputTablePaths);
    PreprocessYPaths(&request->Spec->FilePaths);

    RunOperation(
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
    PreprocessYPaths(&request->Spec->InputTablePaths);
    PreprocessYPath(&request->Spec->OutputTablePath);

    RunOperation(
        request,
        EOperationType::Merge,
        SerializeToYson(request->Spec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
