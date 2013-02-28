#include "stdafx.h"
#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <ytlib/scheduler/config.h>

#include <ytlib/security_client/rpc_helpers.h>

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerCommandBase::TSchedulerCommandBase(ICommandContext* context)
    : TTransactedCommandBase(context)
{ }

void TSchedulerCommandBase::StartOperation(EOperationType type)
{
    TOperationId operationId;
    {
        auto req = SchedulerProxy->StartOperation();
        req->set_type(type);
        *req->mutable_transaction_id() = GetTransactionId(false).ToProto();
        req->set_spec(ConvertToYsonString(Request->Spec).Data());
        NSecurityClient::SetRpcAuthenticatedUser(req, Request->AuthenticatedUser);

        auto rsp = req->Invoke().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        operationId = TOperationId::FromProto(rsp->operation_id());
    }

    ReplySuccess(BuildYsonStringFluently()
        .Value(operationId));
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
{ }

void TMapCommand::DoExecute()
{
    StartOperation(EOperationType::Map);
}

////////////////////////////////////////////////////////////////////////////////

TMergeCommand::TMergeCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
{ }

void TMergeCommand::DoExecute()
{
    StartOperation(EOperationType::Merge);
}

////////////////////////////////////////////////////////////////////////////////

TSortCommand::TSortCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
{ }

void TSortCommand::DoExecute()
{
    StartOperation(EOperationType::Sort);
}

////////////////////////////////////////////////////////////////////////////////

TEraseCommand::TEraseCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
{ }

void TEraseCommand::DoExecute()
{
    StartOperation(EOperationType::Erase);
}

////////////////////////////////////////////////////////////////////////////////

TReduceCommand::TReduceCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
{ }

void TReduceCommand::DoExecute()
{
    StartOperation(EOperationType::Reduce);
}

////////////////////////////////////////////////////////////////////////////////

TMapReduceCommand::TMapReduceCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
{ }

void TMapReduceCommand::DoExecute()
{
    StartOperation(EOperationType::MapReduce);
}

////////////////////////////////////////////////////////////////////////////////

TAbortOperationCommand::TAbortOperationCommand(ICommandContext* context)
    : TTransactedCommandBase(context)
{ }

void TAbortOperationCommand::DoExecute()
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());
    auto req = proxy.AbortOperation();
    *req->mutable_operation_id() = Request->OperationId.ToProto();

    auto rsp = req->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
