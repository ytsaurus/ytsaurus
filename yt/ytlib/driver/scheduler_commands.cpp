#include "stdafx.h"
#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/config.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/transaction_client/transaction.h>

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NCypressClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerCommandBase::TSchedulerCommandBase(const ICommandContextPtr& context)
    : TTransactedCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TSchedulerCommandBase::StartOperation(EOperationType type)
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());

    TOperationId operationId;
    {
        auto req = proxy.StartOperation();
        req->set_type(type);
        *req->mutable_transaction_id() = GetTransactionId(false).ToProto();
        req->set_spec(ConvertToYsonString(Request->Spec).Data());

        auto rsp = req->Invoke().Get();
        if (!rsp->IsOK()) {
            THROW_ERROR(rsp->GetError());
        }

        operationId = TOperationId::FromProto(rsp->operation_id());
    }

    ReplySuccess(BuildYsonStringFluently()
        .Scalar(operationId));
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand(const ICommandContextPtr& context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TMapCommand::DoExecute()
{
    StartOperation(EOperationType::Map);
}

////////////////////////////////////////////////////////////////////////////////

TMergeCommand::TMergeCommand(const ICommandContextPtr& context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TMergeCommand::DoExecute()
{
    StartOperation(EOperationType::Merge);
}

////////////////////////////////////////////////////////////////////////////////

TSortCommand::TSortCommand(const ICommandContextPtr& context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TSortCommand::DoExecute()
{
    StartOperation(EOperationType::Sort);
}

////////////////////////////////////////////////////////////////////////////////

TEraseCommand::TEraseCommand(const ICommandContextPtr& context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TEraseCommand::DoExecute()
{
    StartOperation(EOperationType::Erase);
}

////////////////////////////////////////////////////////////////////////////////

TReduceCommand::TReduceCommand(const ICommandContextPtr& context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TReduceCommand::DoExecute()
{
    StartOperation(EOperationType::Reduce);
}

////////////////////////////////////////////////////////////////////////////////

TMapReduceCommand::TMapReduceCommand(const ICommandContextPtr& context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TMapReduceCommand::DoExecute()
{
    StartOperation(EOperationType::MapReduce);
}

////////////////////////////////////////////////////////////////////////////////

TAbortOperationCommand::TAbortOperationCommand(const ICommandContextPtr& context)
    : TTransactedCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TAbortOperationCommand::DoExecute()
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());
    auto req = proxy.AbortOperation();
    *req->mutable_operation_id() = Request->OperationId.ToProto();
    auto rsp = req->Invoke().Get();
    if (!rsp->IsOK()) {
        THROW_ERROR(rsp->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
