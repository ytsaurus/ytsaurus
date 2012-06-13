#include "stdafx.h"
#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/config.h>

#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/transaction_client/transaction.h>

#include <ytlib/job_proxy/config.h>

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NCypress;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerCommandBase::TSchedulerCommandBase(ICommandContext* context)
    : TTransactedCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TSchedulerCommandBase::StartOperation(EOperationType type)
{
    auto transaction = GetTransaction(false);

    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());

    TOperationId operationId;
    {
        auto startOpReq = proxy.StartOperation();
        startOpReq->set_type(type);
        *startOpReq->mutable_transaction_id() = (transaction ? transaction->GetId() : NullTransactionId).ToProto();
        startOpReq->set_spec(ConvertToYsonString(Request->Spec).Data());

        auto startOpRsp = startOpReq->Invoke().Get();
        if (!startOpRsp->IsOK()) {
            ythrow yexception() << startOpRsp->GetError().ToString();
        }

        operationId = TOperationId::FromProto(startOpRsp->operation_id());
    }

    ReplySuccess(BuildYsonFluently().Scalar(operationId).GetYsonString());
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand(ICommandContext* host)
    : TSchedulerCommandBase(host)
    , TUntypedCommandBase(host)
{ }

void TMapCommand::DoExecute()
{
    StartOperation(EOperationType::Map);
}

////////////////////////////////////////////////////////////////////////////////

TMergeCommand::TMergeCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TMergeCommand::DoExecute()
{
    StartOperation(EOperationType::Merge);
}

////////////////////////////////////////////////////////////////////////////////

TSortCommand::TSortCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TSortCommand::DoExecute()
{
    StartOperation(EOperationType::Sort);
}

////////////////////////////////////////////////////////////////////////////////

TEraseCommand::TEraseCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TEraseCommand::DoExecute()
{
    StartOperation(EOperationType::Erase);
}

////////////////////////////////////////////////////////////////////////////////

TReduceCommand::TReduceCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TReduceCommand::DoExecute()
{
    StartOperation(EOperationType::Reduce);
}

////////////////////////////////////////////////////////////////////////////////

TAbortOperationCommand::TAbortOperationCommand(ICommandContext* context)
    : TTransactedCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TAbortOperationCommand::DoExecute()
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());
    auto abortOpReq = proxy.AbortOperation();
    *abortOpReq->mutable_operation_id() = Request->OperationId.ToProto();
    auto abortOpRsp = abortOpReq->Invoke().Get();
    if (!abortOpRsp->IsOK()) {
        ythrow yexception() << abortOpRsp->GetError().ToString();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
