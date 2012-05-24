#include "stdafx.h"
#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

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

TSchedulerCommandBase::TSchedulerCommandBase(ICommandContext* context)
    : TTransactedCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TSchedulerCommandBase::StartOperation(
    EOperationType type,
    const NYTree::TYson& spec)
{
    auto transaction = GetTransaction(false);

    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());

    TOperationId operationId;
    {
        auto startOpReq = proxy.StartOperation();
        startOpReq->set_type(type);
        *startOpReq->mutable_transaction_id() = (transaction ? transaction->GetId() : NullTransactionId).ToProto();
        startOpReq->set_spec(spec);

        auto startOpRsp = startOpReq->Invoke().Get();
        if (!startOpRsp->IsOK()) {
            ythrow yexception() << startOpRsp->GetError().ToString();
        }

        operationId = TOperationId::FromProto(startOpRsp->operation_id());
    }

    ReplySuccess(BuildYsonFluently().Scalar(operationId.ToString()));
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand(ICommandContext* host)
    : TSchedulerCommandBase(host)
    , TUntypedCommandBase(host)
{ }

void TMapCommand::DoExecute()
{
    StartOperation(
        EOperationType::Map,
        SerializeToYson(Request->Spec));
    // TODO(babenko): dump stderrs
}

////////////////////////////////////////////////////////////////////////////////

TMergeCommand::TMergeCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TMergeCommand::DoExecute()
{
    StartOperation(
        EOperationType::Merge,
        SerializeToYson(Request->Spec));
}

////////////////////////////////////////////////////////////////////////////////

TSortCommand::TSortCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TSortCommand::DoExecute()
{
    StartOperation(
        EOperationType::Sort,
        SerializeToYson(Request->Spec));
}

////////////////////////////////////////////////////////////////////////////////

TEraseCommand::TEraseCommand(ICommandContext* context)
    : TSchedulerCommandBase(context)
    , TUntypedCommandBase(context)
{ }

void TEraseCommand::DoExecute()
{
    StartOperation(
        EOperationType::Erase,
        SerializeToYson(Request->Spec));
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
    abortOpReq->Invoke().Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
