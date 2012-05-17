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

TSchedulerCommandBase::TSchedulerCommandBase(ICommandContext* host)
    : TUntypedCommandBase(host)
{ }

void TSchedulerCommandBase::StartOperation(
    TTransactedRequestPtr request,
    EOperationType type,
    const NYTree::TYson& spec)
{
    auto transaction = Context->GetTransaction(request);

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

    Context->ReplySuccess(BuildYsonFluently().Scalar(operationId.ToString()));
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand(ICommandContext* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

TCommandDescriptor TMapCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TMapCommand::DoExecute(TSchedulerRequestPtr request)
{
    StartOperation(
        request,
        EOperationType::Map,
        SerializeToYson(request->Spec));
    // TODO(babenko): dump stderrs
}

////////////////////////////////////////////////////////////////////////////////

TMergeCommand::TMergeCommand(ICommandContext* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

TCommandDescriptor TMergeCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TMergeCommand::DoExecute(TSchedulerRequestPtr request)
{
    StartOperation(
        request,
        EOperationType::Merge,
        SerializeToYson(request->Spec));
}

////////////////////////////////////////////////////////////////////////////////

TSortCommand::TSortCommand(ICommandContext* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

TCommandDescriptor TSortCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TSortCommand::DoExecute(TSchedulerRequestPtr request)
{
    StartOperation(
        request,
        EOperationType::Sort,
        SerializeToYson(request->Spec));
}

////////////////////////////////////////////////////////////////////////////////

TEraseCommand::TEraseCommand(ICommandContext* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
    , TSchedulerCommandBase(host)
{ }

TCommandDescriptor TEraseCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TEraseCommand::DoExecute(TSchedulerRequestPtr request)
{
    StartOperation(
        request,
        EOperationType::Erase,
        SerializeToYson(request->Spec));
}

////////////////////////////////////////////////////////////////////////////////

TAbortOperationCommand::TAbortOperationCommand(ICommandContext* host)
    : TTypedCommandBase(host)
    , TUntypedCommandBase(host)
{ }

TCommandDescriptor TAbortOperationCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TAbortOperationCommand::DoExecute(TAbortOperationRequestPtr request)
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());
    auto abortOpReq = proxy.AbortOperation();
    *abortOpReq->mutable_operation_id() = request->OperationId.ToProto();
    abortOpReq->Invoke().Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
