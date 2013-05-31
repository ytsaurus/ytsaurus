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

void TSchedulerCommandBase::StartOperation(EOperationType type)
{
    auto req = SchedulerProxy->StartOperation();
    req->set_type(type);
    ToProto(req->mutable_transaction_id(), GetTransactionId(EAllowNullTransaction::Yes));
    ToProto(req->mutable_mutation_id(), Request->MutationId);
    req->set_spec(ConvertToYsonString(Request->Spec).Data());

    CheckAndReply(
        req->Invoke(),
        BIND([] (TSchedulerServiceProxy::TRspStartOperationPtr rsp) -> TYsonString {
            auto operationId = FromProto<TOperationId>(rsp->operation_id());
            return BuildYsonStringFluently().Value(operationId);
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TMapCommand::DoExecute()
{
    StartOperation(EOperationType::Map);
}

////////////////////////////////////////////////////////////////////////////////

void TMergeCommand::DoExecute()
{
    StartOperation(EOperationType::Merge);
}

////////////////////////////////////////////////////////////////////////////////

void TSortCommand::DoExecute()
{
    StartOperation(EOperationType::Sort);
}

////////////////////////////////////////////////////////////////////////////////

void TEraseCommand::DoExecute()
{
    StartOperation(EOperationType::Erase);
}

////////////////////////////////////////////////////////////////////////////////

void TReduceCommand::DoExecute()
{
    StartOperation(EOperationType::Reduce);
}

////////////////////////////////////////////////////////////////////////////////

void TMapReduceCommand::DoExecute()
{
    StartOperation(EOperationType::MapReduce);
}

////////////////////////////////////////////////////////////////////////////////

void TAbortOperationCommand::DoExecute()
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());
    auto req = proxy.AbortOperation();
    ToProto(req->mutable_operation_id(), Request->OperationId);

    CheckAndReply(req->Invoke());
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendOperationCommand::DoExecute()
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());
    auto req = proxy.SuspendOperation();
    ToProto(req->mutable_operation_id(), Request->OperationId);

    auto rsp = req->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

void TResumeOperationCommand::DoExecute()
{
    TSchedulerServiceProxy proxy(Context->GetSchedulerChannel());
    auto req = proxy.ResumeOperation();
    ToProto(req->mutable_operation_id(), Request->OperationId);

    auto rsp = req->Invoke().Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
