#include "stdafx.h"
#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <core/concurrency/scheduler.h>

#include <core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NYTree;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TDumpInputContextCommand::DoExecute()
{
    WaitFor(Context_->GetClient()->DumpInputContext(Request_->JobId, Request_->Path))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TStartOperationCommandBase::DoExecute()
{
    TStartOperationOptions options;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);
    auto asyncOperationId = Context_->GetClient()->StartOperation(
        GetOperationType(),
        ConvertToYsonString(Request_->Spec),
        options);

    auto operationId = WaitFor(asyncOperationId)
        .ValueOrThrow();

    Reply(BuildYsonStringFluently()
        .Value(operationId));
}

////////////////////////////////////////////////////////////////////////////////

EOperationType TMapCommand::GetOperationType() const
{
    return EOperationType::Map;
}

////////////////////////////////////////////////////////////////////////////////

EOperationType TMergeCommand::GetOperationType() const
{
    return EOperationType::Merge;
}

////////////////////////////////////////////////////////////////////////////////

EOperationType TSortCommand::GetOperationType() const
{
    return EOperationType::Sort;
}

////////////////////////////////////////////////////////////////////////////////

EOperationType TEraseCommand::GetOperationType() const
{
    return EOperationType::Erase;
}

////////////////////////////////////////////////////////////////////////////////

EOperationType TReduceCommand::GetOperationType() const
{
    return EOperationType::Reduce;
}

////////////////////////////////////////////////////////////////////////////////

EOperationType TMapReduceCommand::GetOperationType() const
{
    return EOperationType::MapReduce;
}

////////////////////////////////////////////////////////////////////////////////

EOperationType TRemoteCopyCommand::GetOperationType() const
{
    return EOperationType::RemoteCopy;
}

////////////////////////////////////////////////////////////////////////////////

void TAbortOperationCommand::DoExecute()
{
    WaitFor(Context_->GetClient()->AbortOperation(Request_->OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendOperationCommand::DoExecute()
{
    WaitFor(Context_->GetClient()->SuspendOperation(Request_->OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TResumeOperationCommand::DoExecute()
{
    WaitFor(Context_->GetClient()->ResumeOperation(Request_->OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
