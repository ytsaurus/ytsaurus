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
using namespace NHydra;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TSchedulerCommandBase::StartOperation(EOperationType type)
{
    TStartOperationOptions options;
    SetTransactionalOptions(&options);
    SetMutatingOptions(&options);
    auto asyncOperationId = Context_->GetClient()->StartOperation(
        type,
        ConvertToYsonString(Request_->Spec),
        options);

    auto operationId = WaitFor(asyncOperationId)
        .ValueOrThrow();

    Reply(BuildYsonStringFluently()
        .Value(operationId));
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

void TRemoteCopyCommand::DoExecute()
{
    StartOperation(EOperationType::RemoteCopy);
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
