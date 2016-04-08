#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NYTree;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TDumpJobContextCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->DumpJobContext(JobId, Path))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TStraceJobCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->StraceJob(JobId, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result));
}

////////////////////////////////////////////////////////////////////////////////

void TSignalJobCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SignalJob(JobId, SignalName))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAbandonJobCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbandonJob(JobId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TStartOperationCommandBase::Execute(ICommandContextPtr context)
{
    auto asyncOperationId = context->GetClient()->StartOperation(
        GetOperationType(),
        ConvertToYsonString(Spec),
        Options);

    auto operationId = WaitFor(asyncOperationId)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(operationId));
}

////////////////////////////////////////////////////////////////////////////////

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

EOperationType TJoinReduceCommand::GetOperationType() const
{
    return EOperationType::JoinReduce;
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

void TAbortOperationCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbortOperation(OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TSuspendOperationCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendOperation(OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TResumeOperationCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeOperation(OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TCompleteOperationCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->CompleteOperation(OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
