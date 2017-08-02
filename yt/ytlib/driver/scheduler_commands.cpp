#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <yt/ytlib/api/file_reader.h>
#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NYTree;
using namespace NConcurrency;
using namespace NApi;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDumpJobContextCommand::TDumpJobContextCommand()
{
    RegisterParameter("job_id", JobId);
    RegisterParameter("path", Path);
}

void TDumpJobContextCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->DumpJobContext(JobId, Path))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TGetJobInputCommand::TGetJobInputCommand()
{
    RegisterParameter("job_id", JobId);
}

void TGetJobInputCommand::DoExecute(ICommandContextPtr context)
{
    auto jobInputReader = WaitFor(context->GetClient()->GetJobInput(JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;

    while (true) {
        auto block = WaitFor(jobInputReader->Read())
            .ValueOrThrow();

        if (!block)
            break;

        WaitFor(output->Write(block))
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TGetJobStderrCommand::TGetJobStderrCommand()
{
    RegisterParameter("operation_id", OperationId);
    RegisterParameter("job_id", JobId);
}

void TGetJobStderrCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->GetJobStderr(OperationId, JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;
    output->Write(result);
}

////////////////////////////////////////////////////////////////////////////////

TListOperationsCommand::TListOperationsCommand()
{
    RegisterParameter("from_time", Options.FromTime)
        .Optional();
    RegisterParameter("to_time", Options.ToTime)
        .Optional();
    RegisterParameter("cursor_time", Options.CursorTime)
        .Optional();
    RegisterParameter("cursor_direction", Options.CursorDirection)
        .Optional();
    RegisterParameter("user", Options.UserFilter)
        .Optional();
    RegisterParameter("state", Options.StateFilter)
        .Optional();
    RegisterParameter("type", Options.TypeFilter)
        .Optional();
    RegisterParameter("filter", Options.SubstrFilter)
        .Optional();
    RegisterParameter("with_failed_jobs", Options.WithFailedJobs)
        .Optional();
    RegisterParameter("include_archive", Options.IncludeArchive)
        .Optional();
    RegisterParameter("include_counters", Options.IncludeCounters)
        .Optional();
    RegisterParameter("limit", Options.Limit)
        .Optional();
}

void TListOperationsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListOperations(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("operations").BeginList()
                .DoFor(result.Operations, [] (TFluentList fluent, const TOperation& operation) {
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(operation.OperationId)
                            .Item("type").Value(operation.OperationType)
                            .Item("state").Value(operation.OperationState)
                            .Item("authenticated_used").Value(operation.AuthenticatedUser)
                            .Item("brief_progress").Value(operation.BriefProgress)
                            .Item("brief_spec").Value(operation.BriefSpec)
                            .Item("start_time").Value(operation.StartTime)
                            .DoIf(operation.FinishTime.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("finish_time").Value(operation.FinishTime);
                            })
                            .DoIf(operation.Suspended.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("suspended").Value(operation.Suspended);
                            })
                            .DoIf(operation.Weight.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("weight").Value(operation.Weight);
                            })
                        .EndMap();
                })  
            .EndList()
            .Item("incomplete").Value(result.Incomplete)
            .DoIf(result.UserCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("user_counts").BeginMap()
                .DoFor(*result.UserCounts, [] (TFluentMap fluent, const auto& item) {
                    fluent.Item(item.first).Value(item.second);
                })  
                .EndMap();
            })
            .DoIf(result.StateCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("state_counts").BeginMap()
                .DoFor(TEnumTraits<EOperationState>::GetDomainValues(), [&result] (TFluentMap fluent, const EOperationState& item) {
                    i64 count = (*result.StateCounts)[item];
                    if (count) {
                        fluent.Item(FormatEnum(item)).Value(count);
                    }
                })  
                .EndMap();
            })
            .DoIf(result.TypeCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("type_counts").BeginMap()
                .DoFor(TEnumTraits<EOperationType>::GetDomainValues(), [&result] (TFluentMap fluent, const EOperationType& item) {
                    i64 count = (*result.TypeCounts)[item];
                    if (count) {
                        fluent.Item(FormatEnum(item)).Value(count);
                    }
                })
                .EndMap();
            })
            .DoIf(result.FailedJobsCount.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("failed_jobs_count").Value(*result.FailedJobsCount);
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TListJobsCommand::TListJobsCommand()
{
    RegisterParameter("operation_id", OperationId);

    RegisterParameter("job_type", Options.JobType)
        .Optional();
    RegisterParameter("job_state", Options.JobState)
        .Optional();
    RegisterParameter("address", Options.Address)
        .Optional();

    RegisterParameter("sort_field", Options.SortField)
        .Optional();
    RegisterParameter("sort_order", Options.SortOrder)
        .Optional();

    RegisterParameter("limit", Options.Limit)
        .Optional();
    RegisterParameter("offset", Options.Offset)
        .Optional();

    RegisterParameter("has_stderr", Options.HasStderr)
        .Optional();

    RegisterParameter("include_cypress", Options.IncludeCypress)
        .Optional();
    RegisterParameter("include_runtime", Options.IncludeRuntime)
        .Optional();
    RegisterParameter("include_archive", Options.IncludeArchive)
        .Optional();
}

struct TListJobsSchema
{
    template <class TColumnAdder>
    explicit TListJobsSchema(TColumnAdder columnAdder)
        : JobId(columnAdder("job_id", EValueType::String))
        , JobType(columnAdder("job_type", EValueType::String))
        , JobState(columnAdder("job_state", EValueType::String))
        , StartTime(columnAdder("start_time", EValueType::String))
        , FinishTime(columnAdder("finish_time", EValueType::String))
        , Address(columnAdder("address", EValueType::String))
        , Error(columnAdder("error", EValueType::Any))
        , Statistics(columnAdder("statistics", EValueType::Any))
        , StderrSize(columnAdder("stderr_size", EValueType::Uint64))
        , Progress(columnAdder("progress", EValueType::Double))
        , CoreInfos(columnAdder("core_infos", EValueType::Any))
    { }

    const int JobId;
    const int JobType;
    const int JobState;
    const int StartTime;
    const int FinishTime;
    const int Address;
    const int Error;
    const int Statistics;
    const int StderrSize;
    const int Progress;
    const int CoreInfos;
};

TUnversionedValue ToUnversionedValue(const TYsonString& value)
{
    if (value) {
        return MakeUnversionedStringValue(value.GetData());
    } else {
        return MakeUnversionedSentinelValue(EValueType::Null);
    }
}

template <class T>
TUnversionedValue ToUnversionedValue(const T& value)
{
    return MakeUnversionedStringValue(ToString(value));
}

TUnversionedValue ToUnversionedValue(const double& value)
{
    return MakeUnversionedDoubleValue(value);
}

TUnversionedValue ToUnversionedValue(const i64& value)
{
    return MakeUnversionedInt64Value(value);
}

TUnversionedValue ToUnversionedValue(const ui64& value)
{
    return MakeUnversionedUint64Value(value);
}

template <class T>
TUnversionedValue ToUnversionedValue(TNullable<T> value)
{
    if (value) {
        return ToUnversionedValue(*value);
    } else {
        return MakeUnversionedSentinelValue(EValueType::Null);
    }
}

void TListJobsCommand::DoExecute(ICommandContextPtr context)
{
    std::vector<TColumnSchema> columns;
    auto addColumn = [&] (const TString& name, EValueType type) {
        size_t id = columns.size();
        columns.emplace_back(name, type);
        return id;
    };

    TListJobsSchema ids(addColumn);
    TTableSchema schema(columns);

    auto buffer = New<TRowBuffer>();

    auto result = WaitFor(context->GetClient()->ListJobs(OperationId, Options))
        .ValueOrThrow();
 
    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("jobs").BeginList()
                .DoFor(result, [] (TFluentList fluent, const TJob& job) {
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(job.JobId)
                            .Item("type").Value(job.JobType)
                            .Item("state").Value(job.JobState)
                            .Item("start_time").Value(job.StartTime)
                            .Item("finish_time").Value(job.FinishTime)
                            .Item("address").Value(job.Address)
                            .DoIf(job.Error.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("error").Value(job.Error);
                            })
                            .DoIf(job.Statistics.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("statistics").Value(job.Statistics);
                            }) 
                            .DoIf(job.StderrSize.HasValue(), [&] (TFluentMap fluent) {
                                fluent.Item("stderr_size").Value(job.StderrSize);
                            })
                            .DoIf(job.Progress.HasValue(), [&] (TFluentMap fluent) {
                                fluent.Item("progress").Value(job.Progress);
                            })
                            .DoIf(job.CoreInfos.HasValue(), [&] (TFluentMap fluent) {
                                fluent.Item("core_infos").Value(job.CoreInfos);
                            })
                        .EndMap();
                })
            .EndList()
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TStraceJobCommand::TStraceJobCommand()
{
    RegisterParameter("job_id", JobId);
}

void TStraceJobCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->StraceJob(JobId, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result));
}

////////////////////////////////////////////////////////////////////////////////

TSignalJobCommand::TSignalJobCommand()
{
    RegisterParameter("job_id", JobId);
    RegisterParameter("signal_name", SignalName);
}

void TSignalJobCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SignalJob(JobId, SignalName))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TAbandonJobCommand::TAbandonJobCommand()
{
    RegisterParameter("job_id", JobId);
}

void TAbandonJobCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbandonJob(JobId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TPollJobShellCommand::TPollJobShellCommand()
{
    RegisterParameter("job_id", JobId);
    RegisterParameter("parameters", Parameters);
}

void TPollJobShellCommand::OnLoaded()
{
    TCommandBase::OnLoaded();

    // Compatibility with initial job shell protocol.
    if (Parameters->GetType() == NYTree::ENodeType::String) {
        Parameters = NYTree::ConvertToNode(NYson::TYsonString(Parameters->AsString()->GetValue()));
    }
}

void TPollJobShellCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->PollJobShell(
        JobId,
        ConvertToYsonString(Parameters),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

TAbortJobCommand::TAbortJobCommand()
{
    RegisterParameter("job_id", JobId);
    RegisterParameter("interrupt_timeout", Options.InterruptTimeout)
        .Optional();
}

void TAbortJobCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbortJob(JobId, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TStartOperationCommandBase::TStartOperationCommandBase()
{
    RegisterParameter("spec", Spec);
}

void TStartOperationCommandBase::DoExecute(ICommandContextPtr context)
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

TAbortOperationCommand::TAbortOperationCommand()
{
    RegisterParameter("abort_message", Options.AbortMessage)
        .Optional();
}

void TAbortOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbortOperation(OperationId, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TSuspendOperationCommand::TSuspendOperationCommand()
{
    RegisterParameter("abort_running_jobs", Options.AbortRunningJobs)
        .Optional();
}

void TSuspendOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendOperation(OperationId, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TResumeOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeOperation(OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TCompleteOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->CompleteOperation(OperationId))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TGetOperationCommand::TGetOperationCommand()
{   
    RegisterParameter("operation_id", OperationId);
}

void TGetOperationCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetOperation(OperationId);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
