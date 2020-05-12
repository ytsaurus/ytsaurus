#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <yt/client/api/file_reader.h>
#include <yt/client/api/rowset.h>

#include <yt/client/table_client/unversioned_writer.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NDriver {

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

    ProduceEmptyOutput(context);
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
    PipeInputToOutput(jobInputReader, context->Request().OutputStream);
}

////////////////////////////////////////////////////////////////////////////////

TGetJobInputPathsCommand::TGetJobInputPathsCommand()
{
    RegisterParameter("job_id", JobId);
}

void TGetJobInputPathsCommand::DoExecute(ICommandContextPtr context)
{
    auto inputPaths = WaitFor(context->GetClient()->GetJobInputPaths(JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;
    context->ProduceOutputValue(std::move(inputPaths));
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
    WaitFor(output->Write(result))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TGetJobFailContextCommand::TGetJobFailContextCommand()
{
    RegisterParameter("operation_id", OperationId);
    RegisterParameter("job_id", JobId);
}

void TGetJobFailContextCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->GetJobFailContext(OperationId, JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;
    WaitFor(output->Write(result))
        .ThrowOnError();
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
    RegisterParameter("access", Options.AccessFilter)
        .Optional();
    RegisterParameter("state", Options.StateFilter)
        .Optional();
    RegisterParameter("type", Options.TypeFilter)
        .Optional();
    RegisterParameter("filter", Options.SubstrFilter)
        .Optional();
    RegisterParameter("pool", Options.Pool)
        .Optional();
    RegisterParameter("with_failed_jobs", Options.WithFailedJobs)
        .Optional();
    RegisterParameter("include_archive", Options.IncludeArchive)
        .Optional();
    RegisterParameter("include_counters", Options.IncludeCounters)
        .Optional();
    RegisterParameter("limit", Options.Limit)
        .Optional();
    RegisterParameter("attributes", Options.Attributes)
        .Optional();
    RegisterParameter("enable_ui_mode", EnableUIMode)
        .Optional();
    RegisterParameter("archive_fetching_timeout", Options.ArchiveFetchingTimeout)
        .Optional();
}

void TListOperationsCommand::BuildOperations(const TListOperationsResult& result, TFluentMap fluent)
{
    // COMPAT(levysotsky): "operation_type" is a deprecated synonim for "type".
    bool needOperationType = !Options.Attributes || Options.Attributes->contains("operation_type");
    bool needType = !Options.Attributes || Options.Attributes->contains("type");

    auto fillOperationAttributes = [needOperationType, needType] (const TOperation& operation, TFluentMap fluent) {
        fluent
            .OptionalItem("id", operation.Id)
            .OptionalItem("state", operation.State)
            .DoIf(operation.Type.operator bool(), [&] (TFluentMap fluent) {
                if (needType) {
                    fluent.Item("type").Value(operation.Type);
                }
                if (needOperationType) {
                    fluent.Item("operation_type").Value(operation.Type);
                }
            })
            .OptionalItem("authenticated_user", operation.AuthenticatedUser)
            .OptionalItem("start_time", operation.StartTime)
            .OptionalItem("finish_time", operation.FinishTime)
            .OptionalItem("brief_progress", operation.BriefProgress)
            .OptionalItem("progress", operation.Progress)
            .OptionalItem("brief_spec", operation.BriefSpec)
            .OptionalItem("full_spec", operation.FullSpec)
            .OptionalItem("spec", operation.Spec)
            .OptionalItem("unrecognized_spec", operation.UnrecognizedSpec)
            .OptionalItem("runtime_parameters", operation.RuntimeParameters)
            .OptionalItem("suspended", operation.Suspended)
            .OptionalItem("result", operation.Result)
            .OptionalItem("events", operation.Events)
            .OptionalItem("slot_index_per_pool_tree", operation.SlotIndexPerPoolTree)
            .OptionalItem("alerts", operation.Alerts);
    };

    if (EnableUIMode) {
        fluent
            .Item("operations")
                .BeginAttributes()
                    .Item("incomplete").Value(result.Incomplete)
                .EndAttributes()
                .DoListFor(result.Operations, [&] (TFluentList fluent, const TOperation& operation) {
                    fluent.Item()
                        .BeginAttributes()
                            .Do(BIND(fillOperationAttributes, operation))
                        .EndAttributes()
                        .Value(*operation.Id);
                });
    } else {
        fluent
            .Item("operations")
                .DoListFor(result.Operations, [&] (TFluentList fluent, const TOperation& operation) {
                    fluent.Item()
                        .BeginMap()
                            .Do(BIND(fillOperationAttributes, operation))
                        .EndMap();
                })
            .Item("incomplete").Value(result.Incomplete);
    }
}

void TListOperationsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListOperations(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Do(std::bind(&TListOperationsCommand::BuildOperations, this, result, std::placeholders::_1))
            .DoIf(result.PoolCounts.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("pool_counts").BeginMap()
                .DoFor(*result.PoolCounts, [] (TFluentMap fluent, const auto& item) {
                    fluent.Item(item.first).Value(item.second);
                })
                .EndMap();
            })
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
            .OptionalItem("failed_jobs_count", result.FailedJobsCount)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TListJobsCommand::TListJobsCommand()
{
    RegisterParameter("operation_id", OperationId);

    RegisterParameter("type", Options.Type)
        .Alias("job_type")
        .Optional();
    RegisterParameter("state", Options.State)
        .Alias("job_state")
        .Optional();
    RegisterParameter("address", Options.Address)
        .Optional();
    RegisterParameter("with_stderr", Options.WithStderr)
        .Optional();
    RegisterParameter("with_spec", Options.WithSpec)
        .Optional();
    RegisterParameter("with_fail_context", Options.WithFailContext)
        .Optional();
    RegisterParameter("with_competitors", Options.WithCompetitors)
        .Optional();
    RegisterParameter("job_competition_id", Options.JobCompetitionId)
        .Optional();

    RegisterParameter("sort_field", Options.SortField)
        .Optional();
    RegisterParameter("sort_order", Options.SortOrder)
        .Optional();

    RegisterParameter("limit", Options.Limit)
        .Optional();
    RegisterParameter("offset", Options.Offset)
        .Optional();

    RegisterParameter("data_source", Options.DataSource)
        .Optional();

    RegisterParameter("include_cypress", Options.IncludeCypress)
        .Optional();
    RegisterParameter("include_controller_agent", Options.IncludeControllerAgent)
        .Alias("include_runtime")
        .Alias("include_scheduler")
        .Optional();
    RegisterParameter("include_archive", Options.IncludeArchive)
        .Optional();

    RegisterParameter("running_jobs_lookbehind_period", Options.RunningJobsLookbehindPeriod)
        .Optional();
}

void TListJobsCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListJobs(OperationId, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("jobs").DoListFor(result.Jobs, [] (TFluentList fluent, const TJob& job) {
                fluent
                    .Item().Do([&] (TFluentAny innerFluent) {
                        Serialize(job, innerFluent.GetConsumer(), AsStringBuf("id"));
                    });
            })
            .Item("cypress_job_count").Value(result.CypressJobCount)
            // COMPAT(asaitgalin): Remove it in favor of controller_agent_job_count
            .Item("scheduler_job_count").Value(result.ControllerAgentJobCount)
            .Item("controller_agent_job_count").Value(result.ControllerAgentJobCount)
            .Item("archive_job_count").Value(result.ArchiveJobCount)
            .Item("type_counts").DoMapFor(TEnumTraits<NJobTrackerClient::EJobType>::GetDomainValues(), [&] (TFluentMap fluent, const auto& item) {
                i64 count = result.Statistics.TypeCounts[item];
                if (count) {
                    fluent.Item(FormatEnum(item)).Value(count);
                }
            })
            .Item("state_counts").DoMapFor(TEnumTraits<NJobTrackerClient::EJobState>::GetDomainValues(), [&] (TFluentMap fluent, const auto& item) {
                i64 count = result.Statistics.StateCounts[item];
                if (count) {
                    fluent.Item(FormatEnum(item)).Value(count);
                }
            })
            .Item("errors").Value(result.Errors)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TGetJobCommand::TGetJobCommand()
{
    RegisterParameter("operation_id", OperationId);
    RegisterParameter("job_id", JobId);
    RegisterParameter("attributes", Options.Attributes)
        .Optional();
}

void TGetJobCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetJob(OperationId, JobId, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
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

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TPollJobShellCommand::TPollJobShellCommand()
{
    RegisterParameter("job_id", JobId);
    RegisterParameter("parameters", Parameters);

    RegisterPostprocessor([&] {
        // Compatibility with initial job shell protocol.
        if (Parameters->GetType() == NYTree::ENodeType::String) {
            Parameters = NYTree::ConvertToNode(NYson::TYsonString(Parameters->AsString()->GetValue()));
        }
    });
}

void TPollJobShellCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->PollJobShell(
        JobId,
        ConvertToYsonString(Parameters),
        Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "result", result);
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

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TStartOperationCommand::TStartOperationCommand(std::optional<NScheduler::EOperationType> operationType)
{
    RegisterParameter("spec", Spec);
    if (operationType) {
        OperationType = *operationType;
    } else {
        RegisterParameter("operation_type", OperationType);
    }
}

void TStartOperationCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncOperationId = context->GetClient()->StartOperation(
        OperationType,
        ConvertToYsonString(Spec),
        Options);

    auto operationId = WaitFor(asyncOperationId)
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "operation_id", operationId);
}

////////////////////////////////////////////////////////////////////////////////

TMapCommand::TMapCommand()
    : TStartOperationCommand(EOperationType::Map)
{ }

////////////////////////////////////////////////////////////////////////////////

TMergeCommand::TMergeCommand()
    : TStartOperationCommand(EOperationType::Merge)
{ }

////////////////////////////////////////////////////////////////////////////////

TSortCommand::TSortCommand()
    : TStartOperationCommand(EOperationType::Sort)
{ }

////////////////////////////////////////////////////////////////////////////////

TEraseCommand::TEraseCommand()
    : TStartOperationCommand(EOperationType::Erase)
{ }

////////////////////////////////////////////////////////////////////////////////

TReduceCommand::TReduceCommand()
    : TStartOperationCommand(EOperationType::Reduce)
{ }

////////////////////////////////////////////////////////////////////////////////

TJoinReduceCommand::TJoinReduceCommand()
    : TStartOperationCommand(EOperationType::JoinReduce)
{ }

////////////////////////////////////////////////////////////////////////////////

TMapReduceCommand::TMapReduceCommand()
    : TStartOperationCommand(EOperationType::MapReduce)
{ }

////////////////////////////////////////////////////////////////////////////////

TRemoteCopyCommand::TRemoteCopyCommand()
    : TStartOperationCommand(EOperationType::RemoteCopy)
{ }

////////////////////////////////////////////////////////////////////////////////

TAbortOperationCommand::TAbortOperationCommand()
{
    RegisterParameter("abort_message", Options.AbortMessage)
        .Optional();
}

void TAbortOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AbortOperation(OperationIdOrAlias, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TSuspendOperationCommand::TSuspendOperationCommand()
{
    RegisterParameter("abort_running_jobs", Options.AbortRunningJobs)
        .Optional();
}

void TSuspendOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->SuspendOperation(OperationIdOrAlias, Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TResumeOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->ResumeOperation(OperationIdOrAlias))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCompleteOperationCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->CompleteOperation(OperationIdOrAlias))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TUpdateOperationParametersCommand::TUpdateOperationParametersCommand()
{
    RegisterParameter("parameters", Parameters);
}

void TUpdateOperationParametersCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->UpdateOperationParameters(
        OperationIdOrAlias,
        ConvertToYsonString(Parameters),
        Options);

    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TGetOperationCommand::TGetOperationCommand()
{
    RegisterParameter("attributes", Options.Attributes)
        .Optional();
    RegisterParameter("include_runtime", Options.IncludeRuntime)
        .Alias("include_scheduler")
        .Optional();
}

void TGetOperationCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetOperation(OperationIdOrAlias, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
