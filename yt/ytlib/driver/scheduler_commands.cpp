#include "scheduler_commands.h"
#include "config.h"
#include "driver.h"

#include <yt/client/api/file_reader.h>
#include <yt/client/api/rowset.h>

#include <yt/client/table_client/schemaful_writer.h>
#include <yt/client/table_client/row_buffer.h>

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

TGetJobInputPathsCommand::TGetJobInputPathsCommand()
{
    RegisterParameter("job_id", JobId);
}

void TGetJobInputPathsCommand::DoExecute(ICommandContextPtr context)
{
    auto inputPaths = WaitFor(context->GetClient()->GetJobInputPaths(JobId, Options))
        .ValueOrThrow();

    auto output = context->Request().OutputStream;
    output->Write(TSharedRef::FromString(std::move(inputPaths.GetData())));
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
    RegisterParameter("owned_by", Options.OwnedBy)
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
}

void TListOperationsCommand::BuildOperations(const TListOperationsResult& result, TFluentMap fluent)
{
    // COMPAT(levysotsky): "operation_type" is a deprecated synonim for "type".
    bool needOperationType = !Options.Attributes || Options.Attributes->has("operation_type");
    bool needType = !Options.Attributes || Options.Attributes->has("type");

    auto fillOperationAttributes = [needOperationType, needType] (const TOperation& operation, TFluentMap fluent) {
        fluent
            .DoIf(operation.Id.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("id").Value(operation.Id);
            })
            .DoIf(operation.State.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("state").Value(operation.State);
            })
            .DoIf(operation.Type.operator bool(), [&] (TFluentMap fluent) {
                if (needType) {
                    fluent.Item("type").Value(operation.Type);
                }
                if (needOperationType) {
                    fluent.Item("operation_type").Value(operation.Type);
                }
            })
            .DoIf(operation.AuthenticatedUser.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("authenticated_user").Value(operation.AuthenticatedUser);
            })

            .DoIf(operation.StartTime.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("start_time").Value(operation.StartTime);
            })
            .DoIf(operation.FinishTime.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("finish_time").Value(operation.FinishTime);
            })

            .DoIf(operation.BriefProgress.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("brief_progress").Value(operation.BriefProgress);
            })
            .DoIf(operation.Progress.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("progress").Value(operation.Progress);
            })

            .DoIf(operation.BriefSpec.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("brief_spec").Value(operation.BriefSpec);
            })
            .DoIf(operation.FullSpec.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("full_spec").Value(operation.FullSpec);
            })
            .DoIf(operation.Spec.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("spec").Value(operation.Spec);
            })
            .DoIf(operation.UnrecognizedSpec.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("unrecognized_spec").Value(operation.UnrecognizedSpec);
            })

            .DoIf(operation.RuntimeParameters.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("runtime_parameters").Value(operation.RuntimeParameters);
            })
            .DoIf(operation.Suspended.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("suspended").Value(operation.Suspended);
            })
            .DoIf(operation.Result.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("result").Value(operation.Result);
            })
            .DoIf(operation.Events.operator bool(), [&](TFluentMap fluent) {
                fluent.Item("events").Value(operation.Events);
            })
            .DoIf(operation.SlotIndexPerPoolTree.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("slot_index_per_pool_tree").Value(operation.SlotIndexPerPoolTree);
            });
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
            .DoIf(result.FailedJobsCount.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("failed_jobs_count").Value(*result.FailedJobsCount);
            })
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
            .Item("jobs").BeginList()
                .DoFor(result.Jobs, [] (TFluentList fluent, const TJob& job) {
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(job.Id)
                            .Item("type").Value(job.Type)
                            .Item("state").Value(job.State)
                            .Item("address").Value(job.Address)
                            .Item("start_time").Value(job.StartTime)
                            .DoIf(job.FinishTime.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("finish_time").Value(*job.FinishTime);
                            })
                            .DoIf(job.Progress.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("progress").Value(*job.Progress);
                            })
                            .DoIf(job.StderrSize.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("stderr_size").Value(*job.StderrSize);
                            })
                            .DoIf(job.FailContextSize.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("fail_context_size").Value(*job.FailContextSize);
                            })
                            .DoIf(job.HasSpec.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("has_spec").Value(*job.HasSpec);
                            })
                            .DoIf(job.Error.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("error").Value(job.Error);
                            })
                            .DoIf(job.BriefStatistics.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("brief_statistics").Value(job.BriefStatistics);
                            })
                            .DoIf(job.InputPaths.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("input_paths").Value(job.InputPaths);
                            })
                            .DoIf(job.CoreInfos.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("core_infos").Value(job.CoreInfos);
                            })
                        .EndMap();
                })
            .EndList()
            .Item("cypress_job_count").Value(result.CypressJobCount)
            // COMPAT(asaitgalin): Remove it in favor of controller_agent_job_count
            .Item("scheduler_job_count").Value(result.ControllerAgentJobCount)
            .Item("controller_agent_job_count").Value(result.ControllerAgentJobCount)
            .Item("archive_job_count").Value(result.ArchiveJobCount)
            .Item("type_counts").BeginMap()
                .DoFor(TEnumTraits<NJobTrackerClient::EJobType>::GetDomainValues(), [&] (TFluentMap fluent, const auto& item) {
                    i64 count = result.Statistics.TypeCounts[item];
                    if (count) {
                        fluent.Item(FormatEnum(item)).Value(count);
                    }
                })
            .EndMap()
            .Item("state_counts").BeginMap()
                .DoFor(TEnumTraits<NJobTrackerClient::EJobState>::GetDomainValues(), [&] (TFluentMap fluent, const auto& item) {
                    i64 count = result.Statistics.StateCounts[item];
                    if (count) {
                        fluent.Item(FormatEnum(item)).Value(count);
                    }
                })
            .EndMap()
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

    ProduceSingleOutputValue(context, "job", result);
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

    ProduceSingleOutputValue(context, "trace", result);
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

    ProduceEmptyOutput(context);
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

TStartOperationCommand::TStartOperationCommand(TNullable<NScheduler::EOperationType> operationType)
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

} // namespace NDriver
} // namespace NYT
