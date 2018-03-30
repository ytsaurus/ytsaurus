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
    RegisterParameter("enable_ui_mode", EnableUIMode)
        .Optional();
}

void TListOperationsCommand::BuildOperations(const TListOperationsResult& result, TFluentMap fluent)
{
    auto buildOperationsInUIMode = [&] (TFluentList fluent) {
        fluent
            .DoFor(result.Operations, [] (TFluentList fluent, const TOperation& operation) {
                fluent.Item()
                    .BeginAttributes()
                        .Item("operation_type").Value(operation.OperationType)
                        .Item("state").Value(operation.OperationState)
                        .Item("authenticated_user").Value(operation.AuthenticatedUser)
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
                    .EndAttributes()
                    .Value(operation.OperationId);
            });
    };

    if (EnableUIMode) {
        if (result.Incomplete) {
            fluent
                .Item("operations")
                    .BeginAttributes()
                        .Item("incomplete").Value(true)
                    .EndAttributes()
                    .BeginList()
                        .Do(buildOperationsInUIMode)
                    .EndList();
        } else {
            fluent
                .Item("operations")
                    .BeginList()
                        .Do(buildOperationsInUIMode)
                    .EndList();
        }
    } else {
        fluent
            .Item("operations").BeginList()
                .DoFor(result.Operations, [] (TFluentList fluent, const TOperation& operation) {
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(operation.OperationId)
                            .Item("type").Value(operation.OperationType)
                            .Item("state").Value(operation.OperationState)
                            .Item("authenticated_user").Value(operation.AuthenticatedUser)
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
    RegisterParameter("include_scheduler", Options.IncludeScheduler)
        .Alias("include_runtime")
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
            .Item("scheduler_job_count").Value(result.SchedulerJobCount)
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
}

void TGetJobCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetJob(OperationId, JobId, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(result));
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

TStartOperationCommand::TStartOperationCommand()
{
    RegisterParameter("spec", Spec);
    RegisterParameter("operation_type", OperationType)
        .Default();
}

EOperationType TStartOperationCommand::GetOperationType() const
{
    return OperationType;
}

void TStartOperationCommand::DoExecute(ICommandContextPtr context)
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

TUpdateOperationParametersCommand::TUpdateOperationParametersCommand()
{
    RegisterParameter("operation_id", OperationId);
    RegisterParameter("parameters", Parameters);

    RegisterPostprocessor([&] {
        auto parameters = Parameters->AsMap();

        auto ownersNode = parameters->FindChild("owners");
        if (ownersNode) {
            std::vector<TString> owners;
            Deserialize(owners, ownersNode);
            Options.Owners = std::move(owners);
        }

        auto schedulingOptionsPerPoolTreeNode = parameters->FindChild("scheduling_options_per_pool_tree");
        if (schedulingOptionsPerPoolTreeNode) {
            Deserialize(Options.SchedulingOptionsPerPoolTree, schedulingOptionsPerPoolTreeNode);
        }
    });
}

void TUpdateOperationParametersCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->UpdateOperationParameters(OperationId, Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TGetOperationCommand::TGetOperationCommand()
{
    RegisterParameter("operation_id", OperationId);
    RegisterParameter("attributes", Options.Attributes)
        .Optional();
    RegisterParameter("include_scheduler", Options.IncludeScheduler)
        .Optional();
}

void TGetOperationCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetOperation(OperationId, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
