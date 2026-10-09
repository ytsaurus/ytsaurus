#include "api_service_impl.h"

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NScheduler;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterJobInfoMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ListJobs));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ListJobTraces));

    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(DumpJobContext));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetJobInput)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetJobInputPaths));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetJobSpec));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetJobStderr));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetJobTrace)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetJobFailContext));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetJob));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, ListJobs)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TListJobsOptions options;
    SetTimeoutOptions(&options, context.Get());

    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }

    if (request->has_type()) {
        options.Type = NYT::NApi::NRpcProxy::NProto::ConvertJobTypeFromProto(request->type());
    }
    if (request->has_state()) {
        options.State = ConvertJobStateFromProto(request->state());
    }
    if (request->has_address()) {
        options.Address = request->address();
    }
    if (request->has_with_stderr()) {
        options.WithStderr = request->with_stderr();
    }
    if (request->has_with_fail_context()) {
        options.WithFailContext = request->with_fail_context();
    }
    if (request->has_with_spec()) {
        options.WithSpec = request->with_spec();
    }
    if (request->has_with_competitors()) {
        options.WithCompetitors = request->with_competitors();
    }
    if (request->has_collective_id()) {
        options.CollectiveId = FromProto<NJobTrackerClient::TCollectiveId>(request->collective_id());
    }
    if (request->has_job_competition_id()) {
        options.JobCompetitionId = FromProto<TJobId>(request->job_competition_id());
    }
    if (request->has_with_monitoring_descriptor()) {
        options.WithMonitoringDescriptor = request->with_monitoring_descriptor();
    }
    if (request->has_with_interruption_info()) {
        options.WithInterruptionInfo = request->with_interruption_info();
    }
    if (request->has_task_name()) {
        options.TaskName = request->task_name();
    }
    if (request->has_operation_incarnation()) {
        options.OperationIncarnation = request->operation_incarnation();
    }
    if (request->has_from_time()) {
        options.FromTime = FromProto<TInstant>(request->from_time());
    }
    if (request->has_to_time()) {
        options.ToTime = FromProto<TInstant>(request->to_time());
    }
    if (request->has_continuation_token()) {
        options.ContinuationToken = request->continuation_token();
    }
    if (request->has_attributes()) {
        options.Attributes.emplace();
        NYT::CheckedHashSetFromProto(&(*options.Attributes), request->attributes().keys());
    }
    if (request->has_monitoring_descriptor()) {
        options.MonitoringDescriptor = request->monitoring_descriptor();
    }

    options.SortField = FromProto<EJobSortField>(request->sort_field());
    options.SortOrder = FromProto<EJobSortDirection>(request->sort_order());

    options.Limit = request->limit();
    options.Offset = request->offset();

    options.IncludeCypress = request->include_cypress();
    options.IncludeControllerAgent = request->include_controller_agent();
    options.IncludeArchive = request->include_archive();

    options.DataSource = FromProto<EDataSource>(request->data_source());
    options.RunningJobsLookbehindPeriod = FromProto<TDuration>(request->running_jobs_lookbehind_period());

    context->AnnotateRequest()
        .With("OperationIdOrAlias", operationIdOrAlias)
        .With("Type", options.Type)
        .With("State", options.State)
        .With("Address", options.Address)
        .With("IncludeCypress", options.IncludeCypress)
        .With("IncludeControllerAgent", options.IncludeControllerAgent)
        .With("IncludeArchive", options.IncludeArchive)
        .With("JobCompetitionId", options.JobCompetitionId)
        .With("WithCompetitors", options.WithCompetitors)
        .With("WithMonitoringDescriptor", options.WithMonitoringDescriptor)
        .With("WithInterruptionInfo", options.WithInterruptionInfo)
        .With("Attributes", options.Attributes)
        .With("MonitoringDescriptor", options.MonitoringDescriptor);

    ExecuteCall(
        context,
        [=] {
            return client->ListJobs(operationIdOrAlias, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_result(), result);

            context->AnnotateResponse()
                .With("CypressJobCount", result.CypressJobCount)
                .With("ControllerAgentJobCount", result.ControllerAgentJobCount)
                .With("ArchiveJobCount", result.ArchiveJobCount);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, DumpJobContext)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());
    auto path = request->path();

    TDumpJobContextOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("JobId", jobId)
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->DumpJobContext(jobId, path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetJobInput)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());

    TGetJobInputOptions options;
    SetTimeoutOptions(&options, context.Get());

    options.JobSpecSource = FromProto<EJobSpecSource>(request->job_spec_source());

    context->AnnotateRequest()
        .With("JobId", jobId);

    auto jobInputReader = WaitFor(client->GetJobInput(jobId, options))
        .ValueOrThrow();
    HandleInputStreamingRequest(context, jobInputReader);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetJobInputPaths)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());

    TGetJobInputPathsOptions options;
    SetTimeoutOptions(&options, context.Get());

    options.JobSpecSource = FromProto<EJobSpecSource>(request->job_spec_source());

    context->AnnotateRequest()
        .With("JobId", jobId);

    ExecuteCall(
        context,
        [=] {
            return client->GetJobInputPaths(jobId, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_paths(ToProto(result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetJobSpec)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());

    TGetJobSpecOptions options;
    SetTimeoutOptions(&options, context.Get());

    options.OmitNodeDirectory = request->omit_node_directory();
    options.OmitInputTableSpecs = request->omit_input_table_specs();
    options.OmitOutputTableSpecs = request->omit_output_table_specs();
    options.JobSpecSource = FromProto<EJobSpecSource>(request->job_spec_source());

    context->AnnotateRequest()
        .With("JobId", jobId)
        .With("OmitNodeDirectory", options.OmitNodeDirectory)
        .With("OmitInputTableSpecs", options.OmitInputTableSpecs)
        .With("OmitOutputTableSpecs", options.OmitOutputTableSpecs);

    ExecuteCall(
        context,
        [=] {
            return client->GetJobSpec(jobId, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_job_spec(ToProto(result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetJobStderr)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
    auto jobId = FromProto<TJobId>(request->job_id());

    TGetJobStderrOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.Type = FromProto<NApi::EJobStderrType>(request->type());

    context->AnnotateRequest()
        .With("OperationIdOrAlias", operationIdOrAlias)
        .With("JobId", jobId)
        .With("Limit", options.Limit)
        .With("Offset", options.Offset);

    ExecuteCall(
        context,
        [=] {
            return client->GetJobStderr(operationIdOrAlias, jobId, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            context->AnnotateResponse()
                .With("Size", result.Data.size())
                .With("TotalSize", result.TotalSize)
                .With("EndOffset", result.EndOffset);
            response->set_total_size(result.TotalSize);
            response->set_end_offset(result.EndOffset);
            response->Attachments().push_back(result.Data);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetJobTrace)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TGetJobTraceOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto jobId = FromProto<TJobId>(request->job_id());

    if (request->has_trace_id()) {
        options.TraceId = FromProto<NJobTrackerClient::TJobTraceId>(request->trace_id());
    }
    if (request->has_from_time()) {
        options.FromTime = FromProto<TInstant>(request->from_time());
    }
    if (request->has_to_time()) {
        options.ToTime = FromProto<TInstant>(request->to_time());
    }

    context->AnnotateRequest()
        .With("OperationIdOrAlias", operationIdOrAlias)
        .With("JobId", jobId)
        .With("TraceId", options.TraceId)
        .With("FromTime", options.FromTime)
        .With("ToTime", options.ToTime);

    auto jobTraceReader = WaitFor(client->GetJobTrace(operationIdOrAlias, jobId, options))
        .ValueOrThrow();

    HandleInputStreamingRequest(context, jobTraceReader);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ListJobTraces)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
    auto jobId = FromProto<TJobId>(request->job_id());

    TListJobTracesOptions options;
    SetTimeoutOptions(&options, context.Get());

    if (request->has_per_process()) {
        options.PerProcess = request->per_process();
    }

    options.Limit = request->limit();

    context->AnnotateRequest()
        .With("OperationIdOrAlias", operationIdOrAlias)
        .With("JobId", jobId)
        .With("PerProcess", options.PerProcess)
        .With("Limit", options.Limit);

    ExecuteCall(
        context,
        [=] {
            return client->ListJobTraces(operationIdOrAlias, jobId, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_traces(), result);

            context->AnnotateResponse()
                .With("TraceCount", result.size());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetJobFailContext)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
    auto jobId = FromProto<TJobId>(request->job_id());

    TGetJobFailContextOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("OperationIdOrAlias", operationIdOrAlias)
        .With("JobId", jobId);

    ExecuteCall(
        context,
        [=] {
            return client->GetJobFailContext(operationIdOrAlias, jobId, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->Attachments().push_back(std::move(result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetJob)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
    auto jobId = FromProto<TJobId>(request->job_id());

    TGetJobOptions options;
    SetTimeoutOptions(&options, context.Get());

    if (request->has_attributes()) {
        options.Attributes.emplace();
        FromProto(&(*options.Attributes), request->attributes().keys());
    } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
        // COMPAT(max42): remove when no clients older than Aug22 are there.
        options.Attributes.emplace();
        FromProto(&(*options.Attributes), request->legacy_attributes().keys());
    }

    context->AnnotateRequest()
        .With("OperationIdOrAlias", operationIdOrAlias)
        .With("JobId", jobId)
        .With("Attributes", options.Attributes);

    ExecuteCall(
        context,
        [=] {
            return client->GetJob(operationIdOrAlias, jobId, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_info(ToProto(result));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
