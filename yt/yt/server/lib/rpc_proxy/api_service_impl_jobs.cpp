#include "api_service_impl.h"

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NScheduler;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterJobMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AbandonJob));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PollJobShell));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(RunJobShellCommand)
        .SetStreamingEnabled(true)
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AbortJob));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(DumpJobProxyLog));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, AbandonJob)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());
    TAbandonJobOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("JobId", jobId);

    ExecuteCall(
        context,
        [=] {
            return client->AbandonJob(jobId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PollJobShell)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());
    auto parameters = TYsonString(request->parameters());
    auto shellName = YT_OPTIONAL_FROM_PROTO(*request, shell_name);

    TPollJobShellOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("JobId", jobId)
        .With("Parameters", parameters)
        .With("ShellName", shellName);

    ExecuteCall(
        context,
        [=] {
            return client->PollJobShell(jobId, shellName, parameters, options);
        },
        [] (const auto& context, const auto& pollJobShellResponse) {
            auto* response = &context->Response();
            response->set_result(ToProto(pollJobShellResponse.Result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RunJobShellCommand)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());
    auto command = request->command();
    auto shellName = YT_OPTIONAL_FROM_PROTO(*request, shell_name);

    TRunJobShellCommandOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("JobId", jobId)
        .With("Command", command)
        .With("ShellName", shellName);

    auto inputStream = WaitFor(client->RunJobShellCommand(jobId, shellName, command, options))
        .ValueOrThrow();

    HandleInputStreamingRequest(context, inputStream);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AbortJob)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());

    TAbortJobOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_interrupt_timeout()) {
        options.InterruptTimeout = FromProto<TDuration>(request->interrupt_timeout());
    }

    context->AnnotateRequest()
        .With("JobId", jobId)
        .With("InterruptTimeout", options.InterruptTimeout);

    ExecuteCall(
        context,
        [=] {
            return client->AbortJob(jobId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, DumpJobProxyLog)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto jobId = FromProto<TJobId>(request->job_id());
    auto operationId = FromProto<TOperationId>(request->operation_id());
    auto path = FromProto<TYPath>(request->path());

    TDumpJobProxyLogOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("JobId", jobId)
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->DumpJobProxyLog(jobId, operationId, path, options);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
