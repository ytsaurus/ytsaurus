#include "client_impl.h"

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYson;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TOperationId TClient::DoStartOperation(
    EOperationType type,
    const TYsonString& spec,
    const TStartOperationOptions& options)
{
    auto req = SchedulerOperationProxy_->StartOperation();
    SetTransactionId(req, options, true);
    SetMutationId(req, options);
    req->set_type(ToProto<int>(type));
    req->set_spec(spec.ToString());

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return FromProto<TOperationId>(rsp->operation_id());
}

void TClient::DoAbortOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TAbortOperationOptions& options)
{
    auto req = SchedulerOperationProxy_->AbortOperation();
    ToProto(req, operationIdOrAlias);
    if (options.AbortMessage) {
        req->set_abort_message(*options.AbortMessage);
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoSuspendOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TSuspendOperationOptions& options)
{
    auto req = SchedulerOperationProxy_->SuspendOperation();
    ToProto(req, operationIdOrAlias);
    req->set_abort_running_jobs(options.AbortRunningJobs);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoResumeOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TResumeOperationOptions& /*options*/)
{
    auto req = SchedulerOperationProxy_->ResumeOperation();
    ToProto(req, operationIdOrAlias);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoCompleteOperation(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TCompleteOperationOptions& /*options*/)
{
    auto req = SchedulerOperationProxy_->CompleteOperation();
    ToProto(req, operationIdOrAlias);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoUpdateOperationParameters(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TYsonString& parameters,
    const TUpdateOperationParametersOptions& /*options*/)
{
    auto req = SchedulerOperationProxy_->UpdateOperationParameters();
    ToProto(req, operationIdOrAlias);
    req->set_parameters(parameters.ToString());

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
