#include "api_service_impl.h"

#include <yt/yt/server/lib/misc/format_manager.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>
#include <yt/yt/client/scheduler/spec_patch.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NScheduler;
using namespace NYTree;
using namespace NYson;
using namespace NServer;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterOperationMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(StartOperation));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AbortOperation));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(SuspendOperation));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(ResumeOperation));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(CompleteOperation));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(UpdateOperationParameters));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PatchOperationSpec));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, StartOperation)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto type = NYT::NApi::NRpcProxy::NProto::ConvertOperationTypeFromProto(request->type());
    auto specYson = TYsonString(request->spec());

    {
        auto user = context->GetAuthenticationIdentity().User;
        const auto& config = Config_.Acquire();
        const auto& formatConfigs = config->Formats;
        TFormatManager formatManager(formatConfigs, user);
        auto specNode = ConvertToNode(specYson);
        formatManager.ValidateAndPatchOperationSpec(specNode, type);
        specYson = ConvertToYsonString(specNode);
    }

    TStartOperationOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());

    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }

    context->AnnotateRequest()
        .With("OperationType", type)
        .With("Spec", specYson);

    ExecuteCall(
        context,
        [=] {
            return client->StartOperation(type, specYson, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            context->AnnotateResponse()
                .With("OperationId", result);
            ToProto(response->mutable_operation_id(), result);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AbortOperation)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TAbortOperationOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_abort_message()) {
        options.AbortMessage = request->abort_message();
    }

    context->AnnotateRequest()
        .With("OperationId", operationIdOrAlias)
        .With("AbortMessage", options.AbortMessage);

    ExecuteCall(
        context,
        [=] {
            return client->AbortOperation(operationIdOrAlias, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SuspendOperation)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TSuspendOperationOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_abort_running_jobs()) {
        options.AbortRunningJobs = request->abort_running_jobs();
    }
    if (request->has_reason()) {
        options.Reason = request->reason();
    }

    context->AnnotateRequest()
        .With("OperationId", operationIdOrAlias)
        .With("AbortRunningJobs", options.AbortRunningJobs);

    ExecuteCall(
        context,
        [=] {
            return client->SuspendOperation(operationIdOrAlias, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ResumeOperation)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TResumeOperationOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("OperationId", operationIdOrAlias);

    ExecuteCall(
        context,
        [=] {
            return client->ResumeOperation(operationIdOrAlias, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CompleteOperation)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TCompleteOperationOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("OperationId", operationIdOrAlias);

    ExecuteCall(
        context,
        [=] {
            return client->CompleteOperation(operationIdOrAlias, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, UpdateOperationParameters)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    auto parameters = TYsonString(request->parameters());

    TUpdateOperationParametersOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("OperationId", operationIdOrAlias)
        .With("Parameters", parameters);

    ExecuteCall(
        context,
        [=] {
            return client->UpdateOperationParameters(
                operationIdOrAlias,
                parameters,
                options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PatchOperationSpec)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TSpecPatchList patches;
    for (const auto& patch : request->patches()) {
        patches.emplace_back(New<TSpecPatch>());
        NScheduler::FromProto(patches.back(), &patch);
    }

    TPatchOperationSpecOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("OperationId", operationIdOrAlias)
        .With("Patches", MakeFormattableView(patches, TDefaultFormatter()));

    ExecuteCall(
        context,
        [=] {
            return client->PatchOperationSpec(
                operationIdOrAlias,
                patches,
                options);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
