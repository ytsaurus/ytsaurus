#include "api_service_impl.h"

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterFlowMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetPipelineSpec));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(SetPipelineSpec));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetPipelineDynamicSpec));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(SetPipelineDynamicSpec));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(StartPipeline));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(StopPipeline));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PausePipeline));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetPipelineState));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetFlowView));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(FlowExecute));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, GetPipelineSpec)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TGetPipelineSpecOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath);

    ExecuteCall(
        context,
        [=] {
            return client->GetPipelineSpec(pipelinePath, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_version(ToProto(result.Version));
            response->set_spec(ToProto(result.Spec));

            context->AnnotateResponse()
                .With("Version", result.Version);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SetPipelineSpec)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TSetPipelineSpecOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());

    auto spec = TYsonString(request->spec());

    options.Force = request->force();

    options.ExpectedVersion = request->has_expected_version()
        ? std::make_optional<NFlow::TVersion>(request->expected_version())
        : std::nullopt;

    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath)
        .With("Force", options.Force)
        .With("ExpectedVersion", options.ExpectedVersion);

    ExecuteCall(
        context,
        [=] {
            return client->SetPipelineSpec(pipelinePath, spec, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_version(ToProto(result.Version));

            context->AnnotateResponse()
                .With("Version", result.Version);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetPipelineDynamicSpec)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TGetPipelineDynamicSpecOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath);

    ExecuteCall(
        context,
        [=] {
            return client->GetPipelineDynamicSpec(pipelinePath, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_version(ToProto(result.Version));
            response->set_spec(ToProto(result.Spec));

            context->AnnotateResponse()
                .With("Version", result.Version);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SetPipelineDynamicSpec)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TSetPipelineDynamicSpecOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());

    auto spec = TYsonString(request->spec());

    options.ExpectedVersion = request->has_expected_version()
        ? std::make_optional<NFlow::TVersion>(request->expected_version())
        : std::nullopt;

    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath)
        .With("ExpectedVersion", options.ExpectedVersion);

    ExecuteCall(
        context,
        [=] {
            return client->SetPipelineDynamicSpec(pipelinePath, spec, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_version(ToProto(result.Version));

            context->AnnotateResponse()
                .With("Version", result.Version);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, StartPipeline)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TStartPipelineOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath);

    ExecuteCall(
        context,
        [=] {
            return client->StartPipeline(pipelinePath, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, StopPipeline)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TStopPipelineOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath);

    ExecuteCall(
        context,
        [=] {
            return client->StopPipeline(pipelinePath, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PausePipeline)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TPausePipelineOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath);

    ExecuteCall(
        context,
        [=] {
            return client->PausePipeline(pipelinePath, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetPipelineState)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TGetPipelineStateOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath);

    ExecuteCall(
        context,
        [=] {
            return client->GetPipelineState(pipelinePath, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_state(ToProto(result.State));

            context->AnnotateResponse()
                .With("State", result.State);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetFlowView)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TGetFlowViewOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.Cache = request->cache();

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    auto viewPath = FromProto<TYPath>(request->view_path());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath)
        .With("ViewPath", viewPath);

    ExecuteCall(
        context,
        [=] {
            return client->GetFlowView(pipelinePath, viewPath, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_flow_view_part(ToProto(result.FlowViewPart));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, FlowExecute)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TFlowExecuteOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto pipelinePath = FromProto<TYPath>(request->pipeline_path());
    auto command = request->command();
    auto argument = NYson::TYsonString(request->argument());
    context->AnnotateRequest()
        .With("PipelinePath", pipelinePath)
        .With("Command", command);

    ExecuteCall(
        context,
        [=] {
            return client->FlowExecute(pipelinePath, command, argument, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_result(ToProto(result.Result));
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
