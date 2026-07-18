#include "controller_service.h"

#include "config.h"
#include "controller.h"
#include "flow_executor.h"
#include "persisted_state_manager.h"
#include "private.h"

#include <yt/yt/flow/library/cpp/client/controller/controller_service_proxy.h>

#include <yt/yt/flow/library/cpp/common/authenticator.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/build/build.h>

namespace NYT::NFlow::NController {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TControllerService
    : public TServiceBase
{
public:
    TControllerService(
        IFlowExecutorPtr flowExecutor,
        IPipelineAuthenticatorPtr authenticator,
        IInvokerPtr invoker)
        : NRpc::TServiceBase(
            std::move(invoker),
            TControllerServiceProxy::GetDescriptor(),
            ControllerLogger(),
            TServiceOptions{
                .Authenticator = authenticator->CreateYTControllerRpcAuthenticator(),
            })
        , FlowExecutor_(flowExecutor)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetFlowView));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetSpec));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetDynamicSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetDynamicSpec));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetPipelineState));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartPipeline));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PausePipeline));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StopPipeline));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlowExecute));
    }

private:
    const IFlowExecutorPtr FlowExecutor_;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetFlowView)
    {
        TGetFlowViewArg executorArg;
        executorArg.Path = request->path();
        executorArg.Cache = request->cache();
        auto executorResult = FlowExecutor_->GetFlowView(executorArg);
        response->set_flow_view_part(ToProto(executorResult));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetSpec)
    {
        context->SetRequestInfo();

        TGetPipelineSpecArg executorArg;
        auto executorResult = FlowExecutor_->GetPipelineSpec(executorArg);

        response->set_spec(ToProto(ConvertToYsonString(executorResult.Spec)));
        response->set_version(ToProto(executorResult.Version));
        context->SetResponseInfo("Version: %v", executorResult.Version);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SetSpec)
    {
        TSetPipelineSpecArg executorArg;
        executorArg.ExpectedVersion = request->has_expected_version()
            ? std::make_optional(FromProto<TVersion>(request->expected_version()))
            : std::nullopt;
        executorArg.Spec = ConvertTo<NYTree::INodePtr>(TYsonStringBuf(request->spec()));
        executorArg.Force = request->force();

        context->SetRequestInfo("ExpectedVersion: %v, Force: %v", executorArg.ExpectedVersion, executorArg.Force);

        auto executorResult = FlowExecutor_->SetPipelineSpec(executorArg);

        response->set_version(ToProto(executorResult.Version));
        context->SetResponseInfo("Version: %v", executorResult.Version);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetDynamicSpec)
    {
        TGetPipelineDynamicSpecArg executorArg;
        context->SetRequestInfo();

        auto executorResult = FlowExecutor_->GetPipelineDynamicSpec(executorArg);

        response->set_spec(ToProto(ConvertToYsonString(executorResult.Spec)));
        response->set_version(ToProto(executorResult.Version));
        context->SetResponseInfo("Version: %v", executorResult.Version);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SetDynamicSpec)
    {
        TSetPipelineDynamicSpecArg executorArg;
        executorArg.ExpectedVersion = request->has_expected_version()
            ? std::make_optional(FromProto<TVersion>(request->expected_version()))
            : std::nullopt;
        executorArg.Spec = ConvertTo<NYTree::INodePtr>(TYsonStringBuf(request->spec()));

        context->SetRequestInfo("ExpectedVersion: %v", executorArg.ExpectedVersion);

        auto executorResult = FlowExecutor_->SetPipelineDynamicSpec(executorArg);

        response->set_version(ToProto(executorResult.Version));
        context->SetResponseInfo("Version: %v", executorResult.Version);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetPipelineState)
    {
        auto executorResult = FlowExecutor_->GetPipelineState(TGetPipelineStateArg());
        response->set_state(ToProto(executorResult.PipelineState));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, StartPipeline)
    {
        SetTargetPipelineState(EPipelineState::Completed);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, StopPipeline)
    {
        SetTargetPipelineState(EPipelineState::Stopped);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PausePipeline)
    {
        SetTargetPipelineState(EPipelineState::Paused);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, FlowExecute)
    {
        constexpr auto isEntity = [] (TStringBuf ysonString) {
            TMemoryInput input(ysonString);
            TYsonPullParser parser(&input, EYsonType::Node);
            return parser.Next().GetType() == EYsonItemType::EntityValue && parser.Next().GetType() == EYsonItemType::EndOfStream;
        };

        TYsonString argument;
        if (request->argument().empty() || isEntity(request->argument())) {
            argument = TYsonString(TStringBuf("{}"), EYsonType::Node);
        } else {
            argument = TYsonString(request->argument(), EYsonType::Node);
        }
        response->set_result(FlowExecutor_->Execute(request->command(), argument, request->user()).ToString());
        context->Reply();
    }

private:
    void SetTargetPipelineState(EPipelineState state)
    {
        TSetTargetPipelineStateArg executorArg;
        executorArg.TargetPipelineState = state;
        FlowExecutor_->SetTargetPipelineState(executorArg);
    }
};

IServicePtr CreateControllerService(
    IFlowExecutorPtr flowExecutor,
    IPipelineAuthenticatorPtr authenticator,
    IInvokerPtr invoker)
{
    return New<TControllerService>(
        std::move(flowExecutor),
        std::move(authenticator),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
