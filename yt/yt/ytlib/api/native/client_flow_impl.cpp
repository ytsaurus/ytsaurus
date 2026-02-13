#include "client_impl.h"

#include "config.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/flow/lib/client/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NFlow;
using namespace NFlow::NController;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TOptions>
TFlowExecuteOptions MakeFlowExecuteOptions(const TOptions& options)
{
    TFlowExecuteOptions executeOptions;
    static_cast<TTimeoutOptions&>(executeOptions) = static_cast<const TTimeoutOptions&>(options);
    return executeOptions;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::string TClient::DiscoverPipelineControllerLeader(const TYPath& pipelinePath)
{
    YT_LOG_DEBUG("Started discovering pipeline controller leader (PipelinePath: %v)",
        pipelinePath);

    TGetNodeOptions options{
        .Attributes = TAttributeFilter(
            {
                PipelineFormatVersionAttribute,
                LeaderControllerAddressAttribute,
            }),
    };

    auto str = WaitFor(GetNode(pipelinePath, options))
        .ValueOrThrow();

    auto node = ConvertToNode(str);
    const auto& attributes = node->Attributes();

    if (!attributes.Contains(PipelineFormatVersionAttribute)) {
        THROW_ERROR_EXCEPTION("%v is not a valid pipeline; missing attribute %Qv",
            pipelinePath,
            PipelineFormatVersionAttribute);
    }

    if (auto version = attributes.Get<int>(PipelineFormatVersionAttribute); version != CurrentPipelineFormatVersion) {
        THROW_ERROR_EXCEPTION("Invalid pipeline format version: expected %v, got %v",
            CurrentPipelineFormatVersion,
            version);
    }

    if (!attributes.Contains(LeaderControllerAddressAttribute)) {
        THROW_ERROR_EXCEPTION("Cannot discover pipeline controller because attribute %Qv is not set on pipeline. "
            "Probably pipeline controller has never been successfully started or has been unable to publish itself",
            LeaderControllerAddressAttribute);
    }
    auto address = attributes.Get<std::string>(LeaderControllerAddressAttribute);

    YT_LOG_DEBUG("Finished discovering pipeline controller leader (PipelinePath: %v, Address: %v)",
        pipelinePath,
        address);

    return address;
}

TControllerServiceProxy TClient::CreatePipelineControllerLeaderProxy(const std::string& address)
{
    // Cannot use ChannelFactory_ here because it injects internal TVM ticket.
    auto channel = Connection_->GetChannelFactory()->CreateChannel(address);
    TControllerServiceProxy proxy(std::move(channel));
    proxy.SetDefaultTimeout(Connection_->GetConfig()->FlowPipelineControllerRpcTimeout);
    return proxy;
}

void TClient::ValidatePipelinePermission(const NYPath::TYPath& pipelinePath, NYTree::EPermission permission)
{
    NSecurityClient::TPermissionKey permissionKey{
        .Path = pipelinePath,
        .User = Options_.GetAuthenticatedUser(),
        .Permission = permission,
    };
    WaitFor(Connection_->GetPermissionCache()->Get(permissionKey))
        .ThrowOnError("No %v permission for pipeline %Qv", permission, pipelinePath);
}

////////////////////////////////////////////////////////////////////////////////

TGetPipelineSpecResult TClient::DoGetPipelineSpec(
    const NYPath::TYPath& pipelinePath,
    const TGetPipelineSpecOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
        .EndMap();
    auto executeResult = DoFlowExecute(pipelinePath, "get-pipeline-spec", executeArgument, MakeFlowExecuteOptions(options));
    auto executeResultNode = ConvertTo<IMapNodePtr>(executeResult.Result);
    return {
        .Version = executeResultNode->GetChildValueOrThrow<TVersion>("version"),
        .Spec = ConvertToYsonString(executeResultNode->GetChildOrThrow("spec")),
    };
}

TSetPipelineSpecResult TClient::DoSetPipelineSpec(
    const NYPath::TYPath& pipelinePath,
    const NYson::TYsonString& spec,
    const TSetPipelineSpecOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
            .Item("spec").Value(spec)
            .Item("expected_version").Value(options.ExpectedVersion)
            .Item("force").Value(options.Force)
        .EndMap();
    auto executeResult = DoFlowExecute(pipelinePath, "set-pipeline-spec", executeArgument, MakeFlowExecuteOptions(options));
    return {
        .Version = ConvertTo<IMapNodePtr>(executeResult.Result)->GetChildValueOrThrow<TVersion>("version"),
    };
}

TGetPipelineDynamicSpecResult TClient::DoGetPipelineDynamicSpec(
    const NYPath::TYPath& pipelinePath,
    const TGetPipelineDynamicSpecOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
        .EndMap();
    auto executeResult = DoFlowExecute(pipelinePath, "get-pipeline-dynamic-spec", executeArgument, MakeFlowExecuteOptions(options));
    auto executeResultNode = ConvertTo<IMapNodePtr>(executeResult.Result);
    return {
        .Version = executeResultNode->GetChildValueOrThrow<TVersion>("version"),
        .Spec = ConvertToYsonString(executeResultNode->GetChildOrThrow("spec")),
    };
}

TSetPipelineDynamicSpecResult TClient::DoSetPipelineDynamicSpec(
    const NYPath::TYPath& pipelinePath,
    const NYson::TYsonString& spec,
    const TSetPipelineDynamicSpecOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
            .Item("spec").Value(spec)
            .Item("expected_version").Value(options.ExpectedVersion)
        .EndMap();
    auto executeResult = DoFlowExecute(pipelinePath, "set-pipeline-dynamic-spec", executeArgument, MakeFlowExecuteOptions(options));
    return {
        .Version = ConvertTo<IMapNodePtr>(executeResult.Result)->GetChildValueOrThrow<TVersion>("version"),
    };
}

void TClient::DoStartPipeline(
    const TYPath& pipelinePath,
    const TStartPipelineOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
            .Item("target_pipeline_state").Value(EPipelineState::Completed)
        .EndMap();
    DoFlowExecute(pipelinePath, "set-target-pipeline-state", executeArgument, MakeFlowExecuteOptions(options));
}

void TClient::DoStopPipeline(
    const TYPath& pipelinePath,
    const TStopPipelineOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
            .Item("target_pipeline_state").Value(EPipelineState::Stopped)
        .EndMap();
    DoFlowExecute(pipelinePath, "set-target-pipeline-state", executeArgument, MakeFlowExecuteOptions(options));
}

void TClient::DoPausePipeline(
    const TYPath& pipelinePath,
    const TPausePipelineOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
            .Item("target_pipeline_state").Value(EPipelineState::Paused)
        .EndMap();
    DoFlowExecute(pipelinePath, "set-target-pipeline-state", executeArgument, MakeFlowExecuteOptions(options));
}

TPipelineState TClient::DoGetPipelineState(
    const TYPath& pipelinePath,
    const TGetPipelineStateOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
        .EndMap();
    auto executeResult = DoFlowExecute(pipelinePath, "get-pipeline-state", executeArgument, MakeFlowExecuteOptions(options));
    return {
        .State = ConvertTo<IMapNodePtr>(executeResult.Result)->GetChildValueOrThrow<EPipelineState>("pipeline_state"),
    };
}

TGetFlowViewResult TClient::DoGetFlowView(
    const TYPath& pipelinePath,
    const TYPath& viewPath,
    const TGetFlowViewOptions& options)
{
    auto executeArgument = BuildYsonStringFluently()
        .BeginMap()
            .Item("path").Value(viewPath)
            .Item("cache").Value(options.Cache)
        .EndMap();
    auto executeResult = DoFlowExecute(pipelinePath, "get-flow-view", executeArgument, MakeFlowExecuteOptions(options));
    return {
        .FlowViewPart = executeResult.Result,
    };
}

TFlowExecuteResult TClient::DoFlowExecute(
    const NYPath::TYPath& pipelinePath,
    const std::string& command,
    const NYson::TYsonString& argument,
    const TFlowExecuteOptions& options)
{
    ValidatePipelinePermission(pipelinePath, EPermission::Read);
    auto controllerAddress = DiscoverPipelineControllerLeader(pipelinePath);
    auto proxy = CreatePipelineControllerLeaderProxy(controllerAddress);

    auto executeRequest = [&](auto& req) {
        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.GetCode() == NRpc::EErrorCode::TransportError) {
            THROW_ERROR_EXCEPTION("Cannot connect to pipeline controller leader. "
                "Probably controller is stopped or it is failing")
                << TErrorAttribute("flow_execute_command", req->command())
                << TErrorAttribute("pipeline_path", pipelinePath)
                << TErrorAttribute("pipeline_controller_leader_address", controllerAddress)
                << rspOrError;
        }
        return rspOrError.ValueOrThrow();
    };

    // Get and check command-specific permission.
    {
        // Required permission can be cached per pipeline.
        // But this request type is rare and lighweight, so there is no need to cache it so far.
        auto req = proxy.FlowExecute();
        if (options.Timeout) {
            req->SetTimeout(options.Timeout);
        }
        req->set_command("get-command-required-permission");
        TYsonString argument = BuildYsonStringFluently()
            .BeginMap()
                .Item("command").Value(command)
            .EndMap();
        req->set_argument(ToProto(argument));
        auto rsp = executeRequest(req);
        auto requiredPermission = ConvertTo<IMapNodePtr>(TYsonString(rsp->result()))
            ->GetChildValueOrThrow<EPermission>("permission");
        if (requiredPermission != EPermission::Read) {
            ValidatePipelinePermission(pipelinePath, requiredPermission);
        }
    }

    auto req = proxy.FlowExecute();
    if (options.Timeout) {
        req->SetTimeout(options.Timeout);
    }
    req->set_command(command);
    if (argument) {
        req->set_argument(ToProto(argument));
    }
    req->set_user(Options_.GetAuthenticatedUser());
    auto rsp = executeRequest(req);
    return {
        .Result = rsp->has_result() ? TYsonString(rsp->result()) : TYsonString{},
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
