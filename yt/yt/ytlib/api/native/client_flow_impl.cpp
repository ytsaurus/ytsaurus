#include "client_impl.h"

#include "config.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/flow/lib/client/public.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NFlow;
using namespace NFlow::NController;

////////////////////////////////////////////////////////////////////////////////

TString TClient::DiscoverPipelineControllerLeader(const TYPath& pipelinePath)
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

    auto address = attributes.Get<TString>(LeaderControllerAddressAttribute);

    YT_LOG_DEBUG("Finished discovering pipeline controller leader (PipelinePath: %v, Address: %v)",
        pipelinePath,
        address);

    return address;
}

TControllerServiceProxy TClient::CreatePipelineControllerLeaderProxy(const TYPath& pipelinePath)
{
    auto address = DiscoverPipelineControllerLeader(pipelinePath);
    auto channel = ChannelFactory_->CreateChannel(address);
    TControllerServiceProxy proxy(std::move(channel));
    proxy.SetDefaultTimeout(Connection_->GetConfig()->FlowPipelineControllerRpcTimeout);
    return proxy;
}

TGetPipelineSpecResult TClient::DoGetPipelineSpec(
    const NYPath::TYPath& pipelinePath,
    const TGetPipelineSpecOptions& /*options*/)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.GetSpec();
    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return {
        .Version = FromProto<TVersion>(rsp->version()),
        .Spec = TYsonString(rsp->spec()),
    };
}

TSetPipelineSpecResult TClient::DoSetPipelineSpec(
    const NYPath::TYPath& pipelinePath,
    const NYson::TYsonString& spec,
    const TSetPipelineSpecOptions& options)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.SetSpec();
    req->set_spec(spec.ToString());
    req->set_force(options.Force);
    if (options.ExpectedVersion) {
        req->set_expected_version(ToProto<i64>(*options.ExpectedVersion));
    }
    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return {
        .Version = FromProto<TVersion>(rsp->version()),
    };
}

TGetPipelineDynamicSpecResult TClient::DoGetPipelineDynamicSpec(
    const NYPath::TYPath& pipelinePath,
    const TGetPipelineDynamicSpecOptions& /*options*/)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.GetDynamicSpec();
    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return {
        .Version = FromProto<TVersion>(rsp->version()),
        .Spec = TYsonString(rsp->spec()),
    };
}

TSetPipelineDynamicSpecResult TClient::DoSetPipelineDynamicSpec(
    const NYPath::TYPath& pipelinePath,
    const NYson::TYsonString& spec,
    const TSetPipelineDynamicSpecOptions& options)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.SetDynamicSpec();
    req->set_spec(spec.ToString());
    if (options.ExpectedVersion) {
        req->set_expected_version(ToProto<i64>(*options.ExpectedVersion));
    }
    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return {
        .Version = FromProto<TVersion>(rsp->version()),
    };
}

void TClient::DoStartPipeline(
    const TYPath& pipelinePath,
    const TStartPipelineOptions& /*options*/)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.StartPipeline();
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoStopPipeline(
    const TYPath& pipelinePath,
    const TStopPipelineOptions& /*options*/)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.StopPipeline();
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoPausePipeline(
    const TYPath& pipelinePath,
    const TPausePipelineOptions& /*options*/)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.PausePipeline();
    WaitFor(req->Invoke())
        .ThrowOnError();
}

TPipelineStatus TClient::DoGetPipelineStatus(
    const TYPath& pipelinePath,
    const TGetPipelineStatusOptions& /*options*/)
{
    auto proxy = CreatePipelineControllerLeaderProxy(pipelinePath);
    auto req = proxy.GetPipelineStatus();
    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return {
        .State = CheckedEnumCast<EPipelineState>(rsp->state()),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
