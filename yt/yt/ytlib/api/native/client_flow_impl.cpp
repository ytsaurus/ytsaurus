#include "client_impl.h"

#include "config.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/flow/lib/client/public.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NFlow;
using namespace NFlow::NController;

////////////////////////////////////////////////////////////////////////////////

TString TClient::DiscoverPipelineControllerLeader(const TYPath& pipelinePath)
{
    YT_LOG_DEBUG("Started discoverying pipeline controller leader (PipelinePath: %v)",
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

    // TODO(babenko): uncomment after refactoring pipeline creation
    // if (!attributes.Contains(PipelineFormatVersionAttribute)) {
    //     THROW_ERROR_EXCEPTION("%v is not a Flow pipeline store; missing attribute %Qv",
    //         pipelinePath,
    //         PipelineFormatVersionAttribute);
    // }

    // if (auto version = attributes.Get<int>(PipelineFormatVersionAttribute); version != CurrentPipelineFormatVersion) {
    //     THROW_ERROR_EXCEPTION("Invalid pipeline store version: expected %v, got %v",
    //         CurrentPipelineFormatVersion,
    //         version);
    // }

    auto address = attributes.Get<TString>(LeaderControllerAddressAttribute);

    YT_LOG_DEBUG("Finished discoverying pipeline controller leader (PipelinePath: %v, Address: %v)",
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
