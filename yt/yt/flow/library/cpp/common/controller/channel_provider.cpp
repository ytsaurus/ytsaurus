#include "channel_provider.h"

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TControllerChannelProvider::TControllerChannelProvider(std::function<NRpc::IChannelPtr()> getChannel)
    : GetChannelCallback_(std::move(getChannel))
{ }

void TControllerChannelProvider::SetChannel(NRpc::IChannelPtr channel)
{
    Channel_.Store(std::move(channel));
}

const std::string& TControllerChannelProvider::GetEndpointDescription() const
{
    static const std::string Description = "pipeline controller";
    return Description;
}

const NYTree::IAttributeDictionary& TControllerChannelProvider::GetEndpointAttributes() const
{
    return NYTree::EmptyAttributes();
}

TFuture<NRpc::IChannelPtr> TControllerChannelProvider::GetChannel()
{
    if (auto channel = GetChannelCallback_ ? GetChannelCallback_() : Channel_.Acquire()) {
        return MakeFuture(std::move(channel));
    }
    return MakeFuture<NRpc::IChannelPtr>(TError(
        NRpc::EErrorCode::Unavailable,
        "Worker has not discovered the pipeline controller"));
}

TFuture<NRpc::IChannelPtr> TControllerChannelProvider::GetChannel(const NRpc::IClientRequestPtr& /*request*/)
{
    return GetChannel();
}

TFuture<NRpc::IChannelPtr> TControllerChannelProvider::GetChannel(std::string /*serviceName*/)
{
    return GetChannel();
}

void TControllerChannelProvider::Terminate(const TError& /*error*/)
{
    // The connector owns the underlying channel.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
