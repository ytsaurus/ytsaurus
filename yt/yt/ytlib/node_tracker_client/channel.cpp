#include "channel.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NNodeTrackerClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TNodeChannelFactory
    : public INodeChannelFactory
{
public:
    TNodeChannelFactory(IChannelFactoryPtr channelFactory, const TNetworkPreferenceList& networks)
        : ChannelFactory_(channelFactory)
        , Networks_(networks)
    { }

    IChannelPtr CreateChannel(const TNodeDescriptor& descriptor) override
    {
        const auto& address = descriptor.GetAddressOrThrow(Networks_);
        return CreateChannel(address, descriptor.GetDefaultAddress());
    }

    IChannelPtr CreateChannel(const TAddressMap& addresses) override
    {
        const auto& address = GetAddressOrThrow(addresses, Networks_);
        const auto endpointIdentity = FindDefaultAddress(addresses);
        return CreateChannel(address, endpointIdentity);
    }

    IChannelPtr CreateChannel(
        const std::string& address,
        const std::optional<std::string>& endpointIdentity) override
    {
        return ChannelFactory_->CreateChannel(address, endpointIdentity);
    }

private:
    const IChannelFactoryPtr ChannelFactory_;
    const TNetworkPreferenceList Networks_;
};

INodeChannelFactoryPtr CreateNodeChannelFactory(
    IChannelFactoryPtr channelFactory,
    const TNetworkPreferenceList& networks)
{
    return New<TNodeChannelFactory>(channelFactory, networks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
