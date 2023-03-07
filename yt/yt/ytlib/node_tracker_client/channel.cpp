#include "channel.h"

#include <yt/client/node_tracker_client/node_directory.h>

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

    virtual IChannelPtr CreateChannel(const TNodeDescriptor& descriptor) override
    {
        return CreateChannel(descriptor.Addresses());
    }

    virtual IChannelPtr CreateChannel(const TAddressMap& addresses) override
    {
        const auto& addressWithNetwork = GetAddressWithNetworkOrThrow(addresses, Networks_);

        return CreateChannel(addressWithNetwork);
    }

    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        return ChannelFactory_->CreateChannel(address);
    }

    virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override
    {
        return ChannelFactory_->CreateChannel(addressWithNetwork);
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
