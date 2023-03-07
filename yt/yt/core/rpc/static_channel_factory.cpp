#include "static_channel_factory.h"

namespace NYT::NRpc {

using namespace NYT::NBus;

////////////////////////////////////////////////////////////////////////////////

TStaticChannelFactoryPtr TStaticChannelFactory::Add(const TString& address, IChannelPtr channel)
{
    YT_VERIFY(ChannelMap.insert(std::make_pair(address, channel)).second);
    return this;
}

IChannelPtr TStaticChannelFactory::CreateChannel(const TString& address)
{
    auto it = ChannelMap.find(address);
    if (it == ChannelMap.end()) {
        THROW_ERROR_EXCEPTION("Unknown address %Qv", address);
    }
    return it->second;
}

IChannelPtr TStaticChannelFactory::CreateChannel(const TAddressWithNetwork& addressWithNetwork)
{
    return CreateChannel(addressWithNetwork.Address);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
