#pragma once

#include "public.h"

#include <yt/core/rpc/channel.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct INodeChannelFactory
    : public NRpc::IChannelFactory
{
    using NRpc::IChannelFactory::CreateChannel;
    virtual NRpc::IChannelPtr CreateChannel(const TNodeDescriptor& descriptor) = 0;
    virtual NRpc::IChannelPtr CreateChannel(const TAddressMap& addresses) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeChannelFactory)

INodeChannelFactoryPtr CreateNodeChannelFactory(
    NYT::NRpc::IChannelFactoryPtr channelFactory,
    const TNetworkPreferenceList& networks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
