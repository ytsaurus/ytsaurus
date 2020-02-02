#pragma once

#include "roaming_channel.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IReconfigurableRoamingChannelProvider
    : public IRoamingChannelProvider
{
    //! Replaces the current underlying channel with the provided one.
    virtual void SetUnderlyingChannel(IChannelPtr channel) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReconfigurableRoamingChannelProvider)

IReconfigurableRoamingChannelProviderPtr CreateReconfigurableRoamingChannelProvider(
    IChannelPtr initialUnderlyingChannel,
    const TString& endpointDescription,
    const NYTree::IAttributeDictionary& endpointAttributes,
    const TNetworkId& networkId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
