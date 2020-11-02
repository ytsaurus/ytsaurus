#pragma once

#include "public.h"

#include <yt/core/actions/callback.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TString endpointDescription,
    NYTree::IAttributeDictionaryPtr endpointAttributes,
    TDiscoverRequestHook discoverRequestHook = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
