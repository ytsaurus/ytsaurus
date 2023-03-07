#pragma once

#include "public.h"

#include <yt/core/actions/callback.h>

#include <yt/core/misc/error.h>

#include <yt/core/ytree/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

using TDiscoverRequestHook = TCallback<void(NProto::TReqDiscover*)>;

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const TString& endpointDescription,
    const NYTree::IAttributeDictionary& endpointAttributes,
    TDiscoverRequestHook discoverRequestHook = TDiscoverRequestHook());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
