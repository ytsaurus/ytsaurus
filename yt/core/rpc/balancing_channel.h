#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/ytree/public.h>

#include <core/actions/callback.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

using TDiscoverRequestHook = TCallback<void(NProto::TReqDiscover*)>;

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const Stroka& endpointDescription,
    const NYTree::IAttributeDictionary& endpointAttributes,
    TDiscoverRequestHook discoverRequestHook = TDiscoverRequestHook());

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
