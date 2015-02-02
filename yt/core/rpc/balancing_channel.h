#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/callback.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

using TDiscoverRequestHook = TCallback<void(NProto::TReqDiscover*)>;

IChannelPtr CreateBalancingChannel(
    TBalancingChannelConfigPtr config,
    IChannelFactoryPtr channelFactory,
    TDiscoverRequestHook discoverRequestHook = TDiscoverRequestHook());

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
