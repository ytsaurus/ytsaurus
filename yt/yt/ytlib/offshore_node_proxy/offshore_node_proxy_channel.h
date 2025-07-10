#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateOffshoreNodeProxyChannel(
    const TOffshoreNodeProxyChannelConfigPtr& config,
    NRpc::IChannelFactoryPtr channelFactory,
    NRpc::IChannelPtr masterChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
