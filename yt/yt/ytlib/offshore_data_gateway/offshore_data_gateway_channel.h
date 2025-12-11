#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/ytlib/api/native/public.h>
#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateOffshoreDataGatewayChannel(
    const TOffshoreDataGatewayChannelConfigPtr& config,
    NRpc::IChannelFactoryPtr channelFactory,
    NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
