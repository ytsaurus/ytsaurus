#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTabletBalancerClient {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateTabletBalancerChannel(
    TTabletBalancerChannelConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    NRpc::IChannelPtr masterChannel,
    const NNodeTrackerClient::TNetworkPreferenceList& networks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancerClient
