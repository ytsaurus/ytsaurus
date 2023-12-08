#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NBundleController {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateBundleControllerChannel(
    TBundleControllerChannelConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    NRpc::IChannelPtr masterChannel,
    const NNodeTrackerClient::TNetworkPreferenceList& networks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
