#pragma once

#include "public.h"

#include <yt/yt/core/rpc/grpc/public.h>

namespace NYT::NOrm::NClient::NNative {

NRpc::IChannelFactoryPtr CreateChannelFactory(
    const NRpc::NGrpc::TChannelConfigTemplatePtr& grpcChannelConfigTemplate,
    IOrmPeerDiscoveryPtr peerDiscovery,
    std::optional<TString> discoveryAddress,
    bool useIP6Addresses);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
