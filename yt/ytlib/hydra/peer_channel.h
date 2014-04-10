#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing a peer with the requested role.
// TODO(babenko): currently always discovers the leader
NRpc::IChannelPtr CreatePeerChannel(
    TPeerDiscoveryConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    EPeerRole role);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
