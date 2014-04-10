#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing the leader among Hydra peers.
// TODO(babenko): channels to followers
NRpc::IChannelPtr CreateLeaderChannel(
    TPeerConnectionConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
