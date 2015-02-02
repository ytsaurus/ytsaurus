#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing a peer of the suitable kind
//! among Hydra peers.
NRpc::IChannelPtr CreatePeerChannel(
    TPeerConnectionConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    EPeerKind kind);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
