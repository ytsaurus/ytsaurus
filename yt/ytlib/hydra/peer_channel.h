#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing a peer with the requested role.
NRpc::IChannelPtr CreatePeerChannel(
    TPeerDiscoveryConfigPtr config,
    EPeerRole role);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
