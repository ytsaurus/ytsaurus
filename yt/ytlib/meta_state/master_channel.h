#pragma once

#include "public.h"

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing leader among the peers.
NRpc::IChannelPtr CreateLeaderChannel(TMasterDiscoveryConfigPtr config);

//! Creates channel pointing to a random master (possibly a follower).
NRpc::IChannelPtr CreateMasterChannel(TMasterDiscoveryConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
