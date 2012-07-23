#pragma once

// TODO: FixMe
#include <ytlib/meta_state/public.h>

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing leader among the peers.
NRpc::IChannelPtr CreateLeaderChannel(NMetaState::TMasterDiscoveryConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
