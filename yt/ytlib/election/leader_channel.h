#pragma once

#include "leader_lookup.h"

#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing leader among the peers.
NRpc::IChannel::TPtr CreateLeaderChannel(NElection::TLeaderLookup::TConfig* config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
