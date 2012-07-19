#pragma once

#include "leader_lookup.h"

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing leader among the peers.
NRpc::IChannelPtr CreateLeaderChannel(TLeaderLookup::TConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
