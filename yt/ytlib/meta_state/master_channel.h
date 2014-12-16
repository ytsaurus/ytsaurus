#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel that takes care of choosing leader among the peers.
NRpc::IChannelPtr CreateLeaderChannel(
    TMasterDiscoveryConfigPtr config,
    TCallback<bool(const TError&)> isRetriableError = BIND(&NRpc::IsRetriableError));

//! Creates channel pointing to a random master (possibly a follower).
NRpc::IChannelPtr CreateMasterChannel(TMasterDiscoveryConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
