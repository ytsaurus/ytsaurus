#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Describes a result of peer lookup.
struct TPeerDiscoveryResult
{
    //! Peer address.
    Stroka Address;

    //! Quorum epoch.
    TGuid EpochId;
};

//! Starts asynchronous peer discovery.
/*!
 *  For |Any| role, returns a peer uniformly chosen from alive quorum participants.
 *  For |Leader| role, returns the leader, obviously.
 *  For |Follower| role, returns a follower uniformly chosen from alive followers
 *  (however, if there's just one peer it returns the leader).
 */
TFuture<TErrorOr<TPeerDiscoveryResult>> DiscoverPeer(
    TPeerDiscoveryConfigPtr config,
    EPeerRole role);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
