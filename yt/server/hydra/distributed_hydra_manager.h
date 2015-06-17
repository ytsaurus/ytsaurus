#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/actions/public.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedHydraManagerOptions
{
    bool UseFork = false;
    NRpc::TResponseKeeperPtr ResponseKeeper;
};

IHydraManagerPtr CreateDistributedHydraManager(
    TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    IAutomatonPtr automaton,
    NRpc::IServerPtr rpcServer,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    const TDistributedHydraManagerOptions& options = TDistributedHydraManagerOptions());

///////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
