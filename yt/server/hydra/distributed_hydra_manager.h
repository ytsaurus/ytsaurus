#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/actions/public.h>

#include <server/election/public.h>

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
    NElection::IElectionManagerPtr electionManager,
    NElection::TCellManagerPtr cellManager,
    IChangelogStoreFactoryPtr changelogStoreFactory,
    ISnapshotStorePtr snapshotStore,
    const TDistributedHydraManagerOptions& options = TDistributedHydraManagerOptions());

///////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
