#pragma once

#include "public.h"

#include <yt/ytlib/election/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

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
    IChangelogStoreFactoryPtr changelogStoreFactory,
    ISnapshotStorePtr snapshotStore,
    const TDistributedHydraManagerOptions& options = TDistributedHydraManagerOptions());

///////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
