#pragma once

#include "public.h"

#include <yt/server/election/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/profiling/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedHydraManagerOptions
{
    bool UseFork = false;
    bool WriteChangelogsAtFollowers = true;
    bool WriteSnapshotsAtFollowers = true;
    NRpc::TResponseKeeperPtr ResponseKeeper;
    NProfiling::TTagIdList ProfilingTagIds;
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
    const TDistributedHydraManagerOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
