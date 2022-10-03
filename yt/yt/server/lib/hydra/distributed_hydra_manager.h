#pragma once

#include <yt/yt/server/lib/hydra_common/hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/distributed_hydra_manager.h>

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

IDistributedHydraManagerPtr CreateDistributedHydraManager(
    TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    IAutomatonPtr automaton,
    NRpc::IServerPtr rpcServer,
    NElection::IElectionManagerPtr electionManager,
    NElection::TCellId cellId,
    IChangelogStoreFactoryPtr changelogStoreFactory,
    ISnapshotStorePtr snapshotStore,
    NRpc::IAuthenticatorPtr authenticator,
    const TDistributedHydraManagerOptions& options = {},
    const TDistributedHydraManagerDynamicOptions& dynamicOptions = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
