#pragma once

#include <yt/yt/server/lib/hydra_common/hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/distributed_hydra_manager.h>

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

NHydra::IDistributedHydraManagerPtr CreateDistributedHydraManager(
    NHydra::TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    NHydra::IAutomatonPtr automaton,
    NRpc::IServerPtr rpcServer,
    NElection::IElectionManagerPtr electionManager,
    NElection::TCellId cellId,
    NHydra::IChangelogStoreFactoryPtr changelogStoreFactory,
    NHydra::ISnapshotStorePtr snapshotStore,
    NRpc::IAuthenticatorPtr authenticator,
    const NHydra::TDistributedHydraManagerOptions& options = {},
    const NHydra::TDistributedHydraManagerDynamicOptions& dynamicOptions = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
