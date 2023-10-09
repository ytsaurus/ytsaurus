#pragma once

#include "hydra_manager.h"

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedHydraManagerOptions
{
    bool UseFork = false;
    bool EnableObserverPersistence = true;
    NRpc::IResponseKeeperPtr ResponseKeeper;
};

struct TDistributedHydraManagerDynamicOptions
{
    bool AbandonLeaderLeaseDuringRecovery = true;
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedHydraManager
    : public IHydraManager
{
    //! Returns dynamic config.
    /*
     *   \note Thread affinity: any
     */
    virtual TDistributedHydraManagerDynamicOptions GetDynamicOptions() const = 0;

    //! Sets new dynamic config
    /*
     *   \note Thread affinity: any
     */
    virtual void SetDynamicOptions(const TDistributedHydraManagerDynamicOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedHydraManager)

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

} // namespace NYT::NHydra
