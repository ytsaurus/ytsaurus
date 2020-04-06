#pragma once

#include "hydra_manager.h"

#include <yt/server/lib/election/public.h>

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

struct TDistributedHydraManagerDynamicOptions
{
    bool AbandonLeaderLeaseDuringRecovery = false;
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
    const TDistributedHydraManagerOptions& options = {},
    const TDistributedHydraManagerDynamicOptions& dynamicOptions = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
