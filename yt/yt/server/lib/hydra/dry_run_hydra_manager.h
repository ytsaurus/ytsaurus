#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/dry_run/dry_run_hydra_manager.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

NHydra::IHydraManagerPtr CreateDryRunHydraManager(
    NHydra::TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    NHydra::IAutomatonPtr automaton,
    NHydra::ISnapshotStorePtr snapshotStore,
    const NHydra::TDistributedHydraManagerOptions& options,
    NElection::TCellManagerPtr cellManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
