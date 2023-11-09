#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/dry_run/dry_run_hydra_manager.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

IHydraManagerPtr CreateDryRunHydraManager(
    TDistributedHydraManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr automatonInvoker,
    IAutomatonPtr automaton,
    ISnapshotStorePtr snapshotStore,
    const TDistributedHydraManagerOptions& options,
    NElection::TCellManagerPtr cellManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
