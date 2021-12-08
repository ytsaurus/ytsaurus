#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/election/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

NHydra::ISnapshotStorePtr CreateLocalSnapshotStore(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
