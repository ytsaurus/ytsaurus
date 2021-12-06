#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr CreateLocalSnapshotStore(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
