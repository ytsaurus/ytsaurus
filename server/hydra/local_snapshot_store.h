#pragma once

#include "public.h"

#include <yt/ytlib/election/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr CreateLocalSnapshotStore(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
