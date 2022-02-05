#pragma once

#include "private.h"

#include <yt/yt/ytlib/election/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr CreateLocalSnapshotStore(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IFileSnapshotStorePtr fileStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
