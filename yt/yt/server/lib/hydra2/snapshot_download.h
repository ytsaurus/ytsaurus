#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore,
    int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
