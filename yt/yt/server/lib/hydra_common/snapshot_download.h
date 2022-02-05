#pragma once

#include "private.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IFileSnapshotStorePtr fileStore,
    int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
