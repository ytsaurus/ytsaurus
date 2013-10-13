#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TAsyncError DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    ISnapshotStorePtr snapshotStore,
    int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
