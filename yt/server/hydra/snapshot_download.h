#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore,
    int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
