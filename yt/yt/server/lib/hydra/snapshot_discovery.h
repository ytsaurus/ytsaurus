#pragma once

#include "public.h"
#include "snapshot.h"

#include <yt/yt/ytlib/election/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Looks for the latest snapshot within the cell up to a given id.
/*!
 *  If none are found, then |InvalidSegmentId| is returned in the info.
 */
TFuture<TRemoteSnapshotParams> DiscoverLatestSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int maxSnapshotId = std::numeric_limits<i32>::max());

//! Looks for a particular snapshot within the cell.
/*!
 *  If the snapshot is not found, then an error is returned.
 */
TFuture<TRemoteSnapshotParams> DiscoverSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
