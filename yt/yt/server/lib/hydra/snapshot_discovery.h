#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Looks for the latest snapshot within the cell up to a given id.
/*!
 *  If none are found, then |InvalidSegmentId| is returned in the info.
 */
TFuture<TRemoteSnapshotParams> DiscoverLatestSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int maxSnapshotId);

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
