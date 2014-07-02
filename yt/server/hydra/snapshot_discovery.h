#pragma once

#include "public.h"
#include "snapshot.h"

#include <core/actions/future.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TRemoteSnapshotParams
    : public TSnapshotParams
{
    TRemoteSnapshotParams();

    TPeerId PeerId;
    int SnapshotId;
};

//! Looks for the latest snapshot within the cell up to a given id.
/*!
 *  If none are found, then |NonexistingSegmentId| is returned in the info.
 */
TFuture<TRemoteSnapshotParams> DiscoverLatestSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int maxSnapshotId);

//! Looks for a particular snapshot within the cell.
/*!
 *  If the snapshot is not found, then |NonexistingSegmentId| is returned in the info.
 */
TFuture<TRemoteSnapshotParams> DiscoverSnapshot(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
