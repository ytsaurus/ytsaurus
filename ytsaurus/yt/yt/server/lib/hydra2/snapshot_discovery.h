#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>

#include <yt/yt/ytlib/election/public.h>

namespace NYT::NHydra2 {

///////////////////////////////////////////////////////////////////////////////

//! Looks for the latest snapshot within the cell up to a given id.
/*!
 *  If none are found, then |InvalidSegmentId| is returned in the info.
 */
TFuture<NHydra::TRemoteSnapshotParams> DiscoverLatestSnapshot(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int maxSnapshotId = std::numeric_limits<i32>::max());

//! Looks for a particular snapshot within the cell.
/*!
 *  If the snapshot is not found, then an error is returned.
 */
TFuture<NHydra::TRemoteSnapshotParams> DiscoverSnapshot(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
