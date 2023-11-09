#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/logging/log.h>

namespace NYT::NHydra {

///////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TDistributedHydraManagerOptions options,
    NElection::TCellManagerPtr cellManager,
    ISnapshotStorePtr store,
    int snapshotId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
