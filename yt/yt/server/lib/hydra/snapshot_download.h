#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/logging/log.h>

namespace NYT::NHydra {

///////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadSnapshot(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NHydra::TDistributedHydraManagerOptions options,
    NElection::TCellManagerPtr cellManager,
    NHydra::ISnapshotStorePtr store,
    int snapshotId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
