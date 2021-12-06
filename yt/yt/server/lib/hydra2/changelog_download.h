#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
