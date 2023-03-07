#pragma once

#include "public.h"

#include <yt/ytlib/election/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
