#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadChangelog(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    NHydra::IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
