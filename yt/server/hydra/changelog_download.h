#pragma once

#include "public.h"

#include <core/actions/future.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
