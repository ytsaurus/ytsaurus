#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TAsyncError DownloadChangelog(
    TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    IChangelogStorePtr changelogStore,
    int changelogId,
    int recordCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
