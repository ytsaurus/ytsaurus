#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> DownloadChangelog(
    NHydra::TDistributedHydraManagerConfigPtr config,
    NElection::TCellManagerPtr cellManager,
    NHydra::IChangelogPtr changelog,
    int recordCount,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
