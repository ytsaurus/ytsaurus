#pragma once

#include "private.h"
#include "distributed_hydra_manager.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <tuple>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogPtr> RunChangelogAcquisition(
    TDistributedHydraManagerConfigPtr config,
    TEpochContextPtr epochContext,
    int changelogId,
    std::optional<TPeerPriority> priority,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
