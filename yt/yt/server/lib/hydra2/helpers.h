#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/election/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const NElection::TCellManagerPtr& cellManager,
    const NHydra::TDistributedHydraManagerOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
