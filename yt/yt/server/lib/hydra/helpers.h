#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const NElection::TCellManagerPtr& cellManager,
    const TDistributedHydraManagerOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
