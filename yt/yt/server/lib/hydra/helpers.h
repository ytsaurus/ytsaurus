#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const NElection::TCellManagerPtr& cellManager,
    const TDistributedHydraManagerOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
