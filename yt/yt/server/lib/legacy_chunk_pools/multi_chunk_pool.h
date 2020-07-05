#pragma once

#include "public.h"

namespace NYT::NLegacyChunkPools {

////////////////////////////////////////////////////////////////////////////////

IMultiChunkPoolInputPtr CreateMultiChunkPoolInput(
    std::vector<IChunkPoolInputPtr> underlyingPools);

IMultiChunkPoolOutputPtr CreateMultiChunkPoolOutput(
    std::vector<IChunkPoolOutputPtr> underlyingPools);

IMultiChunkPoolPtr CreateMultiChunkPool(
    std::vector<IChunkPoolPtr> underlyingPools);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLegacyChunkPools
