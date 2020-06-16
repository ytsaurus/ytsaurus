#pragma once

#include "public.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

IChunkPoolInputPtr CreateMultiChunkPoolInput(
    std::vector<IChunkPoolInputPtr> underlyingPools);

IChunkPoolOutputPtr CreateMultiChunkPoolOutput(
    std::vector<IChunkPoolOutputPtr> underlyingPools);

IChunkPoolPtr CreateMultiChunkPool(
    std::vector<IChunkPoolPtr> underlyingPools);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
