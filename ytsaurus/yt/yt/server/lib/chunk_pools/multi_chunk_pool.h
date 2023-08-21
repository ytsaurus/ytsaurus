#pragma once

#include "public.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

IMultiChunkPoolInputPtr CreateMultiChunkPoolInput(
    std::vector<IPersistentChunkPoolInputPtr> underlyingPools);

IMultiChunkPoolOutputPtr CreateMultiChunkPoolOutput(
    std::vector<IPersistentChunkPoolOutputPtr> underlyingPools);

IMultiChunkPoolPtr CreateMultiChunkPool(
    std::vector<IPersistentChunkPoolPtr> underlyingPools);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
