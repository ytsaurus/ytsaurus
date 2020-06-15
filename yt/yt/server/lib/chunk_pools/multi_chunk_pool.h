#pragma once

#include "public.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPoolInput> CreateMultiChunkPoolInput(
    std::vector<IChunkPoolInput*> underlyingPools);

std::unique_ptr<IChunkPoolOutput> CreateMultiChunkPoolOutput(
    std::vector<IChunkPoolOutput*> underlyingPools);

std::unique_ptr<IChunkPool> CreateMultiChunkPool(
    std::vector<IChunkPool*> underlyingPools);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
