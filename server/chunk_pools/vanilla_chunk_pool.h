#pragma once

#include "chunk_pool.h"

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

// NB: Vanilla chunk pool implements only IChunkPoolOutput.
std::unique_ptr<IChunkPoolOutput> CreateVanillaChunkPool(int jobCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
