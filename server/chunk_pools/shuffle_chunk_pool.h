#pragma once

#include "private.h"
#include "chunk_pool.h"

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IShuffleChunkPool> CreateShuffleChunkPool(
    int partitionCount,
    i64 dataSizeThreshold);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
