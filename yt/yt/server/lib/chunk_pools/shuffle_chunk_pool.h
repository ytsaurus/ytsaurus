#pragma once

#include "public.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

IShuffleChunkPoolPtr CreateShuffleChunkPool(
    int partitionCount,
    i64 maxDataWeightPerPartition,
    i64 maxChunkSlicePerPartition);

bool IsPartitionOversized(
    i64 partitionDataWeight,
    i64 partitionRowCount,
    i64 partitionSliceCount,
    i64 maxDataWeightPerPartition,
    i64 maxChunkSlicePerPartition) noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
