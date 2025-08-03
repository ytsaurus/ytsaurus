#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_pools/chunk_pool.h>

#include <yt/yt/client/table_client/public.h>

#include <limits>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

IChunkPoolPtr CreateChunkPool(
    NTableClient::ETablePartitionMode partitionMode,
    i64 dataWeightPerPartition,
    std::optional<int> maxPartitionCount,
    bool useNewSlicingImplementationInOrderedPool,
    bool useNewSlicingImplementationInUnorderedPool,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

