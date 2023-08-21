#pragma once

#include "new_sorted_job_builder.h"

#include "sorted_chunk_pool.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

ISortedChunkPoolPtr CreateNewSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
