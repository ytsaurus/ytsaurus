#pragma once

#include <yt/yt/ytlib/chunk_pools/public.h>

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcherFactory)

DECLARE_REFCOUNTED_CLASS(TJobSizeAdjusterConfig)
DECLARE_REFCOUNTED_CLASS(TInputChunkMapping)

DECLARE_REFCOUNTED_STRUCT(IPersistentChunkPoolInput)
DECLARE_REFCOUNTED_STRUCT(IPersistentChunkPoolOutput)
DECLARE_REFCOUNTED_STRUCT(IPersistentChunkPool)
DECLARE_REFCOUNTED_STRUCT(IShuffleChunkPool)
DECLARE_REFCOUNTED_STRUCT(ISortedChunkPool)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkPoolInput)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkPoolOutput)
DECLARE_REFCOUNTED_STRUCT(IMultiChunkPool)
DECLARE_REFCOUNTED_STRUCT(IPersistentChunkPoolJobSplittingHost)

YT_DEFINE_ERROR_ENUM(
    ((DataSliceLimitExceeded)             (2000))
    ((MaxDataWeightPerJobExceeded)        (2001))
    ((MaxPrimaryDataWeightPerJobExceeded) (2002))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
