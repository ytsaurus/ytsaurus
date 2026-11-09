#pragma once

#include "public.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TMultiChunkPoolOptions
{
    TJobSizeAdjusterConfigPtr JobSizeAdjusterConfig;
    i64 InitialDataWeightPerJob = 0;
};

////////////////////////////////////////////////////////////////////////////////

IMultiChunkPoolInputPtr CreateMultiChunkPoolInput(
    std::vector<IPersistentChunkPoolInputPtr> underlyingPools);

IMultiChunkPoolOutputPtr CreateMultiChunkPoolOutput(
    std::vector<IPersistentChunkPoolOutputPtr> underlyingPools,
    const TMultiChunkPoolOptions& options = {});

IMultiChunkPoolPtr CreateMultiChunkPool(
    std::vector<IPersistentChunkPoolPtr> underlyingPools,
    const TMultiChunkPoolOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
