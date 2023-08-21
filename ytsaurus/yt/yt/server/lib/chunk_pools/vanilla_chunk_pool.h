#pragma once

#include "chunk_pool.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TVanillaChunkPoolOptions
{
    int JobCount;
    bool RestartCompletedJobs = false;
    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

// NB: Vanilla chunk pool implements only IPersistentChunkPoolOutput.
IPersistentChunkPoolOutputPtr CreateVanillaChunkPool(const TVanillaChunkPoolOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
