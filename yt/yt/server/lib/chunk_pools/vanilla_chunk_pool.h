#pragma once

#include "chunk_pool.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TVanillaChunkPoolOptions
{
    int JobCount = 0;
    bool RestartCompletedJobs = false;
    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

struct IVanillaChunkPoolOutput
    : public virtual IPersistentChunkPoolOutput
{
    virtual void LostAll() = 0;
    using IPersistentChunkPoolOutput::Extract;
    virtual void Extract(IChunkPoolOutput::TCookie cookie) = 0;

    // COMPAT(pogorelov)
    virtual void SetJobCount(int jobCount) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVanillaChunkPoolOutput);

////////////////////////////////////////////////////////////////////////////////

// NB: Vanilla chunk pools implements only IPersistentChunkPoolOutput.
IVanillaChunkPoolOutputPtr CreateVanillaChunkPool(const TVanillaChunkPoolOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
