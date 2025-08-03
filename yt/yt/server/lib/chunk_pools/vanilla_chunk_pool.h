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

    //! Get actual job count as seen by the job manager.
    virtual int GetJobCount() const = 0;
    //! Either adds pending jobs or invalidates some jobs
    //! and returns their cookies to identify jobs to be aborted.
    //! TODO(coteeq): Rename to SetJobCount after @pogorelov's compat is obsolete.
    virtual std::vector<TOutputCookie> UpdateJobCount(int desiredCount) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVanillaChunkPoolOutput);

////////////////////////////////////////////////////////////////////////////////

// NB: Vanilla chunk pools implements only IPersistentChunkPoolOutput.
IVanillaChunkPoolOutputPtr CreateVanillaChunkPool(const TVanillaChunkPoolOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
