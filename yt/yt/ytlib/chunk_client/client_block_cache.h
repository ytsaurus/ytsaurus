#pragma once

#include "block_cache.h"

#include <yt/yt/core/profiling/profiler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TBlockCacheEntry
{
    TBlockId BlockId;
    EBlockType BlockType;
    TCachedBlock Block;
};

////////////////////////////////////////////////////////////////////////////////

struct IClientBlockCache
    : public IBlockCache
{
    //! Returns all the cached blocks with given types.
    virtual std::vector<TBlockCacheEntry> GetCacheSnapshot(EBlockType blockTypes) const = 0;

    virtual void Reconfigure(const TBlockCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientBlockCache)

////////////////////////////////////////////////////////////////////////////////

//! Creates a simple client-side block cache.
IClientBlockCachePtr CreateClientBlockCache(
    TBlockCacheConfigPtr config,
    EBlockType supportedBlockTypes,
    IMemoryUsageTrackerPtr memoryTracker,
    const NProfiling::TProfiler& profiler = {});

//! Returns an always-empty block cache.
IBlockCachePtr GetNullBlockCache();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
