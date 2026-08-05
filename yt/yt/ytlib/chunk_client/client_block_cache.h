#pragma once

#include "block_cache.h"

#include <yt/yt/ytlib/misc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IClientBlockCache
    : public IBlockCache
{
    virtual void Reconfigure(const TBlockCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientBlockCache)

////////////////////////////////////////////////////////////////////////////////

//! Creates reconfigurable client-side block cache.
/*!
 *  If #manageMemoryLimit is |true| (default), the cache overrides the limit of
 *  the provided memory usage tracker with its total capacity.
 */
IClientBlockCachePtr CreateClientBlockCache(
    TBlockCacheConfigPtr config,
    EBlockType supportedBlockTypes,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    const NProfiling::TProfiler& profiler = {},
    bool manageMemoryLimit = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
