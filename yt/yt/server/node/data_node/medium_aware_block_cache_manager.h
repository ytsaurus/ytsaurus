#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages per-medium block caches.
struct IMediumAwareBlockCacheManager
    : public virtual TRefCounted
{
    //! Returns the block cache configured for the given medium or null.
    virtual NChunkClient::IBlockCachePtr GetBlockCacheForMedium(int mediumIndex) const = 0;

    //! Applies dynamic manager configuration.
    virtual void Reconfigure(const TMediumAwareBlockCacheManagerDynamicConfigPtr& config) = 0;

    //! Applies a full location-count snapshot.
    virtual void UpdateLocationCountPerMedium(const TLocationCountPerMedium& locationCountPerMedium) = 0;

    //! Removes blocks by chunk id from all managed caches.
    virtual void RemoveChunkBlocks(NChunkClient::TChunkId chunkId) = 0;

    //! Aggregates cached blocks by chunk id across all managed caches.
    virtual THashSet<NChunkClient::TBlockInfo> GetCachedBlocksByChunkId(
        NChunkClient::TChunkId chunkId,
        NChunkClient::EBlockType type) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMediumAwareBlockCacheManager)

////////////////////////////////////////////////////////////////////////////////

using TMediumNameResolver = TCallback<std::optional<std::string>(int)>;

////////////////////////////////////////////////////////////////////////////////

IMediumAwareBlockCacheManagerPtr CreateMediumAwareBlockCacheManager(
    TMediumAwareBlockCacheManagerConfigPtr config,
    TLocationCountPerMedium locationCountPerMedium,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    TMediumNameResolver mediumNameResolver,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
