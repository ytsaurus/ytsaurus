#pragma once

#include "public.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// This interface is not exposed through IClientChunkMetaCache.
// It is used only for unittests.
struct  ICachedChunkMeta
    : public virtual TRefCounted
{
    using TMetaFetchCallback = TCallback<TFuture<TRefCountedChunkMetaPtr>(const std::optional<std::vector<int>>& extensionTags)>;

    virtual TFuture<TRefCountedChunkMetaPtr> Fetch(
        std::optional<std::vector<int>> extensionTags,
        const TMetaFetchCallback& metaFetchCallback) = 0;

    virtual i64 GetWeight() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICachedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

ICachedChunkMetaPtr CreateCachedChunkMeta(
    TChunkId chunkId,
    TRefCountedChunkMetaPtr chunkMeta);

////////////////////////////////////////////////////////////////////////////////

struct IClientChunkMetaCache
    : public virtual TRefCounted
{
    using TMetaFetchCallback = ICachedChunkMeta::TMetaFetchCallback;

    virtual TFuture<TRefCountedChunkMetaPtr> Fetch(
        TChunkId chunkId,
        const std::optional<std::vector<int>>& extensionTags,
        const TMetaFetchCallback& metaFetchCallback) = 0;

    virtual void Reconfigure(const TSlruCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientChunkMetaCache)

////////////////////////////////////////////////////////////////////////////////

IClientChunkMetaCachePtr CreateClientChunkMetaCache(
    TClientChunkMetaCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker(),
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
