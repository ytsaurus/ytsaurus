#pragma once

#include "public.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// This interface is not exposed through IClientChunkMetaCache.
// It is used only for unittests.
class ICachedChunkMeta
    : public virtual TRefCounted
{
public:
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

class IClientChunkMetaCache
    : public virtual TRefCounted
{
public:
    using TMetaFetchCallback = ICachedChunkMeta::TMetaFetchCallback;

    virtual TFuture<TRefCountedChunkMetaPtr> Fetch(
        TChunkId chunkId,
        const std::optional<std::vector<int>>& extensionTags,
        const TMetaFetchCallback& metaFetchCallback) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientChunkMetaCache)

////////////////////////////////////////////////////////////////////////////////

IClientChunkMetaCachePtr CreateClientChunkMetaCache(
    TClientChunkMetaCacheConfigPtr config,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
