#pragma once

#include "public.h"

// TODO(dakovalkov): move to .cpp?
#include <yt/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TCachedChunkMeta
    : public TAsyncCacheValueBase<TChunkId, TCachedChunkMeta>
{
public:
    TCachedChunkMeta(
        TChunkId chunkId,
        TRefCountedChunkMetaPtr chunkMeta);

    using TMetaFetchCallback = TCallback<TFuture<TRefCountedChunkMetaPtr>(TChunkId chunkId, const std::optional<std::vector<int>>& extensionTags)>;

    TFuture<TRefCountedChunkMetaPtr> Fetch(
        std::optional<std::vector<int>> extensionTags,
        const TMetaFetchCallback& metaFetchCallback);

    i64 GetWeight() const;

private:
    mutable YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, Lock_);

    TRefCountedChunkMetaPtr MainMeta_;

    using TExtensionState = TFuture<TString>;
    THashMap<int, TExtensionState> Extensions_;

    TRefCountedChunkMetaPtr AssembleChunkMeta(const std::optional<std::vector<int>>& extensionTags) const;

    void OnExtensionsReceived(
        const std::optional<std::vector<int>>& extensionTags,
        const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TCachedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

class TClientChunkMetaCache
    : public TAsyncSlruCacheBase<TChunkId, TCachedChunkMeta>
{
public:
    using TMetaFetchCallback = TCachedChunkMeta::TMetaFetchCallback;

    TClientChunkMetaCache(
        TSlruCacheConfigPtr config,
        IInvokerPtr invoker);

    TFuture<TRefCountedChunkMetaPtr> Fetch(
        TChunkId chunkId,
        const std::optional<std::vector<int>>& extensionTags,
        const TMetaFetchCallback& metaFetchCallback);

protected:
    virtual i64 GetWeight(const TCachedChunkMetaPtr& value) const override;

private:
    IInvokerPtr Invoker_;

    TRefCountedChunkMetaPtr DoFetch(
        TChunkId chunkId,
        const std::optional<std::vector<int>>& extensionTags,
        const TMetaFetchCallback& metaFetchCallback);
};

DEFINE_REFCOUNTED_TYPE(TClientChunkMetaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
