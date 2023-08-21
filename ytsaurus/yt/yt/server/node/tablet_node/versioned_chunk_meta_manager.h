#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TVersionedChunkMetaCacheKey
{
    NChunkClient::TChunkId ChunkId;
    int TableSchemaKeyColumnCount;
    bool PreparedColumnarMeta;

    bool operator ==(const TVersionedChunkMetaCacheKey& other) const;

    operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkMetaCacheEntry
    : public TAsyncCacheValueBase<TVersionedChunkMetaCacheKey, TVersionedChunkMetaCacheEntry>
{
public:
    DEFINE_BYREF_RO_PROPERTY(NTableClient::TCachedVersionedChunkMetaPtr, Meta);

public:
    TVersionedChunkMetaCacheEntry(
        const TVersionedChunkMetaCacheKey& key,
        NTableClient::TCachedVersionedChunkMetaPtr meta);
};

DEFINE_REFCOUNTED_TYPE(TVersionedChunkMetaCacheEntry)

////////////////////////////////////////////////////////////////////////////////

struct IVersionedChunkMetaManager
    : public virtual TRefCounted
{
    virtual TFuture<TVersionedChunkMetaCacheEntryPtr> GetMeta(
        const NChunkClient::IChunkReaderPtr& chunkReader,
        const NTableClient::TTableSchemaPtr& schema,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        bool prepareColumnarMeta = false) = 0;

    virtual void Touch(const TVersionedChunkMetaCacheEntryPtr& entry) = 0;

    virtual void Reconfigure(const TSlruCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedChunkMetaManager)

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkMetaManagerPtr CreateVersionedChunkMetaManager(
    TSlruCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
