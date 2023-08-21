#include "versioned_chunk_meta_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <util/digest/sequence.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

bool TVersionedChunkMetaCacheKey::operator ==(const TVersionedChunkMetaCacheKey& other) const
{
    return
        ChunkId == other.ChunkId &&
        TableSchemaKeyColumnCount == other.TableSchemaKeyColumnCount &&
        PreparedColumnarMeta == other.PreparedColumnarMeta;
}

TVersionedChunkMetaCacheKey::operator size_t() const
{
    return MultiHash(
        ChunkId,
        TableSchemaKeyColumnCount,
        PreparedColumnarMeta);
}

////////////////////////////////////////////////////////////////////////////////

TVersionedChunkMetaCacheEntry::TVersionedChunkMetaCacheEntry(
    const TVersionedChunkMetaCacheKey& key,
    TCachedVersionedChunkMetaPtr meta)
    : TAsyncCacheValueBase(key)
    , Meta_(std::move(meta))
{ }

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkMetaManager
    : public TAsyncSlruCacheBase<TVersionedChunkMetaCacheKey, TVersionedChunkMetaCacheEntry>
    , public IVersionedChunkMetaManager
{
public:
    TVersionedChunkMetaManager(
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : TAsyncSlruCacheBase(
            std::move(config),
            TabletNodeProfiler.WithPrefix("/versioned_chunk_meta_cache"))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        // TODO(akozhikhov): Employ memory tracking cache.
        MemoryUsageTracker_->SetLimit(GetCapacity());
    }

    ~TVersionedChunkMetaManager()
    {
        MemoryUsageTracker_->SetLimit(0);
    }

    TFuture<TVersionedChunkMetaCacheEntryPtr> GetMeta(
        const IChunkReaderPtr& chunkReader,
        const TTableSchemaPtr& schema,
        const TClientChunkReadOptions& chunkReadOptions,
        bool prepareColumnarMeta) override
    {
        TVersionedChunkMetaCacheKey key{
            chunkReader->GetChunkId(),
            schema->GetKeyColumnCount(),
            prepareColumnarMeta
        };

        auto cookie = BeginInsert(key);
        if (cookie.IsActive()) {
            // TODO(savrus,psushin) Move call to dispatcher?
            return chunkReader->GetMeta(chunkReadOptions)
                .Apply(BIND(
                    &TCachedVersionedChunkMeta::Create,
                    prepareColumnarMeta,
                    MemoryUsageTracker_))
                .ApplyUnique(BIND(
                    [cookie = std::move(cookie), key]
                    (TErrorOr<TCachedVersionedChunkMetaPtr>&& metaOrError) mutable
                {
                    if (!metaOrError.IsOK()) {
                        cookie.Cancel(metaOrError);
                        THROW_ERROR(metaOrError);
                    }

                    auto meta = std::move(metaOrError.Value());
                    auto result = New<TVersionedChunkMetaCacheEntry>(key, std::move(meta));
                    cookie.EndInsert(result);
                    return result;
                }));
        } else {
            return cookie.GetValue();
        }
    }

    void Touch(const TVersionedChunkMetaCacheEntryPtr& entry) override
    {
        TAsyncSlruCacheBase::Touch(entry);
    }

    void Reconfigure(const TSlruCacheDynamicConfigPtr& config) override
    {
        TAsyncSlruCacheBase::Reconfigure(config);
        MemoryUsageTracker_->SetLimit(GetCapacity());
    }

private:
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    i64 GetWeight(const TVersionedChunkMetaCacheEntryPtr& entry) const override
    {
        return entry->Meta()->GetMemoryUsage();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkMetaManagerPtr CreateVersionedChunkMetaManager(
    TSlruCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    return New<TVersionedChunkMetaManager>(
        std::move(config),
        std::move(memoryUsageTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
