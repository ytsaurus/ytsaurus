#include "versioned_chunk_meta_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

bool TVersionedChunkMetaCacheKey::operator ==(const TVersionedChunkMetaCacheKey& other) const
{
    return
        ChunkId == other.ChunkId &&
        TableSchemaKeyColumnCount == other.TableSchemaKeyColumnCount;
}

TVersionedChunkMetaCacheKey::operator size_t() const
{
    return MultiHash(
        ChunkId,
        TableSchemaKeyColumnCount);
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
        IBootstrapBase* bootstrap)
        : TAsyncSlruCacheBase(
            std::move(config),
            TabletNodeProfiler.WithPrefix("/versioned_chunk_meta_cache"))
        , Bootstrap_(bootstrap)
    { }

    TFuture<TVersionedChunkMetaCacheEntryPtr> GetMeta(
        const IChunkReaderPtr& chunkReader,
        const TTableSchemaPtr& schema,
        const TClientChunkReadOptions& chunkReadOptions) override
    {
        TVersionedChunkMetaCacheKey key{
            chunkReader->GetChunkId(),
            schema->GetKeyColumnCount()
        };

        auto cookie = BeginInsert(key);
        if (cookie.IsActive()) {
            // TODO(savrus,psushin) Move call to dispatcher?
            return TCachedVersionedChunkMeta::Load(
                std::move(chunkReader),
                chunkReadOptions,
                schema,
                {} /* columnRenameDescriptors */,
                Bootstrap_
                    ->GetMemoryUsageTracker()
                    ->WithCategory(EMemoryCategory::VersionedChunkMeta))
                .ApplyUnique(BIND(
                    [cookie = std::move(cookie), key]
                    (TErrorOr<TCachedVersionedChunkMetaPtr>&& metaOrError) mutable
                {
                    if (metaOrError.IsOK()) {
                        auto result = New<TVersionedChunkMetaCacheEntry>(
                            key,
                            std::move(metaOrError.Value()));
                        cookie.EndInsert(result);
                        return result;
                    }
                
                    cookie.Cancel(metaOrError);
                    metaOrError.ValueOrThrow();
                    YT_ABORT();
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
    }

private:
    IBootstrapBase* const Bootstrap_;

    i64 GetWeight(const TVersionedChunkMetaCacheEntryPtr& entry) const override
    {
        return entry->Meta()->GetMemoryUsage();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkMetaManagerPtr CreateVersionedChunkMetaManager(
    TSlruCacheConfigPtr config,
    IBootstrapBase* bootstrap)
{
    return New<TVersionedChunkMetaManager>(
        std::move(config),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
