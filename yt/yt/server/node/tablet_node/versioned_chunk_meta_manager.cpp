#include "versioned_chunk_meta_manager.h"
#include "private.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/core/misc/async_slru_cache.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

using TVersionedChunkMetaCacheKey = std::pair<TChunkId, TTableSchema>;
DECLARE_REFCOUNTED_CLASS(TVersionedChunkMetaCacheEntry)

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkMetaCacheEntry
    : public TAsyncCacheValueBase<TVersionedChunkMetaCacheKey, TVersionedChunkMetaCacheEntry>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TCachedVersionedChunkMetaPtr, Meta);

public:
    TVersionedChunkMetaCacheEntry(
        const TVersionedChunkMetaCacheKey& key,
        TCachedVersionedChunkMetaPtr meta)
        : TAsyncCacheValueBase(key)
        , Meta_(std::move(meta))
    { }
};

DEFINE_REFCOUNTED_TYPE(TVersionedChunkMetaCacheEntry)

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkMetaManager::TImpl
    : public TAsyncSlruCacheBase<TVersionedChunkMetaCacheKey, TVersionedChunkMetaCacheEntry>
{
public:
    TImpl(
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            config->VersionedChunkMetaCache,
            TabletNodeProfiler.WithPrefix("/versioned_chunk_meta_cache"))
        , Bootstrap_(bootstrap)
    { }

    TFuture<TCachedVersionedChunkMetaPtr> GetMeta(
        IChunkReaderPtr chunkReader,
        // TODO(babenko): refcounted schema
        const TTableSchema& schema,
        const TClientBlockReadOptions& blockReadOptions)
    {
        auto chunkId = chunkReader->GetChunkId();
        auto key = std::make_pair(chunkId, schema);
        auto cookie = BeginInsert(key);
        if (cookie.IsActive()) {
            // TODO(savrus,psushin) Move call to dispatcher?
            auto asyncMeta = TCachedVersionedChunkMeta::Load(
                std::move(chunkReader),
                blockReadOptions,
                New<TTableSchema>(schema),
                {} /* columnRenameDesctiptors */,
                Bootstrap_
                    ->GetMemoryUsageTracker()
                    ->WithCategory(EMemoryCategory::VersionedChunkMeta));

            asyncMeta.Subscribe(BIND([cookie = std::move(cookie), key] (const TErrorOr<TCachedVersionedChunkMetaPtr>& metaOrError) mutable {
                if (metaOrError.IsOK()) {
                    cookie.EndInsert(New<TVersionedChunkMetaCacheEntry>(key, metaOrError.Value()));
                } else {
                    cookie.Cancel(metaOrError);
                }
            }));

            return asyncMeta;
        } else {
            return cookie.GetValue().Apply(BIND([] (const TVersionedChunkMetaCacheEntryPtr& entry) {
                return entry->GetMeta();
            }));
        }
    }

private:
    TBootstrap* const Bootstrap_;

    virtual i64 GetWeight(const TVersionedChunkMetaCacheEntryPtr& entry) const override
    {
        return entry->GetMeta()->GetMemoryUsage();
    }
};

////////////////////////////////////////////////////////////////////////////////

TVersionedChunkMetaManager::TVersionedChunkMetaManager(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TVersionedChunkMetaManager::~TVersionedChunkMetaManager() = default;

TFuture<TCachedVersionedChunkMetaPtr> TVersionedChunkMetaManager::GetMeta(
    IChunkReaderPtr chunkReader,
    const TTableSchema& schema,
    const TClientBlockReadOptions& blockReadOptions)
{
    return Impl_->GetMeta(std::move(chunkReader), schema, blockReadOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
