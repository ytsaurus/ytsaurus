#include "chunk_meta_manager.h"
#include "config.h"
#include "private.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedChunkMeta::TCachedChunkMeta(
    TChunkId chunkId,
    TRefCountedChunkMetaPtr meta,
    TNodeMemoryTrackerPtr memoryTracker)
    : TAsyncCacheValueBase(chunkId)
    , Meta_(std::move(meta))
    , MemoryTrackerGuard_(std::make_unique<TNodeMemoryTrackerGuard>(TNodeMemoryTrackerGuard::Acquire(
        std::move(memoryTracker),
        EMemoryCategory::ChunkMeta,
        Meta_->SpaceUsed())))
{ }

i64 TCachedChunkMeta::GetSize() const
{
    return MemoryTrackerGuard_->GetSize();
}

////////////////////////////////////////////////////////////////////////////////

TCachedBlocksExt::TCachedBlocksExt(
    TChunkId chunkId,
    TRefCountedBlocksExtPtr blocksExt,
    TNodeMemoryTrackerPtr memoryTracker)
    : TAsyncCacheValueBase(chunkId)
    , BlocksExt_(std::move(blocksExt))
    , MemoryTrackerGuard_(std::make_unique<TNodeMemoryTrackerGuard>(TNodeMemoryTrackerGuard::Acquire(
        std::move(memoryTracker),
        EMemoryCategory::ChunkBlockMeta,
        BlocksExt_->SpaceUsed())))
{ }

i64 TCachedBlocksExt::GetSize() const
{
    return MemoryTrackerGuard_->GetSize();
}

////////////////////////////////////////////////////////////////////////////////

class TChunkMetaManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , MetaCache_(New<TChunkMetaCache>(config))
        , BlocksExtCache_(New<TBlocksExtCache>(config))
    {
        YT_VERIFY(Bootstrap_);
    }

    
    TRefCountedChunkMetaPtr FindCachedMeta(TChunkId chunkId)
    {
        auto cachedMeta = MetaCache_->Find(chunkId);
        return cachedMeta ? cachedMeta->GetMeta() : nullptr;
    }

    void PutCachedMeta(
        TChunkId chunkId,
        TRefCountedChunkMetaPtr meta)
    {
        auto cookie = BeginInsertCachedMeta(chunkId);
        if (cookie.IsActive()) {
            EndInsertCachedMeta(std::move(cookie), std::move(meta));
        } else {
            YT_LOG_DEBUG("Failed to cache chunk meta due to concurrent read (ChunkId: %v)",
                chunkId);
        }
    }

    TCachedChunkMetaCookie BeginInsertCachedMeta(TChunkId chunkId)
    {
        return MetaCache_->BeginInsert(chunkId);
    }

    void EndInsertCachedMeta(
        TCachedChunkMetaCookie&& cookie,
        TRefCountedChunkMetaPtr meta)
    {
        auto chunkId = cookie.GetKey();
        auto cachedMeta = New<TCachedChunkMeta>(
            chunkId,
            std::move(meta),
            Bootstrap_->GetMemoryUsageTracker());
        cookie.EndInsert(cachedMeta);

        YT_LOG_DEBUG("Chunk meta is put into cache (ChunkId: %v)",
            chunkId);
    }

    void RemoveCachedMeta(TChunkId chunkId)
    {
         MetaCache_->TryRemove(chunkId);
    }


    TRefCountedBlocksExtPtr FindCachedBlocksExt(TChunkId chunkId)
    {
        auto cachedBlocksExt = BlocksExtCache_->Find(chunkId);
        return cachedBlocksExt ? cachedBlocksExt->GetBlocksExt() : nullptr;
    }

    void PutCachedBlocksExt(TChunkId chunkId, TRefCountedBlocksExtPtr blocksExt)
    {
        auto cookie = BeginInsertCachedBlocksExt(chunkId);
        if (cookie.IsActive()) {
            EndInsertCachedBlocksExt(std::move(cookie), std::move(blocksExt));
        } else {
            YT_LOG_DEBUG("Failed to cache blocks ext due to concurrent read (ChunkId: %v)",
                chunkId);
        }
    }

    TCachedBlocksExtCookie BeginInsertCachedBlocksExt(TChunkId chunkId)
    {
        return BlocksExtCache_->BeginInsert(chunkId);
    }

    void EndInsertCachedBlocksExt(
        TCachedBlocksExtCookie&& cookie,
        TRefCountedBlocksExtPtr blocksExt)
    {
        auto chunkId = cookie.GetKey();
        auto cachedBlocksExt = New<TCachedBlocksExt>(
            chunkId,
            std::move(blocksExt),
            Bootstrap_->GetMemoryUsageTracker());
        cookie.EndInsert(cachedBlocksExt);

        YT_LOG_DEBUG("Blocks ext is put into cache (ChunkId: %v)",
            chunkId);
    }

    void RemoveCachedBlocksExt(TChunkId chunkId)
    {
         BlocksExtCache_->TryRemove(chunkId);
    }

private:
    TBootstrap* const Bootstrap_;

    class TChunkMetaCache
        : public TAsyncSlruCacheBase<TChunkId, TCachedChunkMeta>
    {
    public:
        explicit TChunkMetaCache(TDataNodeConfigPtr config)
            : TAsyncSlruCacheBase(
                config->ChunkMetaCache,
                DataNodeProfiler.AppendPath("/chunk_meta_cache"))
        { }

    protected:
        virtual i64 GetWeight(const TCachedChunkMetaPtr& meta) const override
        {
            VERIFY_THREAD_AFFINITY_ANY();

            return meta->GetSize();
        }
    };

    const TIntrusivePtr<TChunkMetaCache> MetaCache_;

    class TBlocksExtCache
        : public TAsyncSlruCacheBase<TChunkId, TCachedBlocksExt>
    {
    public:
        explicit TBlocksExtCache(TDataNodeConfigPtr config)
            : TAsyncSlruCacheBase(
                config->BlocksExtCache,
                DataNodeProfiler.AppendPath("/blocks_ext_cache"))
        { }

    protected:
        virtual i64 GetWeight(const TCachedBlocksExtPtr& blocksExt) const override
        {
            VERIFY_THREAD_AFFINITY_ANY();

            return blocksExt->GetSize();
        }
    };

    const TIntrusivePtr<TBlocksExtCache> BlocksExtCache_;
};

////////////////////////////////////////////////////////////////////////////////

TChunkMetaManager::TChunkMetaManager(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TChunkMetaManager::~TChunkMetaManager() = default;

TRefCountedChunkMetaPtr TChunkMetaManager::FindCachedMeta(TChunkId chunkId)
{
    return Impl_->FindCachedMeta(chunkId);
}

void TChunkMetaManager::PutCachedMeta(
    TChunkId chunkId,
    TRefCountedChunkMetaPtr meta)
{
    Impl_->PutCachedMeta(chunkId, std::move(meta));
}

TCachedChunkMetaCookie TChunkMetaManager::BeginInsertCachedMeta(TChunkId chunkId)
{
    return Impl_->BeginInsertCachedMeta(chunkId);
}

void TChunkMetaManager::EndInsertCachedMeta(
    TCachedChunkMetaCookie&& cookie,
    TRefCountedChunkMetaPtr meta)
{
    Impl_->EndInsertCachedMeta(
        std::move(cookie),
        std::move(meta));
}

void TChunkMetaManager::RemoveCachedMeta(TChunkId chunkId)
{
    Impl_->RemoveCachedMeta(chunkId);
}

TRefCountedBlocksExtPtr TChunkMetaManager::FindCachedBlocksExt(TChunkId chunkId)
{
    return Impl_->FindCachedBlocksExt(chunkId);
}

void TChunkMetaManager::PutCachedBlocksExt(TChunkId chunkId, TRefCountedBlocksExtPtr blocksExt)
{
    Impl_->PutCachedBlocksExt(chunkId, std::move(blocksExt));
}

TCachedBlocksExtCookie TChunkMetaManager::BeginInsertCachedBlocksExt(TChunkId chunkId)
{
    return Impl_->BeginInsertCachedBlocksExt(chunkId);
}

void TChunkMetaManager::EndInsertCachedBlocksExt(
    TCachedBlocksExtCookie&& cookie,
    TRefCountedBlocksExtPtr blocksExt)
{
    Impl_->EndInsertCachedBlocksExt(
        std::move(cookie),
        std::move(blocksExt));
}

void TChunkMetaManager::RemoveCachedBlocksExt(TChunkId chunkId)
{
    Impl_->RemoveCachedBlocksExt(chunkId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
