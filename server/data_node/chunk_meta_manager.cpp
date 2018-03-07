#include "chunk_meta_manager.h"
#include "config.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedChunkMeta::TCachedChunkMeta(
    const TChunkId& chunkId,
    TRefCountedChunkMetaPtr meta,
    TNodeMemoryTracker* memoryTracker)
    : TAsyncCacheValueBase(chunkId)
    , Meta_(std::move(meta))
    , MemoryTrackerGuard_(std::make_unique<TNodeMemoryTrackerGuard>(TNodeMemoryTrackerGuard::Acquire(
        memoryTracker,
        EMemoryCategory::ChunkMeta,
        Meta_->SpaceUsed())))
{ }

i64 TCachedChunkMeta::GetSize() const
{
    return MemoryTrackerGuard_->GetSize();
}

////////////////////////////////////////////////////////////////////////////////

class TChunkMetaManager::TImpl
    : public TAsyncSlruCacheBase<TChunkId, TCachedChunkMeta>
{
public:
    TImpl(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            config->ChunkMetaCache,
            NProfiling::TProfiler(
                DataNodeProfiler.GetPathPrefix() +
                "/chunk_meta_cache"))
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        YCHECK(Config_);
        YCHECK(Bootstrap_);
    }

    void PutCachedMeta(
        const TChunkId& chunkId,
        TRefCountedChunkMetaPtr meta)
    {
        auto cookie = BeginInsert(chunkId);
        if (cookie.IsActive()) {
            auto cachedMeta = New<TCachedChunkMeta>(
                chunkId,
                std::move(meta),
                Bootstrap_->GetMemoryUsageTracker());
            cookie.EndInsert(cachedMeta);

            LOG_DEBUG("Chunk meta is put into cache (ChunkId: %v)",
                chunkId);
        } else {
            LOG_DEBUG("Failed to cache chunk meta due to concurrent read (ChunkId: %v)",
                chunkId);
        }
    }

    TCachedChunkMetaCookie BeginInsertCachedMeta(const TChunkId& chunkId)
    {
        return BeginInsert(chunkId);
    }

private:
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;


    virtual i64 GetWeight(const TCachedChunkMetaPtr& meta) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return meta->GetSize();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkMetaManager::TChunkMetaManager(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TChunkMetaManager::~TChunkMetaManager()
{ }

void TChunkMetaManager::PutCachedMeta(
    const TChunkId& chunkId,
    TRefCountedChunkMetaPtr meta)
{
    Impl_->PutCachedMeta(chunkId, std::move(meta));
}

TCachedChunkMetaCookie TChunkMetaManager::BeginInsertCachedMeta(const TChunkId& chunkId)
{
    return Impl_->BeginInsertCachedMeta(chunkId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
