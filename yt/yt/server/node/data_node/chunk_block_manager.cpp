#include "chunk_block_manager.h"

#include "private.h"
#include "blob_reader_cache.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "config.h"
#include "location.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/file_reader.h>

#include <yt/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

namespace NYT::NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(
    const TBlockId& blockId,
    const TBlock& data,
    const std::optional<TNodeDescriptor>& source)
    : TAsyncCacheValueBase<TBlockId, TCachedBlock>(blockId)
    , Data_(data)
    , Source_(source)
{ }

////////////////////////////////////////////////////////////////////////////////

class TChunkBlockManager
    : public IChunkBlockManager
    , public TAsyncSlruCacheBase<TBlockId, TCachedBlock>
{
public:
    explicit TChunkBlockManager(TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            bootstrap->GetConfig()->DataNode->BlockCache->CompressedData,
            DataNodeProfiler.WithPrefix("/block_cache/" + FormatEnum(EBlockType::CompressedData)))
        , Bootstrap_(bootstrap)
    { }

    virtual TCachedBlockPtr FindCachedBlock(const TBlockId& blockId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cachedBlock = TAsyncSlruCacheBase::Find(blockId);

        if (cachedBlock) {
            YT_LOG_TRACE("Block cache hit (BlockId: %v)", blockId);
        } else {
            YT_LOG_TRACE("Block cache miss (BlockId: %v)", blockId);
        }

        return cachedBlock;
    }

    virtual void PutCachedBlock(
        const TBlockId& blockId,
        const TBlock& data,
        const std::optional<TNodeDescriptor>& source) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cookie = BeginInsert(blockId);
        if (!cookie.IsActive()) {
            return;
        }

        auto cachedBlock = New<TCachedBlock>(blockId, data, source);
        cookie.EndInsert(cachedBlock);

        if (source) {
            auto guard = Guard(BlocksWithSourceLock_);
            BlocksWithSource_.push_back(std::move(cachedBlock));
        }
    }

    virtual TCachedBlockCookie BeginInsertCachedBlock(const TBlockId& blockId) override
    {
        return BeginInsert(blockId);
    }

    virtual TFuture<std::vector<TBlock>> ReadBlockRange(
        TChunkId chunkId,
        int firstBlockIndex,
        int blockCount,
        const TBlockReadOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        try {
            const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
            // NB: At the moment, range read requests are only possible for the whole chunks.
            auto chunk = chunkRegistry->GetChunkOrThrow(chunkId);
            return chunk->ReadBlockRange(
                firstBlockIndex,
                blockCount,
                options);
        } catch (const std::exception& ex) {
            return MakeFuture<std::vector<TBlock>>(TError(ex));
        }
    }

    virtual TFuture<std::vector<TBlock>> ReadBlockSet(
        TChunkId chunkId,
        const std::vector<int>& blockIndexes,
        const TBlockReadOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        try {
            const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
            auto chunk = chunkRegistry->FindChunk(chunkId);
            auto type = TypeFromId(DecodeChunkId(chunkId).Id);
            if (!chunk) {
                std::vector<TBlock> blocks;
                // During block peering, data nodes exchange individual blocks.
                // Thus the cache may contain a block not bound to any chunk in the registry.
                // We must look for these blocks.
                if (options.BlockCache &&
                    options.FetchFromCache &&
                    (type == EObjectType::Chunk || type == EObjectType::ErasureChunk))
                {
                    for (int blockIndex : blockIndexes) {
                        auto blockId = TBlockId(chunkId, blockIndex);
                        auto block = options.BlockCache->Find(blockId, EBlockType::CompressedData);
                        options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
                        blocks.push_back(block);
                    }
                }
                return MakeFuture(blocks);
            }

            return chunk->ReadBlockSet(blockIndexes, options);
        } catch (const std::exception& ex) {
            return MakeFuture<std::vector<TBlock>>(TError(ex));
        }
    }

    virtual std::vector<TCachedBlockPtr> GetAllBlocksWithSource() override
    {
        std::vector<TCachedBlockPtr> result;
        auto guard = Guard(BlocksWithSourceLock_);
        result.reserve(BlocksWithSource_.size());
        auto sourceIt = BlocksWithSource_.begin();
        auto destIt = sourceIt;
        while (sourceIt != BlocksWithSource_.end()) {
            auto block = sourceIt->Lock();
            if (block) {
                result.push_back(std::move(block));
                *destIt++ = std::move(*sourceIt++);
            } else {
                ++sourceIt;
            }
        }
        BlocksWithSource_.erase(sourceIt, BlocksWithSource_.end());
        return result;
    }

private:
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, BlocksWithSourceLock_);
    std::vector<TWeakPtr<TCachedBlock>> BlocksWithSource_;


    virtual i64 GetWeight(const TCachedBlockPtr& block) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return block->GetData().Size();
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkBlockManagerPtr CreateChunkBlockManager(TBootstrap* bootstrap)
{
    return New<TChunkBlockManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
