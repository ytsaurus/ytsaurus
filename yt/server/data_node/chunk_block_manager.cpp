#include "chunk_block_manager.h"
#include "private.h"
#include "blob_reader_cache.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "config.h"
#include "location.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/file_reader.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/thread_pool.h>

namespace NYT::NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NCellNode;
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

class TChunkBlockManager::TImpl
    : public TAsyncSlruCacheBase<TBlockId, TCachedBlock>
{
public:
    TImpl(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            config->BlockCache->CompressedData,
            DataNodeProfiler.AppendPath("/block_cache/" + FormatEnum(EBlockType::CompressedData)))
        , Config_(config)
        , Bootstrap_(bootstrap)
        , ReaderThreadPool_(New<TThreadPool>(Config_->ReadThreadCount, "ReaderThread"))
        , ReaderInvoker_(CreatePrioritizedInvoker(ReaderThreadPool_->GetInvoker()))
    { }

    TCachedBlockPtr FindCachedBlock(const TBlockId& blockId)
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

    void PutCachedBlock(
        const TBlockId& blockId,
        const TBlock& data,
        const std::optional<TNodeDescriptor>& source)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cookie = BeginInsert(blockId);
        if (!cookie.IsActive()) {
            return;
        }

        auto block = New<TCachedBlock>(blockId, data, source);
        cookie.EndInsert(block);
    }

    TCachedBlockCookie BeginInsertCachedBlock(const TBlockId& blockId)
    {
        return BeginInsert(blockId);
    }

    TFuture<std::vector<TBlock>> ReadBlockRange(
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
        const TBlockReadOptions& options)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        try {
            auto chunkRegistry = Bootstrap_->GetChunkRegistry();
            // NB: At the moment, range read requests are only possible for the whole chunks.
            auto chunk = chunkRegistry->GetChunkOrThrow(chunkId);

            // Hold the read guard.
            auto readGuard = TChunkReadGuard::AcquireOrThrow(chunk);
            auto asyncBlocks = chunk->ReadBlockRange(
                firstBlockIndex,
                blockCount,
                options);
            // Release the read guard upon future completion.
            return asyncBlocks.Apply(BIND(&TImpl::OnBlocksRead, Passed(std::move(readGuard))));
        } catch (const std::exception& ex) {
            return MakeFuture<std::vector<TBlock>>(TError(ex));
        }
    }

    TFuture<std::vector<TBlock>> ReadBlockSet(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndexes,
        const TBlockReadOptions& options)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        try {
            auto chunkRegistry = Bootstrap_->GetChunkRegistry();
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
                        blocks.push_back(block);
                    }
                }
                return MakeFuture(blocks);
            }

            auto readGuard = TChunkReadGuard::AcquireOrThrow(chunk);
            auto asyncBlocks = chunk->ReadBlockSet(blockIndexes, options);

            // Hold the read guard.
            auto asyncResult = asyncBlocks
                .Apply(BIND(&TImpl::OnBlocksRead, Passed(std::move(readGuard))));

            return asyncResult;
        } catch (const std::exception& ex) {
            return MakeFuture<std::vector<TBlock>>(TError(ex));
        }
    }

    IPrioritizedInvokerPtr GetReaderInvoker() const
    {
        return ReaderInvoker_;
    }

private:
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    TThreadPoolPtr ReaderThreadPool_;
    IPrioritizedInvokerPtr ReaderInvoker_;

    virtual i64 GetWeight(const TCachedBlockPtr& block) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return block->GetData().Size();
    }

    static std::vector<TBlock> OnBlocksRead(
        TChunkReadGuard /*guard*/,
        const std::vector<TBlock>& blocks)
    {
        return blocks;
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkBlockManager::TChunkBlockManager(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TChunkBlockManager::~TChunkBlockManager()
{ }

TCachedBlockPtr TChunkBlockManager::FindCachedBlock(const TBlockId& blockId)
{
    return Impl_->FindCachedBlock(blockId);
}

void TChunkBlockManager::PutCachedBlock(
    const TBlockId& blockId,
    const TBlock& data,
    const std::optional<TNodeDescriptor>& source)
{
    Impl_->PutCachedBlock(blockId, data, source);
}

TCachedBlockCookie TChunkBlockManager::BeginInsertCachedBlock(const TBlockId& blockId)
{
    return Impl_->BeginInsertCachedBlock(blockId);
}

TFuture<std::vector<TBlock>> TChunkBlockManager::ReadBlockRange(
    const TChunkId& chunkId,
    int firstBlockIndex,
    int blockCount,
    const TBlockReadOptions& options)
{
    return Impl_->ReadBlockRange(
        chunkId,
        firstBlockIndex,
        blockCount,
        options);
}

TFuture<std::vector<TBlock>> TChunkBlockManager::ReadBlockSet(
    const TChunkId& chunkId,
    const std::vector<int>& blockIndexes,
    const TBlockReadOptions& options)
{
    return Impl_->ReadBlockSet(
        chunkId,
        blockIndexes,
        options);
}

std::vector<TCachedBlockPtr> TChunkBlockManager::GetAllBlocks() const
{
    return Impl_->GetAll();
}

IPrioritizedInvokerPtr TChunkBlockManager::GetReaderInvoker() const
{
    return Impl_->GetReaderInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
