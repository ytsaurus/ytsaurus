#include "stdafx.h"
#include "block_store.h"
#include "private.h"
#include "chunk.h"
#include "config.h"
#include "chunk_registry.h"
#include "blob_reader_cache.h"
#include "location.h"

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <server/cell_node/bootstrap.h>

#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NCellNode;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TBlocksExt;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(
    const TBlockId& blockId,
    const TSharedRef& data,
    const TNullable<TNodeDescriptor>& source)
    : TAsyncCacheValueBase<TBlockId, TCachedBlock>(blockId)
    , Data_(data)
    , Source_(source)
{ }

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TImpl
    : public TAsyncSlruCacheBase<TBlockId, TCachedBlock>
{
public:
    TImpl(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            config->BlockCache->CompressedData,
            NProfiling::TProfiler(
                DataNodeProfiler.GetPathPrefix() +
                "/block_cache/" +
                FormatEnum(EBlockType::CompressedData)))
        , Config_(config)
        , Bootstrap_(bootstrap)
    { }

    void PutBlock(
        const TBlockId& blockId,
        const TSharedRef& data,
        const TNullable<TNodeDescriptor>& source)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TInsertCookie cookie(blockId);
        if (BeginInsert(&cookie)) {
            auto block = New<TCachedBlock>(blockId, data, source);
            cookie.EndInsert(block);

            LOG_DEBUG("Block is put into cache (BlockId: %v, Size: %v, SourceAddress: %v)",
                blockId,
                data.Size(),
                source);
        } else {
            LOG_DEBUG("Failed to cache block due to concurrent read (BlockId: %v, Size: %v, SourceAddress: %v)",
                blockId,
                data.Size(),
                source);
        }
    }

    TCachedBlockPtr FindBlock(const TBlockId& blockId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cachedBlock = TAsyncSlruCacheBase::Find(blockId);
        if (cachedBlock) {
            LogCacheHit(cachedBlock);
        }

        return cachedBlock;
    }

    TFuture<TSharedRef> ReadBlock(
        const TChunkId& chunkId,
        int blockIndex,
        i64 priority,
        bool enableCaching)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // During block peering, data nodes exchange individual blocks.
        // Thus the cache may contain a block not bound to any chunk in the registry.
        // Handle these "unbounded" blocks first.
        // If none is found then look for the owning chunk.
        TBlockId blockId(chunkId, blockIndex);
        auto cachedBlock = FindBlock(blockId);
        if (cachedBlock) {
            LogCacheHit(cachedBlock);
            return MakeFuture(cachedBlock->GetData());
        }

        LogCacheMiss(blockId);

        TInsertCookie cookie(blockId);
        if (enableCaching) {
            if (!BeginInsert(&cookie)) {
                return cookie.GetValue().Apply(
                    BIND([] (const TCachedBlockPtr& cachedBlock) {
                        return cachedBlock->GetData();
                    }));
            }
        }

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);
        if (!chunk) {
            return MakeFuture(TSharedRef());
        }

        auto readGuard = TChunkReadGuard::TryAcquire(chunk);
        if (!readGuard) {
            return MakeFuture(TSharedRef());
        }

        return chunk
            ->ReadBlocks(blockIndex, 1, priority)
            .Apply(BIND(
                &TImpl::OnBlockRead,
                chunk,
                blockIndex,
                Passed(std::move(cookie)),
                Passed(std::move(readGuard))));
    }

    TFuture<std::vector<TSharedRef>> ReadBlocks(
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
        i64 priority)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // NB: Range requests bypass block cache.

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);
        if (!chunk) {
            return MakeFuture(std::vector<TSharedRef>());
        }

        auto readGuard = TChunkReadGuard::TryAcquire(chunk);
        if (!readGuard) {
            return MakeFuture<std::vector<TSharedRef>>(TError(
                NChunkClient::EErrorCode::NoSuchChunk,
                "Cannot read chunk %v since it is scheduled for removal",
                chunkId));
        }

        return chunk
            ->ReadBlocks(firstBlockIndex, blockCount, priority)
             .Apply(BIND(
                &TImpl::OnBlocksRead,
                Passed(std::move(readGuard))));
    }

    i64 GetPendingReadSize() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return PendingReadSize_.load();
    }

    TPendingReadSizeGuard IncreasePendingReadSize(i64 delta)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YASSERT(delta >= 0);
        UpdatePendingReadSize(delta);
        return TPendingReadSizeGuard(delta, Bootstrap_->GetBlockStore());
    }

    void DecreasePendingReadSize(i64 delta)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        UpdatePendingReadSize(-delta);
    }

private:
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    std::atomic<i64> PendingReadSize_ = {0};


    virtual i64 GetWeight(TCachedBlock* block) const override
    {
        return block->GetData().Size();
    }

    void LogCacheHit(TCachedBlockPtr block)
    {
        LOG_TRACE("Block cache hit (BlockId: %v)",
            block->GetKey());
    }

    void LogCacheMiss(const TBlockId& blockId)
    {
        LOG_TRACE("Block cache miss (BlockId: %v)",
            blockId);
    }

    void UpdatePendingReadSize(i64 delta)
    {
        i64 result = (PendingReadSize_ += delta);
        LOG_TRACE("Pending read size updated (PendingReadSize: %v, Delta: %v)",
            result,
            delta);
    }

    static TSharedRef OnBlockRead(
        IChunkPtr chunk,
        int blockIndex,
        TInsertCookie cookie,
        TChunkReadGuard /*readGuard*/,
        const std::vector<TSharedRef>& blocks)
    {
        YASSERT(blocks.size() <= 1);

        TBlockId blockId(chunk->GetId(), blockIndex);
        if (blocks.empty()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchBlock,
                "No such block %v",
                blockId);
        }

        const auto& block = blocks[0];
        auto cachedBlock = New<TCachedBlock>(blockId, block, Null);
        cookie.EndInsert(cachedBlock);

        return block;
    }

    static std::vector<TSharedRef> OnBlocksRead(
        TChunkReadGuard /*readGuard*/,
        const std::vector<TSharedRef>& blocks)
    {
        return blocks;
    }

};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TBlockStore::~TBlockStore()
{ }

TCachedBlockPtr TBlockStore::FindBlock(const TBlockId& blockId)
{
    return Impl_->FindBlock(blockId);
}

TFuture<TSharedRef> TBlockStore::ReadBlock(
    const TChunkId& chunkId,
    int blockIndex,
    i64 priority,
    bool enableCaching)
{
    return Impl_->ReadBlock(
        chunkId,
        blockIndex,
        priority,
        enableCaching);
}

TFuture<std::vector<TSharedRef>> TBlockStore::ReadBlocks(
    const TChunkId& chunkId,
    int firstBlockIndex,
    int blockCount,
    i64 priority)
{
    return Impl_->ReadBlocks(
        chunkId,
        firstBlockIndex,
        blockCount,
        priority);
}

void TBlockStore::PutBlock(
    const TBlockId& blockId,
    const TSharedRef& data,
    const TNullable<TNodeDescriptor>& source)
{
    Impl_->PutBlock(blockId, data, source);
}

i64 TBlockStore::GetPendingReadSize() const
{
    return Impl_->GetPendingReadSize();
}

TPendingReadSizeGuard TBlockStore::IncreasePendingReadSize(i64 delta)
{
    return Impl_->IncreasePendingReadSize(delta);
}

std::vector<TCachedBlockPtr> TBlockStore::GetAllBlocks() const
{
    return Impl_->GetAll();
}

////////////////////////////////////////////////////////////////////////////////

TPendingReadSizeGuard::TPendingReadSizeGuard(
    i64 size,
    TBlockStorePtr owner)
    : Size_(size)
    , Owner_(owner)
{ }

TPendingReadSizeGuard& TPendingReadSizeGuard::operator=(TPendingReadSizeGuard&& other)
{
    swap(*this, other);
    return *this;
}

TPendingReadSizeGuard::~TPendingReadSizeGuard()
{
    if (Owner_) {
        Owner_->Impl_->DecreasePendingReadSize(Size_);
    }
}

TPendingReadSizeGuard::operator bool() const
{
    return Owner_ != nullptr;
}

i64 TPendingReadSizeGuard::GetSize() const
{
    return Size_;
}

void swap(TPendingReadSizeGuard& lhs, TPendingReadSizeGuard& rhs)
{
    using std::swap;
    swap(lhs.Size_, rhs.Size_);
    swap(lhs.Owner_, rhs.Owner_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
