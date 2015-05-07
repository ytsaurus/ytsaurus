#include "stdafx.h"
#include "block_store.h"
#include "private.h"
#include "chunk.h"
#include "config.h"
#include "chunk_registry.h"
#include "blob_reader_cache.h"
#include "location.h"

#include <ytlib/object_client/helpers.h>

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

using namespace NObjectClient;
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
            LOG_TRACE("Block cache hit (BlockId: %v)", blockId);
        } else {
            LOG_TRACE("Block cache miss (BlockId: %v)", blockId);
        }

        return cachedBlock;
    }

    TFuture<std::vector<TSharedRef>> ReadBlocks(
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        bool enableCaching)
    {
        if (IsJournalChunk(chunkId)) {
            // Journal chunk: shortcut.
            auto chunkRegistry = Bootstrap_->GetChunkRegistry();
            auto chunk = chunkRegistry->GetChunkOrThrow(chunkId);
            return chunk->ReadBlocks(firstBlockIndex, blockCount, priority);
        } else {
            // Blob chunk: reduce to block set read.
            std::vector<int> blockIndexes;
            for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
                blockIndexes.push_back(blockIndex);
            }
            return ReadBlocks(chunkId, blockIndexes, priority, enableCaching);
        }
    }

    TFuture<std::vector<TSharedRef>> ReadBlocks(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndexes,
        i64 priority,
        bool enableCaching)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        enableCaching &= !IsJournalChunk(chunkId);

        auto chunkRegistry = Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(chunkId);
        if (!chunk) {
            // During block peering, data nodes exchange individual blocks.
            // Thus the cache may contain a block not bound to any chunk in the registry.
            // We must look for these blocks.
            std::vector<TSharedRef> blocks;
            if (!IsJournalChunk(chunkId)) {
                for (int blockIndex : blockIndexes) {
                    auto blockId = TBlockId(chunkId, blockIndex);
                    auto cachedBlock = FindBlock(blockId);
                    blocks.push_back(cachedBlock ? cachedBlock->GetData() : TSharedRef());
                }
            }
            return MakeFuture(blocks);
        }

        auto session = New<TReadSession>();
        session->ChunkId = chunkId;
        for (int blockIndex : blockIndexes) {
            TReadSession::TBlockEntry entry;
            entry.BlockIndex = blockIndex;
            session->Blocks.emplace_back(std::move(entry));
        }
        
        session->ReadGuard = TChunkReadGuard::TryAcquire(chunk);
        if (!session->ReadGuard) {
            return MakeFuture<std::vector<TSharedRef>>(TError(
                NChunkClient::EErrorCode::NoSuchChunk,
                "Cannot read chunk %v since it is scheduled for removal",
                chunkId));
        }

        // Results to wait for, including cache and read requests.
        std::vector<TFuture<void>> asyncResults;

        // Fetch blocks from cache, if appropriate.
        if (!IsJournalChunk(chunkId)) {
            for (auto& entry : session->Blocks) {
                auto blockId = TBlockId(chunkId, entry.BlockIndex);
                auto cachedBlock = FindBlock(blockId);
                if (cachedBlock) {
                    entry.Data = cachedBlock->GetData();
                    entry.Cached = true;
                } else if (enableCaching) {
                    entry.Cookie = TInsertCookie(blockId);
                    if (!BeginInsert(&entry.Cookie)) {
                        entry.Cached = true;
                        auto asyncResult = entry.Cookie.GetValue().Apply(
                            BIND([session, &entry] (const TCachedBlockPtr& cachedBlock) {
                                entry.Data = cachedBlock->GetData();
                            }));
                        asyncResults.emplace_back(std::move(asyncResult));
                    }
                }
            }
        }

        // Extract maximum contiguous ranges of uncached blocks.
        {
            int localIndex = 0;
            while (localIndex < blockIndexes.size()) {
                if (session->Blocks[localIndex].Cached) {
                    ++localIndex;
                    continue;                    
                }

                int startLocalIndex = localIndex;
                int startBlockIndex = session->Blocks[startLocalIndex].BlockIndex;
                int endLocalIndex = startLocalIndex;
                while (endLocalIndex < blockIndexes.size() &&
                       !session->Blocks[endLocalIndex].Cached &&
                       session->Blocks[endLocalIndex].BlockIndex == startBlockIndex + (endLocalIndex - startLocalIndex))
                {
                    ++endLocalIndex;
                }

                int blockCount = endLocalIndex - startLocalIndex;
                auto asyncResult = chunk->ReadBlocks(startBlockIndex, blockCount, priority).Apply(BIND(
                    &TImpl::OnBlocksRead,
                    session,
                    startLocalIndex,
                    blockCount));
                asyncResults.emplace_back(std::move(asyncResult));

                localIndex = endLocalIndex;
            }
        }

        return Combine(asyncResults).Apply(BIND(&TImpl::OnAllBlocksRead, session));
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


    struct TReadSession
        : public TIntrinsicRefCounted
    {
        struct TBlockEntry
        {
            int BlockIndex = -1;
            TSharedRef Data;
            TInsertCookie Cookie;
            bool Cached = false;
        };

        TChunkId ChunkId;
        TChunkReadGuard ReadGuard;
        std::vector<TBlockEntry> Blocks;
    };

    using TReadSessionPtr = TIntrusivePtr<TReadSession>;


    virtual i64 GetWeight(TCachedBlock* block) const override
    {
        return block->GetData().Size();
    }


    void UpdatePendingReadSize(i64 delta)
    {
        i64 result = (PendingReadSize_ += delta);
        LOG_TRACE("Pending read size updated (PendingReadSize: %v, Delta: %v)",
            result,
            delta);
    }


    static void OnBlocksRead(
        TReadSessionPtr session,
        int startLocalIndex,
        int blockCount,
        const std::vector<TSharedRef>& blocks)
    {
        for (int localIndex = startLocalIndex; localIndex < startLocalIndex + blockCount; ++localIndex) {
            int blockIndex = session->Blocks[localIndex].BlockIndex;
            auto blockId = TBlockId(session->ChunkId, blockIndex);
            auto& entry = session->Blocks[localIndex];
            auto block = localIndex - startLocalIndex < blocks.size()
                ? blocks[localIndex - startLocalIndex]
                : TSharedRef();
            entry.Data = block;
            if (entry.Cookie.IsActive()) {
                YCHECK(block);
                auto cachedBlock = New<TCachedBlock>(blockId, block, Null);
                entry.Cookie.EndInsert(cachedBlock);
            }
        }
    }

    static std::vector<TSharedRef> OnAllBlocksRead(TReadSessionPtr session)
    {
        std::vector<TSharedRef> blocks;

        // Move data from session.
        blocks.reserve(session->Blocks.size());
        for (auto& entry : session->Blocks) {
            blocks.emplace_back(std::move(entry.Data));
        }

        return blocks;
    }


    static bool IsJournalChunk(const TChunkId& chunkId)
    {
        return TypeFromId(DecodeChunkId(chunkId).Id) == EObjectType::Journal;
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

TFuture<std::vector<TSharedRef>> TBlockStore::ReadBlocks(
    const TChunkId& chunkId,
    int firstBlockIndex,
    int blockCount,
    i64 priority,
    bool enableCaching)
{
    return Impl_->ReadBlocks(
        chunkId,
        firstBlockIndex,
        blockCount,
        priority,
        enableCaching);
}

TFuture<std::vector<TSharedRef>> TBlockStore::ReadBlocks(
    const TChunkId& chunkId,
    const std::vector<int>& blockIndexes,
    i64 priority,
    bool enableCaching)
{
    return Impl_->ReadBlocks(
        chunkId,
        blockIndexes,
        priority,
        enableCaching);
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
