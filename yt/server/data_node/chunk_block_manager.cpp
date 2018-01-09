#include "chunk_block_manager.h"
#include "private.h"
#include "blob_reader_cache.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "config.h"
#include "location.h"
#include "peer_block_distributor.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/file_reader.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_service.h>

#include <util/random/random.h>

namespace NYT {
namespace NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TBlocksExt;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static const auto& Profiler = DataNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(
    const TBlockId& blockId,
    const TBlock& data,
    const TNullable<TNodeDescriptor>& source)
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
            NProfiling::TProfiler(
                DataNodeProfiler.GetPathPrefix() +
                "/block_cache/" +
                FormatEnum(EBlockType::CompressedData)))
        , Config_(config)
        , Bootstrap_(bootstrap)
        , OrchidService_(IYPathService::FromProducer(BIND(&TImpl::BuildOrchid, MakeWeak(this))))
    { }

    TCachedBlockPtr FindCachedBlock(const TBlockId& blockId)
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

    void PutCachedBlock(
        const TBlockId& blockId,
        const TBlock& data,
        const TNullable<TNodeDescriptor>& source)
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

        auto updateRecentlyReadBlockQueueCallback = BIND(
            &TImpl::UpdateRecentlyReadBlockQueue,
            MakeStrong(this),
            chunkId,
            blockIndexes);
        auto updatePeerBlockDistributor = BIND(
            &TImpl::UpdatePeerBlockDistributor,
            MakeStrong(this),
            chunkId,
            blockIndexes);

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
                return MakeFuture(blocks)
                    .Apply(updateRecentlyReadBlockQueueCallback)
                    .Apply(updatePeerBlockDistributor);
            }

            auto readGuard = TChunkReadGuard::AcquireOrThrow(chunk);
            auto asyncBlocks = chunk->ReadBlockSet(blockIndexes, options);

            // Hold the read guard.
            auto asyncResult = asyncBlocks
                .Apply(BIND(&TImpl::OnBlocksRead, Passed(std::move(readGuard))));

            if (type == EObjectType::Chunk || type == EObjectType::ErasureChunk) {
                asyncResult = asyncResult
                    .Apply(updateRecentlyReadBlockQueueCallback)
                    .Apply(updatePeerBlockDistributor);
            }

            return asyncResult;
        } catch (const std::exception& ex) {
            return MakeFuture<std::vector<TBlock>>(TError(ex));
        }
    }

    struct TReadBlock
    {
        TBlockId BlockId;
        i64 Size;
        TInstant Time;
        EBlockOrigin BlockOrigin;
    };

    std::vector<TReadBlock> GetRecentlyReadBlocks() const
    {
        // Note that after acquiring this lock we are effectively blocking the ReadBlockSet requests.
        TReaderGuard guard(RecentlyReadBlockQueueLock_);

        const auto& Profiler = NDataNode::Profiler;
        PROFILE_TIMING("/recently_read_block_request_time") {
            return std::vector<TReadBlock>(RecentlyReadBlockQueue_.begin(), RecentlyReadBlockQueue_.end());
        }
    }

    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }

private:
    const TDataNodeConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    IYPathServicePtr OrchidService_;

    TReaderWriterSpinLock RecentlyReadBlockQueueLock_;
    // This queue stores pairs <blockId, blockSize>.
    std::deque<TReadBlock> RecentlyReadBlockQueue_;

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

    std::vector<TBlock> UpdateRecentlyReadBlockQueue(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndexes,
        const std::vector<TBlock>& blocks)
    {
        std::vector<TReadBlock> readBlocks;
        auto now = NProfiling::GetInstant();
        // NB: blocks may contain less elements than blockIndexes.
        for (int listIndex = 0; listIndex < std::min(blockIndexes.size(), blocks.size()); ++listIndex) {
            const auto& block = blocks[listIndex];
            if (block && RandomNumber<double>() < Config_->RecentlyReadBlockQueueSampleRate) {
                int blockIndex = blockIndexes[listIndex];
                readBlocks.emplace_back(TReadBlock{
                    TBlockId(chunkId, blockIndex),
                    static_cast<i64>(block.Size()),
                    now,
                    block.BlockOrigin});
            }
        }

        if (!readBlocks.empty()) {
            TWriterGuard guard(RecentlyReadBlockQueueLock_);
            RecentlyReadBlockQueue_.insert(RecentlyReadBlockQueue_.end(), readBlocks.begin(), readBlocks.end());
            if (RecentlyReadBlockQueue_.size() > Config_->RecentlyReadBlockQueueSize) {
                RecentlyReadBlockQueue_.erase(
                    RecentlyReadBlockQueue_.begin(),
                    RecentlyReadBlockQueue_.end() - Config_->RecentlyReadBlockQueueSize);
            }
        }

        return blocks;
    }

    std::vector<TBlock> UpdatePeerBlockDistributor(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndexes,
        const std::vector<TBlock>& blocks)
    {
        for (int listIndex = 0; listIndex < std::min(blockIndexes.size(), blocks.size()); ++listIndex) {
            int blockIndex = blockIndexes[listIndex];
            const auto& block = blocks[listIndex];
            if (block) {
                Bootstrap_->GetPeerBlockDistributor()->OnBlockRequested(TBlockId{chunkId, blockIndex}, block.Size());
            }
        }
        return blocks;
    }

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("recently_read_blocks")
                    .DoListFor(GetRecentlyReadBlocks(), [] (TFluentList fluent, const TReadBlock& readBlock) {
                        fluent.Item()
                            .BeginMap()
                                .Item("chunk_id").Value(readBlock.BlockId.ChunkId)
                                .Item("block_index").Value(readBlock.BlockId.BlockIndex)
                                .Item("size").Value(readBlock.Size)
                                .Item("time").Value(readBlock.Time)
                                .Item("block_origin").Value(readBlock.BlockOrigin)
                            .EndMap();
                    })
            .EndMap();
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
    const TNullable<TNodeDescriptor>& source)
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

IYPathServicePtr TChunkBlockManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
