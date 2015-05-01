#include "stdafx.h"
#include "local_chunk_reader.h"

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <server/data_node/chunk.h>
#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

class TLocalChunkReader
    : public IChunkReader
{
public:
    TLocalChunkReader(
        TBootstrap* bootstrap,
        TReplicationReaderConfigPtr config,
        IChunkPtr chunk,
        IBlockCachePtr blockCache,
        TClosure failureHandler)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Chunk_(std::move(chunk))
        , BlockCache_(std::move(blockCache))
        , FailureHandler_(std::move(failureHandler))
    { }

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(const std::vector<int>& blockIndexes) override
    {
        auto blockStore = Bootstrap_->GetBlockStore();

        std::vector<TFuture<TSharedRef>> asyncBlocks;
        asyncBlocks.reserve(blockIndexes.size());

        i64 priority = 0;
        for (int blockIndex : blockIndexes) {
            auto blockId = TBlockId(Chunk_->GetId(), blockIndex);
            auto cachedBlock = BlockCache_->Find(blockId, EBlockType::CompressedData);
            if (cachedBlock) {
                asyncBlocks.push_back(MakeFuture(cachedBlock));
                continue;
            }

            auto asyncBlock = blockStore->ReadBlock(
                Chunk_->GetId(),
                blockIndex,
                priority,
                Config_->EnableCaching);

            asyncBlocks.push_back(asyncBlock.Apply(BIND(
                &TLocalChunkReader::OnGotBlock,
                MakeStrong(this),
                blockIndex)));

            // Assign decreasing priorities to block requests to take advantage of sequential read.
            --priority;
        }

        return Combine(asyncBlocks);
    }

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(int firstBlockIndex, int blockCount) override
    {
        std::vector<int> blockIndexes;
        for (int index = firstBlockIndex; index < firstBlockIndex + blockCount; ++index) {
            blockIndexes.push_back(index);
        }
        return ReadBlocks(blockIndexes);
    }

    virtual TFuture<TChunkMeta> GetMeta(
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>>& extensionTags) override
    {
        return Chunk_->ReadMeta(0, extensionTags).Apply(BIND(
            &TLocalChunkReader::OnGotMeta,
            MakeStrong(this),
            partitionTag));
    }

    virtual TChunkId GetChunkId() const override
    {
        return Chunk_->GetId();
    }

private:
    const TBootstrap* Bootstrap_;
    const TReplicationReaderConfigPtr Config_;
    const IChunkPtr Chunk_;
    const IBlockCachePtr BlockCache_;
    const TClosure FailureHandler_;


    TSharedRef OnGotBlock(int blockIndex, const TErrorOr<TSharedRef>& blockOrError)
    {
        if (!blockOrError.IsOK()) {
            OnFailed();
            THROW_ERROR_EXCEPTION(
                NDataNode::EErrorCode::LocalChunkReaderFailed,
                "Error reading local chunk block %v:%v",
                Chunk_->GetId(),
                blockIndex)
                    << blockOrError;
        }

        const auto& block = blockOrError.Value();
        if (!block) {
            OnFailed();
            THROW_ERROR_EXCEPTION(
                NDataNode::EErrorCode::LocalChunkReaderFailed,
                "Local chunk block %v:%v is not available",
                Chunk_->GetId(),
                blockIndex);
        }

        return block;
    }

    TChunkMeta OnGotMeta(
        const TNullable<int>& partitionTag,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        if (!metaOrError.IsOK()) {
            OnFailed();
            THROW_ERROR metaOrError;
        }

        const auto& meta = metaOrError.Value();
        return partitionTag
           ? FilterChunkMetaByPartitionTag(*meta, *partitionTag)
           : TChunkMeta(*meta);
    }

    void OnFailed()
    {
        if (FailureHandler_) {
            FailureHandler_.Run();
        }
    }

};

IChunkReaderPtr CreateLocalChunkReader(
    TBootstrap* bootstrap,
    TReplicationReaderConfigPtr config,
    IChunkPtr chunk,
    IBlockCachePtr blockCache,
    TClosure failureHandler)
{
    return New<TLocalChunkReader>(
        bootstrap,
        std::move(config),
        std::move(chunk),
        std::move(blockCache),
        std::move(failureHandler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
