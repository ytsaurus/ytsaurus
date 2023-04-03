#include "cache_reader.h"

#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "block_cache.h"

namespace NYT::NChunkClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TCacheReader
    : public IChunkReader
{
public:
    TCacheReader(
        TChunkId chunkId,
        IBlockCachePtr blockCache)
        : ChunkId_(chunkId)
        , BlockCache_(std::move(blockCache))
    {  }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        const std::vector<int>& blockIndexes) override
    {
        // NB: Cache-based readers shouldn't report chunk reader statistics.

        std::vector<TBlock> blocks;
        for (auto index : blockIndexes) {
            TBlockId blockId(ChunkId_, index);
            auto block = BlockCache_->FindBlock(blockId, EBlockType::CompressedData).Block;
            if (!block) {
                return MakeFuture<std::vector<TBlock>>(TError("Block %v is not found in the compressed data cache",
                    blockId));
            }

            blocks.push_back(std::move(block));
        }
        return MakeFuture(std::move(blocks));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        int firstBlockIndex,
        int blockCount) override
    {
        // NB: Cache-based readers shouldn't report chunk reader statistics.

        std::vector<TBlock> blocks;
        for (int index = 0; index < blockCount; ++index) {
            TBlockId blockId(ChunkId_, firstBlockIndex + index);
            auto block = BlockCache_->FindBlock(blockId, EBlockType::CompressedData).Block;
            if (!block) {
                return MakeFuture<std::vector<TBlock>>(TError("Block %v is not found in the compressed data cache",
                    blockId));
            }

            blocks.push_back(std::move(block));
        }

        return MakeFuture(std::move(blocks));
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& /*options*/,
        std::optional<int> /*partitionTag*/,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
        // Cache-based readers shouldn't ask meta from chunk reader.
        YT_ABORT();
    }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    TInstant GetLastFailureTime() const override
    {
        return TInstant();
    }

private:
    const TChunkId ChunkId_;
    const IBlockCachePtr BlockCache_;
};

IChunkReaderPtr CreateCacheReader(
    TChunkId chunkId,
    IBlockCachePtr blockCache)
{
    return New<TCacheReader>(chunkId, std::move(blockCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
