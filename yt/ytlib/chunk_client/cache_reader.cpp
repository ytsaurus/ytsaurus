#include "cache_reader.h"

#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "block_cache.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TCacheReader
    : public IChunkReader
{
public:
    TCacheReader(
        const TChunkId& chunkId,
        IBlockCachePtr blockCache)
        : ChunkId_(chunkId)
        , BlockCache_(std::move(blockCache))
    {  }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
        const std::vector<int>& blockIndexes,
        const TNullable<i64>& /* estimatedSize */) override
    {
        // NB: Cache-based readers shouldn't report chunk reader statistics.

        std::vector<TBlock> blocks;
        for (auto index : blockIndexes) {
            TBlockId blockId(ChunkId_, index);
            auto block = BlockCache_->Find(blockId, EBlockType::CompressedData);
            if (!block) {
                return MakeFuture<std::vector<TBlock>>(TError("Block %v is not found in the compressed data cache", blockId));
            }

            blocks.push_back(block);
        }
        return MakeFuture(std::move(blocks));
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
        int firstBlockIndex,
        int blockCount,
        const TNullable<i64>& /* estimatedSize */) override
    {
        // NB: Cache-based readers shouldn't report chunk reader statistics.

        std::vector<TBlock> blocks;
        for (int index = 0; index < blockCount; ++index) {
            TBlockId blockId(ChunkId_, firstBlockIndex + index);
            auto block = BlockCache_->Find(blockId, EBlockType::CompressedData);
            if (!block) {
                return MakeFuture<std::vector<TBlock>>(TError("Block %v is not found in the compressed data cache", blockId));
            }

            blocks.push_back(block);
        }

        return MakeFuture(std::move(blocks));
    }

    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientBlockReadOptions& /*options*/,
        TNullable<int> /*partitionTag*/,
        const TNullable<std::vector<int>>& /*extensionTags*/) override
    {
        // Cache-based readers shouldn't ask meta from chunk reader.
        Y_UNREACHABLE();
    }

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    virtual bool IsValid() const override
    {
        return true;
    }

private:
    const TChunkId ChunkId_;
    const IBlockCachePtr BlockCache_;
};

IChunkReaderPtr CreateCacheReader(
    const TChunkId& chunkId,
    IBlockCachePtr blockCache)
{
    return New<TCacheReader>(chunkId, std::move(blockCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
