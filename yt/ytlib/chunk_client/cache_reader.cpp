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

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
        const std::vector<int>& blockIndexes,
        std::optional<i64> /* estimatedSize */) override
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
        std::optional<i64> /* estimatedSize */) override
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
        std::optional<int> /*partitionTag*/,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
        // Cache-based readers shouldn't ask meta from chunk reader.
        YT_ABORT();
    }

    virtual TFuture<TSharedRef> LookupRows(
        const TClientBlockReadOptions& /*options*/,
        const TSharedRange<NTableClient::TKey>& /*lookupKeys*/,
        NCypressClient::TObjectId /*tableId*/,
        NHydra::TRevision /*revision*/,
        const NTableClient::TTableSchema& /*tableSchema*/,
        std::optional<i64> /*estimatedSize*/,
        std::atomic<i64>* /*uncompressedDataSize*/,
        const NTableClient::TColumnFilter& /*columnFilter*/,
        NTableClient::TTimestamp /*timestamp*/,
        NCompression::ECodec /*codecId*/,
        bool /*produceAllVersions*/) override
    {
        YT_UNIMPLEMENTED();
    }

    virtual bool IsLookupSupported() const
    {
        YT_UNIMPLEMENTED();
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
    TChunkId chunkId,
    IBlockCachePtr blockCache)
{
    return New<TCacheReader>(chunkId, std::move(blockCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
