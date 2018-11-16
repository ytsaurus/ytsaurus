#include "memory_reader.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TMemoryReader
    : public IChunkReader
{
public:
    TMemoryReader(
        TRefCountedChunkMetaPtr meta,
        std::vector<TBlock> blocks)
        : Meta_(std::move(meta))
        , Blocks_(std::move(blocks))
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
        const std::vector<int>& blockIndexes,
        const TNullable<i64>& /* estimatedSize */) override
    {
        std::vector<TBlock> blocks;
        for (auto index : blockIndexes) {
            YCHECK(index < Blocks_.size());
            blocks.push_back(Blocks_[index]);
        }

        for (const auto& block : blocks) {
            block.ValidateChecksum();
        }

        return MakeFuture(std::move(blocks));
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
        int firstBlockIndex,
        int blockCount,
        const TNullable<i64>& /* estimatedSize */) override
    {
        if (firstBlockIndex >= Blocks_.size()) {
            return MakeFuture(std::vector<TBlock>());
        }

        auto blocks = std::vector<TBlock>(
            Blocks_.begin() + firstBlockIndex,
            Blocks_.begin() + std::min(static_cast<size_t>(blockCount), Blocks_.size() - firstBlockIndex));

        for (const auto& block : blocks) {
            block.ValidateChecksum();
        }

        return MakeFuture(std::move(blocks));
    }

    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientBlockReadOptions& /*options*/,
        TNullable<int> partitionTag,
        const TNullable<std::vector<int>>& extensionTags) override
    {
        YCHECK(!partitionTag);
        return MakeFuture(New<TRefCountedChunkMeta>(FilterChunkMetaByExtensionTags(*Meta_, extensionTags)));
    }

    virtual TChunkId GetChunkId() const override
    {
        return NullChunkId;
    }

    virtual bool IsValid() const override
    {
        return true;
    }

private:
    const TRefCountedChunkMetaPtr Meta_;
    const std::vector<TBlock> Blocks_;

};

IChunkReaderPtr CreateMemoryReader(
    TRefCountedChunkMetaPtr meta,
    std::vector<TBlock> blocks)
{
    return New<TMemoryReader>(
        std::move(meta),
        std::move(blocks));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
