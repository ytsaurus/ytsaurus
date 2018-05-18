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
        const TChunkMeta& meta,
        std::vector<TBlock> blocks)
        : Meta_(meta)
        , Blocks_(std::move(blocks))
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
        const std::vector<int>& blockIndexes) override
    {
        std::vector<TBlock> blocks;
        for (auto index : blockIndexes) {
            YCHECK(index < Blocks_.size());
            blocks.push_back(Blocks_[index]);
        }
        return MakeFuture(std::move(blocks));
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
        int firstBlockIndex,
        int blockCount) override
    {
        if (firstBlockIndex >= Blocks_.size()) {
            return MakeFuture(std::vector<TBlock>());
        }

        return MakeFuture(std::vector<TBlock>(
            Blocks_.begin() + firstBlockIndex,
            Blocks_.begin() + std::min(static_cast<size_t>(blockCount), Blocks_.size() - firstBlockIndex)));
    }

    virtual TFuture<TChunkMeta> GetMeta(
        const TClientBlockReadOptions& /*options*/,
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>>& extensionTags) override
    {
        YCHECK(!partitionTag);
        return MakeFuture(FilterChunkMetaByExtensionTags(Meta_, extensionTags));
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
    const TChunkMeta Meta_;
    const std::vector<TBlock> Blocks_;

};

IChunkReaderPtr CreateMemoryReader(
    const TChunkMeta& meta,
    std::vector<TBlock> blocks)
{
    return New<TMemoryReader>(meta, std::move(blocks));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
