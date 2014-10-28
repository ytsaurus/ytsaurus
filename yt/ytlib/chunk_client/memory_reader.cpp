#include "memory_reader.h"
#include "chunk_reader.h"
#include "chunk_meta_extensions.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

class TMemoryReader
    : public IChunkReader
{
public:
    TMemoryReader(
        TChunkMeta meta,
        std::vector<TSharedRef> blocks)
        : Meta_(std::move(meta))
        , Blocks_(std::move(blocks))
    { }

    virtual TAsyncReadBlocksResult ReadBlocks(const std::vector<int>& blockIndexes) override
    {
        std::vector<TSharedRef> blocks;
        for (auto index : blockIndexes) {
            YCHECK(index < Blocks_.size());
            blocks.push_back(Blocks_[index]);
        }
        return MakeFuture(TReadBlocksResult(std::move(blocks)));
    }

    virtual TAsyncReadBlocksResult ReadBlocks(int firstBlockIndex, int blockCount) override
    {
        if (firstBlockIndex >= Blocks_.size()) {
            return MakeFuture(TReadBlocksResult());
        }

        return MakeFuture(TReadBlocksResult(std::vector<TSharedRef>(
            Blocks_.begin() + firstBlockIndex,
            Blocks_.begin() + std::min(static_cast<size_t>(blockCount), Blocks_.size() - firstBlockIndex))));
    }

    virtual TAsyncGetMetaResult GetMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* extensionTags = nullptr) override
    {
        YCHECK(!partitionTag);
        return MakeFuture(TGetMetaResult(
            extensionTags
            ? FilterChunkMetaByExtensionTags(Meta_, *extensionTags)
            : Meta_));
    }

    virtual TChunkId GetChunkId() const override
    {
        // ToDo(psushin): make YUNIMPLEMENTED, after fixing sequential reader.
        return NullChunkId;
    }

private:
    TChunkMeta Meta_;
    std::vector<TSharedRef> Blocks_;

};

IChunkReaderPtr CreateMemoryReader(const TChunkMeta& meta, const std::vector<TSharedRef>& blocks)
{
    return New<TMemoryReader>(meta, blocks);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
