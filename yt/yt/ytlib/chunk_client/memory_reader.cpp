#include "memory_reader.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "private.h"

namespace NYT::NChunkClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

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

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        const std::vector<int>& blockIndexes) override
    {
        std::vector<TBlock> blocks;
        blocks.reserve(blockIndexes.size());
        for (auto index : blockIndexes) {
            YT_VERIFY(index < std::ssize(Blocks_));
            blocks.push_back(Blocks_[index]);
        }

        for (const auto& block : blocks) {
            auto error = block.ValidateChecksum();
            YT_LOG_FATAL_UNLESS(error.IsOK(), error, "Block checksum mismatch during memory block reading");
        }

        return MakeFuture(std::move(blocks));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        int firstBlockIndex,
        int blockCount) override
    {
        if (firstBlockIndex >= std::ssize(Blocks_)) {
            return MakeFuture(std::vector<TBlock>());
        }

        auto blocks = std::vector<TBlock>(
            Blocks_.begin() + firstBlockIndex,
            Blocks_.begin() + std::min(static_cast<size_t>(blockCount), Blocks_.size() - firstBlockIndex));

        for (const auto& block : blocks) {
            auto error = block.ValidateChecksum();
            YT_LOG_FATAL_UNLESS(error.IsOK(), error, "Block checksum mismatch during memory block reading");
        }

        return MakeFuture(std::move(blocks));
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& /*options*/,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        YT_VERIFY(!partitionTag);
        return MakeFuture(New<TRefCountedChunkMeta>(FilterChunkMetaByExtensionTags(*Meta_, extensionTags)));
    }

    TChunkId GetChunkId() const override
    {
        return NullChunkId;
    }

    TInstant GetLastFailureTime() const override
    {
        return TInstant();
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

} // namespace NYT::NChunkClient
