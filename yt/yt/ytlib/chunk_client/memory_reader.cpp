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

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& /*options*/,
        const std::vector<int>& blockIndexes,
        std::optional<i64> /* estimatedSize */) override
    {
        std::vector<TBlock> blocks;
        for (auto index : blockIndexes) {
            YT_VERIFY(index < Blocks_.size());
            blocks.push_back(Blocks_[index]);
        }

        for (const auto& block : blocks) {
            auto error = block.ValidateChecksum();
            YT_LOG_FATAL_UNLESS(error.IsOK(), error, "Block checksum mismatch during memory block reading");
        }

        return MakeFuture(std::move(blocks));
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& /*options*/,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> /* estimatedSize */) override
    {
        if (firstBlockIndex >= Blocks_.size()) {
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

    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& /*options*/,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        YT_VERIFY(!partitionTag);
        return MakeFuture(New<TRefCountedChunkMeta>(FilterChunkMetaByExtensionTags(*Meta_, extensionTags)));
    }

    virtual TChunkId GetChunkId() const override
    {
        return NullChunkId;
    }

    virtual TInstant GetLastFailureTime() const override
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
