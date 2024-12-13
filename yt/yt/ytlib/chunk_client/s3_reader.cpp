#include "s3_reader.h"

#include "chunk_reader.h"
#include "chunk_reader_allowing_repair.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////

class TS3ReadSession final
{
};

////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Keep in mind that we want IChunkReaderAllowingRepair.
class TS3Reader
    : public IChunkReader
{
public:
    TS3Reader(
        TS3MediumStubPtr medium,
        TS3ReaderConfigPtr config,
        TChunkId chunkId)
        : Medium_(std::move(medium))
        , Client_(Medium_->GetClient())
        , Config_(std::move(config))
        , ChunkId_(std::move(chunkId))
        , ChunkPlacement_(Medium_->GetChunkPlacement(ChunkId_))
    {
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        // TODO(achulkov2): Improve this code to aggregate ranges of consecutive blocks.
        auto blockRanges = std::vector<TBlockRange>{};
        blockRanges.reserve(blockIndexes.size());
        for (const auto blockIndex : blockIndexes) {
            blockRanges.push_back({.StartBlockIndex = blockIndex, .EndBlockIndex = blockIndex + 1});
        }
        return ReadBlockRanges(options, blockRanges);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        auto blockRange = TBlockRange{
            .StartBlockIndex = firstBlockIndex,
            .EndBlockIndex = firstBlockIndex + blockCount
        };
        return ReadBlockRanges(options, {blockRange});
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = {}) override
    {
        // TODO(achulkov2): Support partition tag and extension tags.
        // TODO(achulkov2): Do not forget about statistics.

        YT_UNIMPLEMENTED();
    }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    TInstant GetLastFailureTime() const override
    {
        // TODO(achulkov2): Implement.

        YT_UNIMPLEMENTED();
    }

private:
    const TS3MediumStubPtr Medium_;
    const NS3::IClientPtr Client_;
    const TS3ReaderConfigPtr Config_;
    const TChunkId ChunkId_;
    const TS3MediumStub::TS3ObjectPlacement ChunkPlacement_;

    // TODO(achulkov2): How do we want to cache meta?
    TRefCountedChunkMetaPtr ChunkMeta_;

    struct TBlockRange
    {
        //! Inclusive.
        int StartBlockIndex = 0;
        //! Not inclusive.
        int EndBlockIndex = 0;
    };

    TFuture<std::vector<TBlock>> ReadBlockRanges(
        const TReadBlocksOptions& options,
        const std::vector<TBlockRange>& blockRanges)
    {
        // TODO(achulkov2): Implement.

        YT_UNIMPLEMENTED();
    }
};



////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateS3Reader(
    TS3MediumStubPtr medium,
    TS3ReaderConfigPtr config,
    TChunkId chunkId)
{
    return New<TS3Reader>(
        std::move(medium),
        std::move(config),
        std::move(chunkId));
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient