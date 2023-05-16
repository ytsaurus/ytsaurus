#include "block_tracking_chunk_reader.h"

#include <yt/yt/core/misc/memory_reference_tracker.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBlockTrackingChunkReader)

class TBlockTrackingChunkReader
    : public IChunkReader
{
public:
    TBlockTrackingChunkReader(
        IChunkReaderPtr underlying,
        IMemoryReferenceTrackerPtr tracker)
        : Underlying_(std::move(underlying))
        , Tracker_(std::move(tracker))
    {
        YT_VERIFY(Underlying_);
        YT_VERIFY(Tracker_);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        return TrackBlocks(Underlying_->ReadBlocks(options, blockIndexes));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        return TrackBlocks(Underlying_->ReadBlocks(options, firstBlockIndex, blockCount));
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = {}) override
    {
        return Underlying_->GetMeta(options, partitionTag, extensionTags);
    }

    TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    TInstant GetLastFailureTime() const override
    {
        return Underlying_->GetLastFailureTime();
    }

private:
    const IChunkReaderPtr Underlying_;
    const IMemoryReferenceTrackerPtr Tracker_;

    TFuture<std::vector<TBlock>> TrackBlocks(const TFuture<std::vector<TBlock>>& future)
    {
        return future.Apply(BIND([this, this_ = MakeStrong(this)] (const std::vector<TBlock>& blocks) {
            std::vector<TBlock> output;
            output.reserve(blocks.size());

            for (const auto& block : blocks) {
                output.push_back(block);
                output.back().Data = TrackMemory(Tracker_, block.Data, /*keepExistingTracking*/ true);
            }

            return output;
        }));
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockTrackingChunkReader)

IChunkReaderPtr CreateBlockTrackingChunkReader(
    IChunkReaderPtr underlying,
    IMemoryReferenceTrackerPtr tracker)
{
    return New<TBlockTrackingChunkReader>(
        std::move(underlying),
        std::move(tracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
