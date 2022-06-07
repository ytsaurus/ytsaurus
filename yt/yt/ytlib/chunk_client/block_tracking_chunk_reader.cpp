#include "block_tracking_chunk_reader.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TBlockTrackingChunkReader)

class TBlockTrackingChunkReader
    : public IChunkReader
{
public:
    TBlockTrackingChunkReader(
        IChunkReaderPtr underlying,
        IBlockTrackerPtr tracker,
        std::optional<NNodeTrackerClient::EMemoryCategory> category)
        : Underlying_(std::move(underlying))
        , BlockTracker_(std::move(tracker))
        , Category_(category)
    {
        YT_VERIFY(Underlying_);
        YT_VERIFY(BlockTracker_);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& options,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize = {}) override
    {
        return ApplyBlockTracking(Underlying_->ReadBlocks(options, blockIndexes, estimatedSize));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> estimatedSize = {}) override
    {
        return ApplyBlockTracking(Underlying_->ReadBlocks(options, firstBlockIndex, blockCount, estimatedSize));
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
    const IBlockTrackerPtr BlockTracker_;
    const std::optional<NNodeTrackerClient::EMemoryCategory> Category_;

    TFuture<std::vector<TBlock>> ApplyBlockTracking(const TFuture<std::vector<TBlock>>& future)
    {
        return future.Apply(BIND([this, this_ = MakeStrong(this)] (const std::vector<TBlock>& blocks) {
            std::vector<TBlock> output(blocks.size());
            for (int i = 0; i < std::ssize(blocks); ++i) {
                output[i] = AttachCategory(
                    blocks[i],
                    BlockTracker_,
                    Category_);
            }
            return output;
        }));
    }
};

DEFINE_REFCOUNTED_TYPE(TBlockTrackingChunkReader);

IChunkReaderPtr CreateBlockTrackingChunkReader(
    IChunkReaderPtr underlying,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category)
{
    return New<TBlockTrackingChunkReader>(std::move(underlying), std::move(tracker), category);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
