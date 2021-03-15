#include "file_reader_adapter.h"
#include "chunk_reader_allowing_repair.h"
#include "file_reader.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TFileReaderAdapter
    : public IChunkReaderAllowingRepair
{
public:
    explicit TFileReaderAdapter(TFileReaderPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize) override
    {
        return Underlying_->ReadBlocks(options, blockIndexes, estimatedSize);
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> estimatedSize) override
    {
        return Underlying_->ReadBlocks(options, firstBlockIndex, blockCount, estimatedSize);
    }

    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientBlockReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        return Underlying_->GetMeta(options, partitionTag, extensionTags);
    }

    virtual TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    virtual TInstant GetLastFailureTime() const override
    {
        return {};
    }

    virtual void SetSlownessChecker(TCallback<TError(i64, TDuration)> /*slownessChecker*/) override
    { }

private:
    const TFileReaderPtr Underlying_;
};

IChunkReaderAllowingRepairPtr CreateFileReaderAdapter(TFileReaderPtr underlying)
{
    return New<TFileReaderAdapter>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
