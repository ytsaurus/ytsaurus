#include "chunk_file_reader_adapter.h"
#include "chunk_file_reader.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_allowing_repair.h>

namespace NYT::NIO {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TChunkFileReaderAdapter
    : public IChunkReaderAllowingRepair
{
public:
    explicit TChunkFileReaderAdapter(TChunkFileReaderPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& options,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize) override
    {
        return Underlying_->ReadBlocks(options, blockIndexes, estimatedSize);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> estimatedSize) override
    {
        return Underlying_->ReadBlocks(options, firstBlockIndex, blockCount, estimatedSize);
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        Y_UNUSED(extensionTags);
        return Underlying_->GetMeta(options, partitionTag);
    }

    TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    TInstant GetLastFailureTime() const override
    {
        return {};
    }

    void SetSlownessChecker(TCallback<TError(i64, TDuration)> /*slownessChecker*/) override
    { }

private:
    const TChunkFileReaderPtr Underlying_;
};

IChunkReaderAllowingRepairPtr CreateChunkFileReaderAdapter(
    TChunkFileReaderPtr underlying)
{
    return New<TChunkFileReaderAdapter>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
