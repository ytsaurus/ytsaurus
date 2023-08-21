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
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        return Underlying_->ReadBlocks(options.ClientOptions, blockIndexes);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        return Underlying_->ReadBlocks(options.ClientOptions, firstBlockIndex, blockCount);
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
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
