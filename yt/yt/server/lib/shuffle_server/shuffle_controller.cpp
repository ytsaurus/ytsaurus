#include "shuffle_controller.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TShuffleController
    : public IShuffleController
{
public:
    TShuffleController(
        int partitionCount,
        IInvokerPtr invoker,
        ITransactionPtr transaction)
        : PartitionCount_(partitionCount)
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Transaction_(std::move(transaction))
    { }

    TFuture<void> RegisterChunks(
        std::vector<TInputChunkPtr> chunks,
        std::optional<int> writerIndex) override
    {
        return BIND(
            &TShuffleController::DoRegisterChunks,
            MakeStrong(this),
            Passed(std::move(chunks)),
            writerIndex)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    TFuture<std::vector<TInputChunkSlicePtr>> FetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> writerIndexRange) override
    {
        return BIND(
            &TShuffleController::DoFetchChunks,
            MakeStrong(this),
            partitionIndex,
            writerIndexRange)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    const int PartitionCount_;

    const IInvokerPtr SerializedInvoker_;
    const ITransactionPtr Transaction_;

    std::vector<TInputChunkPtr> Chunks_;

    std::map<int, std::vector<int>> WriterIndexToChunkIndices_;

    void DoRegisterChunks(std::vector<TInputChunkPtr> chunks, std::optional<int> writerIndex)
    {
        Chunks_.reserve(chunks.size());
        for (auto& chunk : chunks) {
            const auto* partitionsExt = chunk->PartitionsExt().get();
            YT_VERIFY(partitionsExt);
            YT_VERIFY(partitionsExt->row_counts_size() == PartitionCount_);
            YT_VERIFY(partitionsExt->uncompressed_data_sizes_size() == PartitionCount_);

            if (writerIndex) {
                WriterIndexToChunkIndices_[*writerIndex].push_back(std::ssize(Chunks_));
            }
            Chunks_.push_back(std::move(chunk));
        }
    }

    std::vector<TInputChunkSlicePtr> DoFetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> writerIndexRange)
    {
        THROW_ERROR_EXCEPTION_IF(
            partitionIndex < 0 || partitionIndex >= PartitionCount_,
            "Invalid partition index: expected a value between 0 and %v (exclusive), but received %v",
            PartitionCount_,
            partitionIndex);

        std::vector<TInputChunkSlicePtr> result;

        auto tryAddChunk = [&] (int index) {
            const auto& chunk = Chunks_[index];
            const auto* partitionsExt = chunk->PartitionsExt().get();
            i64 rowCount = partitionsExt->row_counts()[partitionIndex];
            i64 dataSize = partitionsExt->uncompressed_data_sizes()[partitionIndex];
            i64 compressedDataSize = DivCeil(
                chunk->GetCompressedDataSize(),
                chunk->GetRowCount()) * rowCount;

            if (rowCount > 0) {
                result.push_back(New<TInputChunkSlice>(chunk));
                result.back()->OverrideSize(rowCount, dataSize, compressedDataSize);
            }
        };

        if (writerIndexRange) {
            for (auto it = WriterIndexToChunkIndices_.lower_bound(writerIndexRange->first);
                it != WriterIndexToChunkIndices_.end() && it->first < writerIndexRange->second;
                ++it)
            {
                for (int index : it->second) {
                    tryAddChunk(index);
                }
            }
        } else {
            for (int index = 0; index < std::ssize(Chunks_); ++index) {
                tryAddChunk(index);
            }
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

IShuffleControllerPtr CreateShuffleController(
    int partitionCount,
    IInvokerPtr invoker,
    ITransactionPtr transaction)
{
    return New<TShuffleController>(
        partitionCount,
        std::move(invoker),
        std::move(transaction));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
