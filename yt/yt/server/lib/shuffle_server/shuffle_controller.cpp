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
        std::optional<int> writerIndex,
        bool overwriteExistingWriterData) override
    {
        return BIND(
            &TShuffleController::DoRegisterChunks,
            MakeStrong(this),
            Passed(std::move(chunks)),
            writerIndex,
            overwriteExistingWriterData)
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
    struct TWriterChunk
    {
        TInputChunkPtr Chunk;
        std::optional<int> Epoch;
        std::optional<int> WriterIndex;
    };

    const int PartitionCount_;

    const IInvokerPtr SerializedInvoker_;
    const ITransactionPtr Transaction_;

    std::vector<TWriterChunk> Chunks_;

    std::map<int, std::vector<int>> WriterIndexToChunkIndices_;
    std::unordered_map<int, int> WriterIndexToEpoch_;

    void DoRegisterChunks(std::vector<TInputChunkPtr> chunks, std::optional<int> writerIndex, bool overwriteExistingWriterData)
    {
        std::optional<int> currentEpoch;
        if (writerIndex) {
            if (overwriteExistingWriterData) {
                auto it = WriterIndexToEpoch_.find(*writerIndex);
                if (it == WriterIndexToEpoch_.end()) {
                    it = WriterIndexToEpoch_.insert({*writerIndex, 0}).first;
                }
                YT_VERIFY(it->second < std::numeric_limits<int>::max());
                ++it->second;
            }
            currentEpoch = WriterIndexToEpoch_[*writerIndex];
        }

        Chunks_.reserve(chunks.size());
        for (auto& chunk : chunks) {
            const auto* partitionsExt = chunk->PartitionsExt().get();
            YT_VERIFY(partitionsExt);
            YT_VERIFY(partitionsExt->row_counts_size() == PartitionCount_);
            YT_VERIFY(partitionsExt->uncompressed_data_sizes_size() == PartitionCount_);

            if (writerIndex) {
                WriterIndexToChunkIndices_[*writerIndex].push_back(std::ssize(Chunks_));
            }

            Chunks_.push_back(TWriterChunk{
                .Chunk = std::move(chunk),
                .Epoch = currentEpoch,
                .WriterIndex = writerIndex,
            });
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
            if (chunk.WriterIndex.has_value() && chunk.Epoch < WriterIndexToEpoch_[*chunk.WriterIndex]) {
                return;
            }

            const auto* partitionsExt = chunk.Chunk->PartitionsExt().get();
            i64 rowCount = partitionsExt->row_counts()[partitionIndex];
            i64 dataSize = partitionsExt->uncompressed_data_sizes()[partitionIndex];
            i64 compressedDataSize = DivCeil(
                chunk.Chunk->GetCompressedDataSize(),
                chunk.Chunk->GetRowCount()) * rowCount;
            i64 uncompressedDataSize = DivCeil(
                chunk.Chunk->GetUncompressedDataSize(),
                chunk.Chunk->GetRowCount()) * rowCount;

            if (rowCount > 0) {
                result.push_back(New<TInputChunkSlice>(chunk.Chunk));
                result.back()->OverrideSize(rowCount, dataSize, compressedDataSize, uncompressedDataSize);
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
