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

    TFuture<void> RegisterChunks(std::vector<TInputChunkPtr> chunks) override
    {
        return BIND(
            &TShuffleController::DoRegisterChunks,
            MakeStrong(this),
            Passed(std::move(chunks)))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    TFuture<std::vector<TInputChunkSlicePtr>> FetchChunks(int partitionIndex) override
    {
        return BIND(
            &TShuffleController::DoFetchChunks,
            MakeStrong(this),
            partitionIndex)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    const int PartitionCount_;

    const IInvokerPtr SerializedInvoker_;
    const ITransactionPtr Transaction_;

    std::vector<TInputChunkPtr> Chunks_;

    void DoRegisterChunks(std::vector<TInputChunkPtr> chunks)
    {
        Chunks_.reserve(chunks.size());
        for (auto& chunk : chunks) {
            const auto* partitionsExt = chunk->PartitionsExt().get();
            YT_VERIFY(partitionsExt);
            YT_VERIFY(partitionsExt->row_counts_size() == PartitionCount_);
            YT_VERIFY(partitionsExt->uncompressed_data_sizes_size() == PartitionCount_);

            Chunks_.emplace_back(std::move(chunk));
        }
    }

    std::vector<TInputChunkSlicePtr> DoFetchChunks(int partitionIndex)
    {
        THROW_ERROR_EXCEPTION_IF(
            partitionIndex < 0 || partitionIndex >= PartitionCount_,
            "Invalid partition index: expected a value between 0 and %v (exclusive), but received %v",
            PartitionCount_,
            partitionIndex);

        std::vector<TInputChunkSlicePtr> result;
        for (const auto& chunk : Chunks_) {
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
