#include "shuffle_controller.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;

using NApi::NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

class TShuffleController
    : public IShuffleController
{
public:
    TShuffleController(
        ITransactionPtr transaction,
        int partitionCount,
        IInvokerPtr invoker)
        : Transaction_(std::move(transaction))
        , PartitionCount_(partitionCount)
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
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

    TTransactionId GetTransactionId() const override
    {
        return Transaction_->GetId();
    }

    TFuture<void> Finish() override
    {
        return BIND(
            &TShuffleController::DoFinish,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    ~TShuffleController()
    {
        YT_VERIFY(IsFinished_);
    }

private:
    const ITransactionPtr Transaction_;
    const int PartitionCount_;

    bool IsFinished_ = false;

    std::vector<TInputChunkPtr> Chunks_;
    IInvokerPtr SerializedInvoker_;

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

            if (rowCount > 0) {
                result.emplace_back(New<TInputChunkSlice>(chunk));
                result.back()->OverrideSize(rowCount, dataSize);
            }
        }

        return result;
    }

    void DoFinish()
    {
        YT_VERIFY(!IsFinished_);
        IsFinished_ = true;

        WaitFor(Transaction_->Commit().AsVoid())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<IShuffleControllerPtr> CreateShuffleController(
    IClientPtr client,
    int partitionCount,
    IInvokerPtr invoker)
{
    return client->StartTransaction(ETransactionType::Master)
        .ApplyUnique(BIND([partitionCount, invoker = std::move(invoker)](ITransactionPtr&& transaction) mutable {
            return New<TShuffleController>(
                std::move(transaction),
                partitionCount,
                std::move(invoker));
        }))
        .As<IShuffleControllerPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
