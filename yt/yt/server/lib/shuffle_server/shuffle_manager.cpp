#include "shuffle_manager.h"
#include "shuffle_controller.h"

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYPath;

using NApi::NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

class TShuffleManager
    : public IShuffleManager
{
public:
    TShuffleManager(IClientPtr client, IInvokerPtr invoker)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , SerializedInvoker_(CreateSerializedInvoker(Invoker_))
    { }

    TFuture<TTransactionId> StartShuffle(int partitionCount) override
    {
        return CreateShuffleController(Client_, partitionCount, Invoker_)
            .ApplyUnique(BIND(&TShuffleManager::DoRegisterShuffle, MakeStrong(this))
                .AsyncVia(SerializedInvoker_));
    }

    TFuture<void> FinishShuffle(TTransactionId transactionId) override
    {
        return BIND(
            &TShuffleManager::DoFinishShuffle,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run(transactionId);
    }

    TFuture<void> RegisterChunks(
        TTransactionId transactionId,
        std::vector<TInputChunkPtr> chunks) override
    {
        return BIND(
            &TShuffleManager::DoRegisterChunks,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run(transactionId, std::move(chunks));
    }

    TFuture<std::vector<TInputChunkSlicePtr>> FetchChunks(
        TTransactionId transactionId,
        int partitionIndex) override
    {
        return BIND(
            &TShuffleManager::DoFetchChunks,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run(transactionId, partitionIndex);
    }

private:
    const IClientPtr Client_;
    IInvokerPtr Invoker_;
    IInvokerPtr SerializedInvoker_;

    THashMap<TTransactionId, IShuffleControllerPtr> ShuffleControllers_;

    const IShuffleControllerPtr& FindShuffleControllerOrThrow(const TTransactionId& transactionId) const
    {
        auto it = ShuffleControllers_.find(transactionId);
        THROW_ERROR_EXCEPTION_IF(
            it == ShuffleControllers_.end(),
            "Shuffle with id %Qv does not exist",
            transactionId);
        return it->second;
    }

    TTransactionId DoRegisterShuffle(IShuffleControllerPtr&& controller)
    {
        auto transactionId = controller->GetTransactionId();
        ShuffleControllers_[transactionId] = controller;
        return transactionId;
    }

    TFuture<void> DoFinishShuffle(TTransactionId transactionId)
    {
        auto shuffleController = FindShuffleControllerOrThrow(transactionId);
        ShuffleControllers_.erase(transactionId);

        return shuffleController->Finish();
    }

    TFuture<void> DoRegisterChunks(
        TTransactionId transactionId,
        std::vector<TInputChunkPtr> chunks)
    {
        const auto& shuffleController = FindShuffleControllerOrThrow(transactionId);
        return shuffleController->RegisterChunks(std::move(chunks));
    }

    TFuture<std::vector<TInputChunkSlicePtr>> DoFetchChunks(
        TTransactionId transactionId,
        int partitionIndex)
    {
        const auto& shuffleController = FindShuffleControllerOrThrow(transactionId);
        return shuffleController->FetchChunks(partitionIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

IShuffleManagerPtr CreateShuffleManager(
    IClientPtr client,
    IInvokerPtr invoker)
{
    return New<TShuffleManager>(std::move(client), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
