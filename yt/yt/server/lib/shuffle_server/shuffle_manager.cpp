#include "shuffle_manager.h"

#include "private.h"
#include "shuffle_controller.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTransactionClient;

using NApi::NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ShuffleServiceLogger;

////////////////////////////////////////////////////////////////////////////////

class TShuffleManager
    : public IShuffleManager
{
public:
    TShuffleManager(IClientPtr client, IInvokerPtr invoker)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , SerializedInvoker_(CreateSerializedInvoker(Invoker_))
        , Profiler_("/shuffle_manager")
        , ActiveShuffleCounter_(Profiler_.Gauge("/active"))
    { }

    TFuture<TTransactionId> StartShuffle(
        int partitionCount,
        TTransactionId parentTransactionId) override
    {
        TTransactionStartOptions options;
        options.ParentId = parentTransactionId;
        options.PingAncestors = false;

        return Client_->StartTransaction(ETransactionType::Master, options)
            .ApplyUnique(BIND(&TShuffleManager::DoStartShuffle, MakeStrong(this), partitionCount)
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
    const IInvokerPtr Invoker_;
    const IInvokerPtr SerializedInvoker_;

    TProfiler Profiler_;
    TGauge ActiveShuffleCounter_;

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

    TTransactionId DoStartShuffle(int partitionCount, ITransactionPtr&& transaction)
    {
        auto transactionId = transaction->GetId();
        YT_LOG_DEBUG("Shuffle transaction is created (TransactionId: %v)", transactionId);

        transaction->SubscribeAborted(
            BIND(
                &TShuffleManager::OnTransactionAborted,
                MakeStrong(this),
                transactionId)
            .Via(Invoker_));

        transaction->SubscribeCommitted(
            BIND(&TShuffleManager::OnTransactionCommitted,
                MakeStrong(this),
                transactionId)
            .Via(Invoker_));

        ShuffleControllers_[transactionId] = CreateShuffleController(
            partitionCount,
            Invoker_,
            std::move(transaction));

        ActiveShuffleCounter_.Update(ShuffleControllers_.size());

        return transactionId;
    }

    void OnTransactionAborted(TTransactionId transactionId, const TErrorOr<void>& error)
    {
        YT_LOG_INFO(error, "Shuffle transaction is aborted (TransactionId: %v)", transactionId);

        DoFinishShuffle(transactionId);
    }

    void OnTransactionCommitted(TTransactionId transactionId)
    {
        YT_LOG_INFO("Shuffle transaction is committed (TransactionId: %v)", transactionId);

        DoFinishShuffle(transactionId);
    }

    void DoFinishShuffle(TTransactionId transactionId)
    {
        EraseOrCrash(ShuffleControllers_, transactionId);
        ActiveShuffleCounter_.Update(ShuffleControllers_.size());
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
    return New<TShuffleManager>(
        std::move(client),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
