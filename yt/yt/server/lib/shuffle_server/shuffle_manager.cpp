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
using namespace NPushBasedShuffleClient;
using namespace NTransactionClient;

using NApi::NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ShuffleServiceLogger;

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
        TTransactionId parentTransactionId,
        bool usePushBasedShuffle,
        std::string account,
        std::string medium,
        int replicationFactor,
        TPushShuffleConfigPtr pushConfig) override
    {
        TTransactionStartOptions options;
        options.ParentId = parentTransactionId;
        options.PingAncestors = false;

        return Client_->StartTransaction(ETransactionType::Master, options)
            .AsUnique()
            .Apply(BIND(
                &TShuffleManager::DoStartShuffle,
                MakeStrong(this),
                partitionCount,
                usePushBasedShuffle,
                std::move(account),
                std::move(medium),
                replicationFactor,
                std::move(pushConfig))
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

    TFuture<IShuffleControllerPtr> GetController(TTransactionId transactionId) const override
    {
        return BIND(&TShuffleManager::GetShuffleControllerOrThrow, MakeStrong(this), transactionId)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    const IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const IInvokerPtr SerializedInvoker_;

    TProfiler Profiler_;
    TGauge ActiveShuffleCounter_;

    THashMap<TTransactionId, IShuffleControllerPtr> ShuffleControllers_;

    IShuffleControllerPtr GetShuffleControllerOrThrow(TTransactionId transactionId) const
    {
        auto it = ShuffleControllers_.find(transactionId);
        THROW_ERROR_EXCEPTION_IF(
            it == ShuffleControllers_.end(),
            "Shuffle with id %v does not exist",
            transactionId);
        return it->second;
    }

    TTransactionId DoStartShuffle(
        int partitionCount,
        bool usePushBasedShuffle,
        std::string account,
        std::string medium,
        int replicationFactor,
        TPushShuffleConfigPtr pushConfig,
        ITransactionPtr&& transaction)
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

        if (usePushBasedShuffle) {
            ShuffleControllers_[transactionId] = CreatePushBasedShuffleController(
                partitionCount,
                Invoker_,
                Client_,
                std::move(transaction),
                std::move(account),
                std::move(medium),
                replicationFactor,
                std::move(pushConfig));
        } else {
            ShuffleControllers_[transactionId] = CreatePullBasedShuffleController(
                partitionCount,
                Invoker_,
                std::move(transaction));
        }

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
