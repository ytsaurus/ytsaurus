#include "input_transactions_manager.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/client/transaction_client/public.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NYPath;
using namespace NYTree;
using namespace NLogging;
using namespace NTransactionClient;
using namespace NScheduler;

using NScheduler::TOperationId;

////////////////////////////////////////////////////////////////////////////////

TInputTransactionsManager::TInputTransactionsManager(
    TInputClients clients,
    TOperationId operationId,
    const std::vector<TRichYPath>& filesAndTables,
    bool forceStartNativeTransaction,
    TTransactionId userTransactionId,
    TControllerAgentConfigPtr config,
    TLogger logger)
    : OperationId_(operationId)
    , Client_(clients.Client)
    , SchedulerClient_(clients.SchedulerClient)
    , Logger(logger)
    , ControllerConfig_(config)
    , UserTransactionId_(userTransactionId)
{
    for (const auto& path : filesAndTables) {
        ParentToTransaction_[GetTransactionParentFromPath(path)] = nullptr;
        if (auto parent = path.GetTransactionId(); parent) {
            OldNonTrivialInputTransactionParents_.push_back(*parent);
        }
    }

    if (forceStartNativeTransaction) {
        ParentToTransaction_[UserTransactionId_] = nullptr;
    }
}

TFuture<void> TInputTransactionsManager::Start(
    IAttributeDictionaryPtr transactionAttributes)
{
    std::vector<TFuture<void>> transactionFutures;

    for (const auto& [parentTransactionId, _] : ParentToTransaction_) {
        YT_LOG_INFO("Starting input transaction (ParentId: %v)", parentTransactionId);

        TTransactionStartOptions options;
        options.AutoAbort = false;
        options.PingAncestors = false;
        options.Attributes = transactionAttributes->Clone();
        options.ParentId = parentTransactionId;
        options.Timeout = ControllerConfig_->OperationTransactionTimeout;
        options.PingPeriod = ControllerConfig_->OperationTransactionPingPeriod;

        auto transactionFuture = Client_->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            options);

        transactionFutures.push_back(
            transactionFuture.Apply(
                BIND(
                    [=, this, this_ = MakeStrong(this)]
                    (const TErrorOr<NNative::ITransactionPtr>& transactionOrError)
                    {
                        THROW_ERROR_EXCEPTION_IF_FAILED(
                            transactionOrError,
                            "Error starting input transaction");

                        auto transaction = transactionOrError.Value();

                        YT_LOG_INFO("Input transaction started (TransactionId: %v)",
                            transaction->GetId());

                        YT_VERIFY(ParentToTransaction_.contains(parentTransactionId));
                        // NB: Assignments are not racy, because invoker of this "Apply" is serialized.
                        ParentToTransaction_[parentTransactionId] = transaction;
                        if (parentTransactionId == UserTransactionId_) {
                            NativeInputTransaction_ = transaction;
                        }
                    })
                        .AsyncVia(GetCurrentInvoker())));
    }

    return AllSucceeded(std::move(transactionFutures));
}

std::vector<TRichTransactionId> TInputTransactionsManager::RestoreFromNestedTransactions(
    const TControllerTransactionIds& transactionIds) const
{
    if (transactionIds.NestedInputIds.size() != OldNonTrivialInputTransactionParents_.size()) {
        THROW_ERROR_EXCEPTION(
            "Transaction count from Cypress differs from internal transaction count, will use clean start")
            << TErrorAttribute("cypress_count", transactionIds.NestedInputIds.size())
            << TErrorAttribute("internal_count", OldNonTrivialInputTransactionParents_.size());
    }

    std::vector<TRichTransactionId> flatTransactionIds(ParentToTransaction_.size());
    THashMap<TTransactionId, int> parentToIndex;
    for (const auto& [i, parentAndTransaction] : Enumerate(ParentToTransaction_)) {
        parentToIndex[parentAndTransaction.first] = i;
    }

    for (int i = 0; i < std::ssize(OldNonTrivialInputTransactionParents_); ++i) {
        auto txId = transactionIds.NestedInputIds[i];
        auto parent = OldNonTrivialInputTransactionParents_[i];
        auto& flatTxId = flatTransactionIds[parentToIndex[parent]];
        auto richTxId = TRichTransactionId { .Id = txId, .ParentId = parent};
        if (flatTxId.Id) {
            if (flatTxId != richTxId) {
                THROW_ERROR_EXCEPTION("Expected transactions with same parent to be equal")
                    << TErrorAttribute("index", i)
                    << TErrorAttribute("transaction_id_left", flatTxId)
                    << TErrorAttribute("transaction_id_right", richTxId);
            }
        } else {
            flatTxId = richTxId;
        }
    }

    if (ParentToTransaction_.contains(UserTransactionId_)) {
        flatTransactionIds[parentToIndex[UserTransactionId_]] = TRichTransactionId {
            .Id = transactionIds.InputId,
            .ParentId = UserTransactionId_
        };
    }

    return flatTransactionIds;
}

TFuture<void> TInputTransactionsManager::Revive(TControllerTransactionIds transactionIds)
{
    // COMPAT(coteeq)
    if (transactionIds.InputIds.empty()) {
        YT_LOG_DEBUG("Received input transactions in old format, trying to restore into new format");
        try {
            transactionIds.InputIds = RestoreFromNestedTransactions(transactionIds);
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex).Wrap("Failed to restore transactions from old format"));
        }
    }

    if (auto error = ValidateSchedulerTransactions(transactionIds); !error.IsOK()) {
        return MakeFuture(error);
    }

    std::vector<ITransactionPtr> transactions;
    for (const auto& [i, txId] : Enumerate(transactionIds.InputIds)) {
        YT_VERIFY(txId.Id != NullTransactionId);
        TTransactionAttachOptions options;
        options.Ping = true;
        options.PingAncestors = false;
        options.PingPeriod = ControllerConfig_->OperationTransactionPingPeriod;

        ITransactionPtr transaction;
        try {
            transaction = Client_->AttachTransaction(txId.Id, options);
            ParentToTransaction_[txId.ParentId] = transaction;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error attaching operation transaction (TransactionId: %v)",
                txId.Id);
        }
        transactions.push_back(transaction);
    }

    std::vector<TFuture<void>> pingFutures;

    for (int i = 0; i < std::ssize(transactions); ++i) {
        auto transaction = transactions[i];
        if (!transaction) {
            YT_LOG_INFO("Input transaction is missing, will use clean start "
                    "(TransactionId: %v)",
                    transactionIds.InputIds[i].Id);
            pingFutures.push_back(
                MakeFuture(
                    TError("Failed to attach transaction")
                        << TErrorAttribute("transaction_id", transactionIds.InputIds[i])));
        } else {
            pingFutures.push_back(
                transaction->Ping()
                    .Apply(BIND([txId = transaction->GetId()] (const TError& error) {
                        if (!error.IsOK()) {
                            THROW_ERROR TError("Failed to ping transaction").Wrap(error)
                                << TErrorAttribute("transaction_id", txId);
                        }
                    })
                        .AsyncVia(GetCurrentInvoker())));
        }
    }

    return AllSucceeded(std::move(pingFutures))
        .Apply(BIND([this, this_ = MakeStrong(this)] {
            for (const auto& [parent, transaction] : ParentToTransaction_) {
                YT_VERIFY(transaction);
                if (parent == UserTransactionId_) {
                    NativeInputTransaction_ = transaction;
                }
            }
        })
            .AsyncVia(GetCurrentInvoker()));
}

TTransactionId TInputTransactionsManager::GetTransactionIdForObject(
    const NYPath::TRichYPath& path) const
{
    auto it = ParentToTransaction_.find(GetTransactionParentFromPath(path));
    YT_VERIFY(it != ParentToTransaction_.end());
    return it->second->GetId();
}

std::vector<TTransactionId> TInputTransactionsManager::GetCompatDuplicatedNestedTransactionIds() const
{
    std::vector<TTransactionId> transactionIds;
    for (const auto& parent : OldNonTrivialInputTransactionParents_) {
        const auto& tx = ParentToTransaction_.at(parent);
        transactionIds.push_back(tx->GetId());
    }

    return transactionIds;
}

void TInputTransactionsManager::FillSchedulerTransactionIds(
    TControllerTransactionIds* transactionIds) const
{
    for (const auto& [parent, tx] : ParentToTransaction_) {
        YT_VERIFY(tx);
        transactionIds->InputIds.push_back(
            TRichTransactionId { .Id = tx->GetId(), .ParentId = parent });
    }

    // COMPAT(coteeq)
    auto nativeId = GetNativeInputTransactionId();
    transactionIds->InputId = nativeId;
    transactionIds->NestedInputIds = GetCompatDuplicatedNestedTransactionIds();
}

TFuture<void> TInputTransactionsManager::Abort()
{
    std::vector<TFuture<void>> abortFutures;
    for (const auto& [_, transaction] : ParentToTransaction_) {
        if (transaction) {
            abortFutures.push_back(SchedulerClient_->AttachTransaction(transaction->GetId())->Abort());
        }
    }

    return AllSet(abortFutures).AsVoid();
}

TTransactionId TInputTransactionsManager::GetNativeInputTransactionId() const
{
    return NativeInputTransaction_ ? NativeInputTransaction_->GetId() : NullTransactionId;
}

TTransactionId TInputTransactionsManager::GetTransactionParentFromPath(const TRichYPath& path) const
{
    return path.GetTransactionId().value_or(UserTransactionId_);
}

TError TInputTransactionsManager::ValidateSchedulerTransactions(
    const NScheduler::TControllerTransactionIds& transactionIds) const
{
    if (transactionIds.InputIds.size() != ParentToTransaction_.size()) {
        return TError("Inconsistent number of transactions")
                << TErrorAttribute("cypress_transactions_count", transactionIds.InputIds.size())
                << TErrorAttribute("controller_transactions_count", ParentToTransaction_.size());
    }

    for (const auto& [i, txId] : Enumerate(transactionIds.InputIds)) {
        if (!txId.Id) {
            return TError(
                "Found null transaction coming from scheduler, considering all transactions to be lost")
                << TErrorAttribute("transaction_id", txId.Id)
                << TErrorAttribute("parent_transaction_id", txId.ParentId)
                << TErrorAttribute("transaction_index", i);
        }
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
