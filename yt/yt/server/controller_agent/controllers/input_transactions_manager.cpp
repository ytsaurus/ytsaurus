#include "input_transactions_manager.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/transaction_client/public.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NHiveClient;
using namespace NYPath;
using namespace NYTree;
using namespace NLogging;
using namespace NTransactionClient;
using namespace NScheduler;

using NScheduler::TOperationId;
using NApi::NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

TClusterResolver::TClusterResolver(IClientPtr client)
    : LocalClusterName_(client->GetClusterName().value_or(""))
{ }

TClusterName TClusterResolver::GetClusterName(const TRichYPath& path)
{
    auto clusterName = path.GetCluster();
    if (clusterName && !IsLocalClusterName(*clusterName)) {
        return TClusterName(*clusterName);
    }
    return LocalClusterName;
}

const TString& TClusterResolver::GetLocalClusterName() const
{
    return LocalClusterName_;
}

bool TClusterResolver::IsLocalClusterName(const TString& clusterName) const
{
    return IsLocal(TClusterName(clusterName)) || clusterName == LocalClusterName_;
}

void TClusterResolver::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, LocalClusterName_);
}

////////////////////////////////////////////////////////////////////////////////

TInputTransactionsManager::TInputTransactionsManager(
    IClientPtr client,
    TClusterResolverPtr clusterResolver,
    TOperationId operationId,
    const std::vector<TRichYPath>& filesAndTables,
    bool forceStartLocalTransaction,
    TTransactionId userTransactionId,
    const TString& authenticatedUser,
    TControllerAgentConfigPtr config,
    TLogger logger)
    : OperationId_(operationId)
    , Logger(logger)
    , ControllerConfig_(config)
    , UserTransactionId_(userTransactionId)
    , ClusterResolver_(clusterResolver)
{
    auto createClient = [&] (const auto& name) {
        if (!Clients_.contains(name)) {
            Clients_[name] = IsLocal(name)
                ? client
                : client
                    ->GetNativeConnection()
                    ->GetClusterDirectory()
                    ->GetConnectionOrThrow(name.Underlying())
                    ->CreateNativeClient(client->GetOptions());
        }
    };
    for (const auto& path : filesAndTables) {
        ParentToTransaction_[GetTransactionParentFromPath(path)] = nullptr;
        auto clusterName = ClusterResolver_->GetClusterName(path);
        ValidateRemoteOperationsAllowed(clusterName, authenticatedUser, path);
        createClient(clusterName);

        if (auto parent = path.GetTransactionId(); parent) {
            OldNonTrivialInputTransactionParents_.push_back(*parent);
        }
    }

    if (forceStartLocalTransaction) {
        createClient(LocalClusterName);
        auto localParent = TRichTransactionId{
            .Id = UserTransactionId_,
            .Cluster = LocalClusterName,
        };
        ParentToTransaction_[localParent] = nullptr;
    }
}

TFuture<void> TInputTransactionsManager::Start(
    IAttributeDictionaryPtr transactionAttributes)
{
    std::vector<TFuture<void>> transactionFutures;

    for (const auto& [parentTransaction, _] : ParentToTransaction_) {
        YT_LOG_INFO("Starting input transaction (Parent: %v)", parentTransaction);

        TTransactionStartOptions options;
        options.AutoAbort = false;
        options.PingAncestors = false;
        options.Attributes = transactionAttributes->Clone();
        options.ParentId = parentTransaction.Id;
        options.Timeout = ControllerConfig_->OperationTransactionTimeout;
        options.PingPeriod = ControllerConfig_->OperationTransactionPingPeriod;

        auto transactionFuture = Clients_[parentTransaction.Cluster]->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            options);

        transactionFutures.push_back(
            transactionFuture.Apply(
                BIND(
                    [=, this, this_ = MakeStrong(this), parentTransaction = parentTransaction]
                    (const TErrorOr<NNative::ITransactionPtr>& transactionOrError) {
                        THROW_ERROR_EXCEPTION_IF_FAILED(
                            transactionOrError,
                            "Error starting input transaction");

                        auto transaction = transactionOrError.Value();

                        YT_LOG_INFO("Input transaction started (TransactionId: %v)",
                            transaction->GetId());

                        YT_VERIFY(ParentToTransaction_.contains(parentTransaction));
                        // NB: Assignments are not racy, because invoker of this "Apply" is serialized.
                        ParentToTransaction_[parentTransaction] = transaction;
                        if (parentTransaction.Id == UserTransactionId_) {
                            LocalInputTransaction_ = transaction;
                        }
                    })
                        .AsyncVia(GetCurrentInvoker())));
    }

    return AllSucceeded(std::move(transactionFutures));
}

std::vector<TRichTransactionId> TInputTransactionsManager::RestoreFromNestedTransactions(
    const TControllerTransactionIds& transactionIds) const
{
    /// Old (aka NonTrivial) transactions come in a fixed layout, which is hopefully the same as in
    /// OldNonTrivialInputTransactionParents.
    ///
    /// OldNonTrivialInputTransactionParents: [p1,  p2,  p1 ]
    /// transactionIds.NestedInputIds:        [tx1, tx2, tx1]
    ///
    /// We need to restore a mapping parent_transaction -> transaction, given transactions from
    /// cypress and parents from snapshot. The tricky part is that transactionIds.NestedInputIds is
    /// kind of a mapping table -> transaction, so transactions may be duplicated there. The
    /// following code also does a little sanity check: it verifies that each parent has exactly one
    /// unique child.
    if (transactionIds.NestedInputIds.size() != OldNonTrivialInputTransactionParents_.size()) {
        THROW_ERROR_EXCEPTION(
            "Transaction count from Cypress differs from internal transaction count, will use clean start")
            << TErrorAttribute("cypress_count", transactionIds.NestedInputIds.size())
            << TErrorAttribute("internal_count", OldNonTrivialInputTransactionParents_.size());
    }

    std::vector<TRichTransactionId> flatTransactionIds(ParentToTransaction_.size());
    std::map<TRichTransactionId, int> parentToIndex;
    for (const auto& [i, parentAndTransaction] : Enumerate(ParentToTransaction_)) {
        const auto& [parent, _] = parentAndTransaction;
        YT_VERIFY(IsLocal(parent.Cluster));
        parentToIndex[parent] = i;
    }

    for (int i = 0; i < std::ssize(OldNonTrivialInputTransactionParents_); ++i) {
        auto transactionId = transactionIds.NestedInputIds[i];
        auto oldParentId = OldNonTrivialInputTransactionParents_[i];
        auto& flatTransactionId = flatTransactionIds[parentToIndex[MakeRichTransactionId(oldParentId)]];
        auto richTransactionId = TRichTransactionId{
            .Id = transactionId,
            .ParentId = oldParentId,
            .Cluster = LocalClusterName
        };
        bool firstTimeSeen = flatTransactionId.Id == NullTransactionId;
        if (firstTimeSeen) {
            flatTransactionId = richTransactionId;
        } else {
            if (flatTransactionId != richTransactionId) {
                THROW_ERROR_EXCEPTION("Expected transactions with same parent to be equal")
                    << TErrorAttribute("index", i)
                    << TErrorAttribute("transaction_id_left", flatTransactionId)
                    << TErrorAttribute("transaction_id_right", richTransactionId);
            }
        }
    }

    auto inputTransactionParent = MakeRichTransactionId(UserTransactionId_);
    if (ParentToTransaction_.contains(inputTransactionParent)) {
        YT_VERIFY(!flatTransactionIds[parentToIndex[inputTransactionParent]].Id);
        flatTransactionIds[parentToIndex[inputTransactionParent]] = TRichTransactionId{
            .Id = transactionIds.InputId,
            .ParentId = UserTransactionId_,
            .Cluster = LocalClusterName,
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
            return MakeFuture(
                TError("Failed to restore transactions from old format")
                    << ex);
        }
    }

    if (auto error = ValidateSchedulerTransactions(transactionIds); !error.IsOK()) {
        return MakeFuture(error);
    }

    std::vector<ITransactionPtr> transactions;
    for (const auto& [_, transactionId] : Enumerate(transactionIds.InputIds)) {
        YT_VERIFY(transactionId.Id != NullTransactionId);
        TTransactionAttachOptions options;
        options.Ping = true;
        options.PingAncestors = false;
        options.PingPeriod = ControllerConfig_->OperationTransactionPingPeriod;

        ITransactionPtr transaction;
        try {
            transaction = Clients_[transactionId.Cluster]->AttachTransaction(transactionId.Id, options);
            auto parent = TRichTransactionId{
                .Id = transactionId.ParentId,
                .Cluster = transactionId.Cluster,
            };
            YT_VERIFY(ParentToTransaction_.contains(parent) && !ParentToTransaction_[parent]);
            ParentToTransaction_[parent] = transaction;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error attaching operation transaction (TransactionId: %v)",
                transactionId.Id);
        }
        transactions.push_back(transaction);
    }

    std::vector<TFuture<void>> pingFutures;

    for (int i = 0; i < std::ssize(transactions); ++i) {
        auto transaction = transactions[i];
        if (!transaction) {
            YT_LOG_INFO(
                "Input transaction is missing, will use clean start "
                "(TransactionId: %v)",
                transactionIds.InputIds[i].Id);
            pingFutures.push_back(
                MakeFuture(
                    TError("Failed to attach transaction")
                        << TErrorAttribute("transaction_id", transactionIds.InputIds[i])));
        } else {
            pingFutures.push_back(
                transaction->Ping()
                    .Apply(BIND([transactionId = transaction->GetId()] (const TError& error) {
                        if (!error.IsOK()) {
                            THROW_ERROR_EXCEPTION("Failed to ping transaction")
                                << TErrorAttribute("transaction_id", transactionId)
                                << error;
                        }
                    })
                        .AsyncVia(GetCurrentInvoker())));
        }
    }

    return AllSucceeded(std::move(pingFutures))
        .Apply(BIND([this, this_ = MakeStrong(this)] {
            for (const auto& [parent, transaction] : ParentToTransaction_) {
                YT_VERIFY(transaction);
                if (parent.Id == UserTransactionId_) {
                    LocalInputTransaction_ = transaction;
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
        const auto& transaction = GetOrCrash(ParentToTransaction_, MakeRichTransactionId(parent));
        transactionIds.push_back(transaction->GetId());
    }

    return transactionIds;
}

void TInputTransactionsManager::FillSchedulerTransactionIds(
    TControllerTransactionIds* transactionIds) const
{
    for (const auto& [parent, transaction] : ParentToTransaction_) {
        YT_VERIFY(transaction);
        transactionIds->InputIds.push_back(
            TRichTransactionId{
                .Id = transaction->GetId(),
                .ParentId = parent.Id,
                .Cluster = parent.Cluster,
            });
    }

    // COMPAT(coteeq)
    auto localId = GetLocalInputTransactionId();
    transactionIds->InputId = localId;
    transactionIds->NestedInputIds = GetCompatDuplicatedNestedTransactionIds();
}

TFuture<void> TInputTransactionsManager::Abort(IClientPtr schedulerClient)
{
    std::vector<TFuture<void>> abortFutures;
    for (const auto& [parent, transaction] : ParentToTransaction_) {
        if (transaction) {
            auto client = schedulerClient;
            if (!IsLocal(parent.Cluster)) {
                client = schedulerClient
                    ->GetNativeConnection()
                    ->GetClusterDirectory()
                    ->GetConnectionOrThrow(parent.Cluster.Underlying())
                    ->CreateNativeClient(schedulerClient->GetOptions());
                if (!client) {
                    auto error = TError(
                        "Failed to create scheduler client for cluster %Qv",
                        parent.Cluster);
                    YT_LOG_WARNING(error, "Failed to abort input transaction (TransactionId: %v)",
                        transaction->GetId());
                    abortFutures.push_back(MakeFuture(error));
                    continue;
                }
            }

            try {
                abortFutures.push_back(
                    client
                        ->AttachTransaction(transaction->GetId())
                        ->Abort());
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(
                    ex,
                    "Error attaching operation transaction for abort (TransactionId: %v)",
                    transaction->GetId());
            }

        }
    }

    return AllSet(abortFutures).AsVoid();
}

TTransactionId TInputTransactionsManager::GetLocalInputTransactionId() const
{
    return LocalInputTransaction_ ? LocalInputTransaction_->GetId() : NullTransactionId;
}

TClusterResolverPtr TInputTransactionsManager::GetClusterResolver() const
{
    return ClusterResolver_;
}

TRichTransactionId TInputTransactionsManager::GetTransactionParentFromPath(const TRichYPath& path) const
{
    TRichTransactionId parent;
    parent.Cluster = ClusterResolver_->GetClusterName(path);

    auto effectiveUserTransactionId = IsLocal(parent.Cluster)
        ? UserTransactionId_
        : NullTransactionId;

    parent.Id = path.GetTransactionId().value_or(effectiveUserTransactionId);
    parent.ParentId = NullTransactionId;
    return parent;
}

TError TInputTransactionsManager::ValidateSchedulerTransactions(
    const NScheduler::TControllerTransactionIds& transactionIds) const
{
    if (transactionIds.InputIds.size() != ParentToTransaction_.size()) {
        return TError("Inconsistent number of transactions")
                << TErrorAttribute("cypress_transactions_count", transactionIds.InputIds.size())
                << TErrorAttribute("controller_transactions_count", ParentToTransaction_.size());
    }

    for (const auto& [i, transactionId] : Enumerate(transactionIds.InputIds)) {
        if (!transactionId.Id) {
            return TError(
                "Found null transaction coming from scheduler, considering all transactions to be lost")
                << TErrorAttribute("transaction_id", transactionId.Id)
                << TErrorAttribute("parent_transaction_id", transactionId.ParentId)
                << TErrorAttribute("transaction_index", i);
        }
    }

    return TError();
}

void TInputTransactionsManager::ValidateRemoteOperationsAllowed(
    const NScheduler::TClusterName& clusterName,
    const TString& authenticatedUser,
    const NYPath::TRichYPath& path) const
{
    if (!IsLocal(clusterName)) {
        const auto& disallowRemoteConfig = ControllerConfig_->DisallowRemoteOperations;
        if (!disallowRemoteConfig->AllowedUsers.contains(authenticatedUser)) {
            THROW_ERROR_EXCEPTION(
                "User %Qv is not allowed to start operations with remote clusters",
                authenticatedUser)
                << TErrorAttribute("input_table_path", path);
        }
        if (!disallowRemoteConfig->AllowedClusters.contains(clusterName.Underlying())) {
            THROW_ERROR_EXCEPTION(
                "Cluster %Qv is not allowed to be an input remote cluster",
                clusterName)
                << TErrorAttribute("input_table_path", path);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
