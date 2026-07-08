#include "prelock_tracker.h"

#include "config.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constinit const auto& Logger = SequoiaServerLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPrelockTracker
    : public IPrelockTracker
    , public TMasterAutomatonPart
{
public:
    explicit TPrelockTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(
            bootstrap,
            EAutomatonThreadQueue::SequoiaTransactionService)
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "PrelockTracker.Values",
            BIND_NO_PROPAGATE(&TPrelockTracker::Save, Unretained(this)));
        RegisterLoader(
            "PrelockTracker.Values",
            BIND_NO_PROPAGATE(&TPrelockTracker::Load, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(
            BIND_NO_PROPAGATE(&TPrelockTracker::OnTransactionFinished, Unretained(this)));
        transactionManager->SubscribeTransactionAborted(
            BIND_NO_PROPAGATE(&TPrelockTracker::OnTransactionFinished, Unretained(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TPrelockTracker::OnDynamicConfigChanged, Unretained(this)));
    }

    void AcquirePrelockUnchecked(
        TTransaction* owningTransaction,
        TTransaction* lockingTransaction,
        TNodeId nodeId,
        const TLockRequest& lockRequest) override
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(HasMutationContext());
        YT_VERIFY(owningTransaction);

        if (lockRequest.Mode == ELockMode::None) {
            return;
        }

        if (!lockingTransaction && lockRequest.Mode == ELockMode::Snapshot) {
            YT_LOG_ALERT("Attempt to take %Qlv prelock without transaction (NodeId: %v, OwningTransactionId: %v)",
                ELockMode::Snapshot,
                nodeId,
                owningTransaction->GetId());
            return;
        }

        auto lockingTransactionId = GetObjectId(lockingTransaction);
        OwningTransactionToPrelocks_[owningTransaction->GetId()].emplace_back(
            TVersionedNodeId{nodeId, lockingTransactionId},
            lockRequest);

        auto prelockCount = ++LockingTransactionToPrelockCounts_[lockingTransactionId][std::pair(nodeId, lockRequest)];
        if (prelockCount == 1) {
            RegisterPrelock(lockingTransaction, nodeId, lockRequest);
        }

        YT_LOG_DEBUG("Prelock acquired (OwningTransaction: %v, LockingTransaction: %v, NodeId: %v, LockRequest: %v, PrelockCount: %v)",
            owningTransaction->GetId(),
            GetObjectId(lockingTransaction),
            nodeId,
            lockRequest,
            prelockCount);
    }

    TError CheckLock(
        TTransaction* lockingTransaction,
        TNodeId nodeId,
        const TLockRequest& lockRequest) const override
    {
        if (!IsEnabled()) {
            return TError{};
        }

        VerifyPersistentStateRead();

        if (lockRequest.Mode == ELockMode::Snapshot) {
            return CheckNewSnapshotPrelock(lockingTransaction, nodeId);
        } else {
            TError error;

            TVersionedNodeId versionedNodeId{nodeId, GetObjectId(lockingTransaction)};

            CheckExistingSnapshotPrelocks(&error, lockingTransaction, nodeId, lockRequest);
            CheckExistingExclusivePrelocks(&error, versionedNodeId, lockRequest);
            CheckExistingSharedPrelocks(&error, versionedNodeId, lockRequest);
            CheckExistingSharedKeyPrelocks(&error, versionedNodeId, lockRequest);

            return error;
        }
    }

private:
    // Each prelock is associated with two (not necessarily distinct)
    // transactions: an owning transaction and locking transaction.

    // Persistent fields.
    THashMap<TTransactionId, THashMap<std::pair<TNodeId, TLockRequest>, int>> LockingTransactionToPrelockCounts_;
    THashMap<TTransactionId, std::vector<std::pair<TVersionedNodeId, TLockRequest>>> OwningTransactionToPrelocks_;

    // Non-persistent but unambiguously restorable from persistent fields.
    THashMap<TNodeId, std::set<TTransactionId>> ExclusivePrelocks_;
    THashMap<TNodeId, std::map<TTransactionId, int>> SharedPrelocks_;
    THashMap<std::pair<TNodeId, std::string>, std::set<TTransactionId>> SharedChildKeyPrelocks_;
    THashMap<std::pair<TNodeId, std::string>, std::set<TTransactionId>> SharedAttributeKeyPrelocks_;
    THashSet<TVersionedNodeId> SnapshotPrelocks_;

    struct TPrebranch
    {
        int Shared = 0;
        int Exclusive = 0;

        ELockMode GetStrongestMode() const
        {
            return Exclusive > 0 ? ELockMode::Exclusive : ELockMode::Shared;
        }

        bool Empty() const
        {
            return Shared == 0 && Exclusive == 0;
        }
    };
    THashMap<TVersionedNodeId, TPrebranch> NonSnapshotPrebranches_;

    void ResetState()
    {
        LockingTransactionToPrelockCounts_.clear();
        OwningTransactionToPrelocks_.clear();

        ExclusivePrelocks_.clear();
        SharedPrelocks_.clear();
        SharedChildKeyPrelocks_.clear();
        SharedAttributeKeyPrelocks_.clear();
        SnapshotPrelocks_.clear();
        NonSnapshotPrebranches_.clear();
    }

    void Clear() override
    {
        ResetState();

        TCompositeAutomatonPart::Clear();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig)
    {
        auto enabled = IsEnabled();

        if (enabled != oldConfig->SequoiaManager->EnablePrelockTracker) {
            YT_LOG_INFO("Prelock tracker %v", enabled ? "enabled" : "disabled");

            if (!enabled) {
                ResetState();
            }
        }
    }

    void Save(NCellMaster::TSaveContext& context)
    {
        using NYT::Save;

        Save(context, LockingTransactionToPrelockCounts_);
        Save(context, OwningTransactionToPrelocks_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        Load(context, LockingTransactionToPrelockCounts_);
        Load(context, OwningTransactionToPrelocks_);
    }

    void OnAfterSnapshotLoaded() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        for (const auto& [lockingTransactionId, prelockCounts] : LockingTransactionToPrelockCounts_) {
            auto* lockingTransaction = lockingTransactionId
                ? transactionManager->GetTransaction(lockingTransactionId)
                : nullptr;

            for (const auto& prelockInfo : prelockCounts) {
                const auto& [nodeId, lockRequest] = prelockInfo.first;
                RegisterPrelock(lockingTransaction, nodeId, lockRequest);
            }
        }
    }

    bool IsEnabled() const
    {
        return Bootstrap_->GetDynamicConfig()->SequoiaManager->EnablePrelockTracker;
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(HasHydraContext());
        YT_VERIFY(transaction);

        YT_LOG_DEBUG("Droping prelocks (TransactionId: %v)", transaction->GetId());

        OnLockingTransactionFinished(transaction);
        OnOwningTransactionFinished(transaction);
    }

    void OnOwningTransactionFinished(TTransaction* owningTransaction)
    {
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(owningTransaction);

        std::vector<std::pair<TVersionedNodeId, TLockRequest>> prelocks;

        if (auto it = OwningTransactionToPrelocks_.find(owningTransaction->GetId());
            it != OwningTransactionToPrelocks_.end())
        {
            prelocks = std::move(it->second);
            OwningTransactionToPrelocks_.erase(it);
        }

        SortBy(prelocks, [] (const std::pair<TVersionedNodeId, TLockRequest>& prelock) {
            return prelock.first.TransactionId;
        });

        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        for (int i = 0, j = 0; i < std::ssize(prelocks); i = j) {
            auto lockingTransactionId = prelocks[i].first.TransactionId;

            do {
                j++;
            } while (j < std::ssize(prelocks) && prelocks[j].first.TransactionId == lockingTransactionId);

            auto it = LockingTransactionToPrelockCounts_.find(lockingTransactionId);
            if (it == LockingTransactionToPrelockCounts_.end()) {
                // It's OK for locking transaction to be not alive anymore.
                continue;
            }

            auto* lockingTransaction = lockingTransactionId
                ? transactionManager->GetTransaction(lockingTransactionId)
                : nullptr;

            auto& prelockCounts = it->second;
            YT_VERIFY(!prelockCounts.empty());

            for (const auto& [versionedNodeId, lockRequest] : TRange(&prelocks[i], j - i)) {
                auto prelockIt = GetIteratorOrCrash(prelockCounts, std::pair(versionedNodeId.ObjectId, lockRequest));
                auto prelockCount = --prelockIt->second;
                YT_LOG_DEBUG("Prelock released (OwningTransaction: %v, LockingTransaction: %v, NodeId: %v, LockRequest: %v, PrelockCount: %v)",
                    owningTransaction->GetId(),
                    lockingTransactionId,
                    versionedNodeId.ObjectId,
                    lockRequest,
                    prelockCount);
                if (prelockCount == 0) {
                    UnregisterPrelock(lockingTransaction, versionedNodeId.ObjectId, lockRequest);
                    prelockCounts.erase(prelockIt);
                }
            }

            if (prelockCounts.empty()) {
                LockingTransactionToPrelockCounts_.erase(it);
            }
        }
    }

    void OnLockingTransactionFinished(TTransaction* lockingTransaction)
    {
        YT_VERIFY(HasHydraContext());
        YT_VERIFY(lockingTransaction);

        auto it = LockingTransactionToPrelockCounts_.find(lockingTransaction->GetId());
        if (it != LockingTransactionToPrelockCounts_.end()) {
            for (const auto& [nodeIdWithLockRequest, count] : it->second) {
                const auto& [nodeId, lockRequest] = nodeIdWithLockRequest;
                UnregisterPrelock(lockingTransaction, nodeId, lockRequest);
            }
            LockingTransactionToPrelockCounts_.erase(it);
        }
    }

    void UnregisterPrelock(TTransaction* lockingTransaction, TNodeId nodeId, const TLockRequest& lockRequest)
    {
        YT_VERIFY(HasHydraContext());

        auto lockingTransactionId = GetObjectId(lockingTransaction);

        if (lockRequest.Mode == ELockMode::Snapshot) {
            EraseOrCrash(SnapshotPrelocks_, TVersionedNodeId{nodeId, lockingTransactionId});
        } else if (lockRequest.Mode == ELockMode::Exclusive) {
            auto it = GetIteratorOrCrash(ExclusivePrelocks_, nodeId);
            EraseOrCrash(it->second, lockingTransactionId);
            if (it->second.empty()) {
                ExclusivePrelocks_.erase(it);
            }
        } else if (lockRequest.Mode == ELockMode::Shared) {
            {
                auto nodeIt = GetIteratorOrCrash(SharedPrelocks_, nodeId);
                auto transactionIt = GetIteratorOrCrash(nodeIt->second, lockingTransactionId);
                if (--transactionIt->second == 0) {
                    nodeIt->second.erase(transactionIt);
                    if (nodeIt->second.empty()) {
                        SharedPrelocks_.erase(nodeIt);
                    }
                }
            }

            THashMap<std::pair<TNodeId, std::string>, std::set<TTransactionId>>* sharedKeyPrelocks = nullptr;
            if (lockRequest.Key.Kind == ELockKeyKind::Child) {
                sharedKeyPrelocks = &SharedChildKeyPrelocks_;
            } else if (lockRequest.Key.Kind == ELockKeyKind::Attribute) {
                sharedKeyPrelocks = &SharedAttributeKeyPrelocks_;
            }

            if (sharedKeyPrelocks) {
                auto nodeIt = GetIteratorOrCrash(*sharedKeyPrelocks, std::pair(nodeId, lockRequest.Key.Name));
                EraseOrCrash(nodeIt->second, lockingTransactionId);
                if (nodeIt->second.empty()) {
                    sharedKeyPrelocks->erase(nodeIt);
                }
            }
        }

        if (lockRequest.Mode != ELockMode::Snapshot) {
            UnregisterNonSnapshotPrebranches(lockingTransaction, nodeId, lockRequest.Mode);
        }

        YT_LOG_DEBUG("Prelock unregistered (NodeId: %v, LockingTransactionId: %v, LockRequest: %v)",
            nodeId,
            lockingTransactionId,
            lockRequest);
    }

    void RegisterPrelock(TTransaction* lockingTransaction, TNodeId nodeId, const TLockRequest& lockRequest)
    {
        YT_VERIFY(HasHydraContext());

        auto lockingTransactionId = GetObjectId(lockingTransaction);

        if (lockRequest.Mode == ELockMode::Snapshot) {
            InsertOrCrash(SnapshotPrelocks_, TVersionedNodeId{nodeId, lockingTransactionId});
        } else if (lockRequest.Mode == ELockMode::Exclusive) {
            InsertOrCrash(ExclusivePrelocks_[nodeId], lockingTransactionId);
        } else if (lockRequest.Mode == ELockMode::Shared) {
            ++SharedPrelocks_[nodeId][lockingTransactionId];
            if (lockRequest.Key.Kind == ELockKeyKind::Child) {
                InsertOrCrash(SharedChildKeyPrelocks_[std::pair(nodeId, lockRequest.Key.Name)], lockingTransactionId);
            } else if (lockRequest.Key.Kind == ELockKeyKind::Attribute) {
                InsertOrCrash(SharedAttributeKeyPrelocks_[std::pair(nodeId, lockRequest.Key.Name)], lockingTransactionId);
            }
        }

        if (lockRequest.Mode != ELockMode::Snapshot) {
            RegisterNonSnapshotPrebranches(lockingTransaction, nodeId, lockRequest.Mode);
        }

        YT_LOG_DEBUG("Prelock registered (NodeId: %v, LockingTransactionId: %v, LockRequest: %v)",
            nodeId,
            lockingTransactionId,
            lockRequest);
    }

    void RegisterNonSnapshotPrebranches(TTransaction* lockingTransaction, TNodeId nodeId, ELockMode lockMode)
    {
        YT_VERIFY(lockMode == ELockMode::Exclusive || lockMode == ELockMode::Shared);

        for (auto* transaction = lockingTransaction; transaction; transaction = transaction->GetParent()) {
            auto& prebranch = NonSnapshotPrebranches_[TVersionedNodeId{nodeId, transaction->GetId()}];
            if (lockMode == ELockMode::Shared) {
                ++prebranch.Shared;
            } else {
                ++prebranch.Exclusive;
            }
        }
    }

    void UnregisterNonSnapshotPrebranches(TTransaction* lockingTransaction, TNodeId nodeId, ELockMode lockMode)
    {
        YT_VERIFY(lockMode == ELockMode::Shared || lockMode == ELockMode::Exclusive);

        for (auto* transaction = lockingTransaction; transaction; transaction = transaction->GetParent()) {
            auto it = GetIteratorOrCrash(NonSnapshotPrebranches_, TVersionedNodeId{nodeId, transaction->GetId()});
            if (lockMode == ELockMode::Exclusive) {
                --it->second.Exclusive;
            } else {
                --it->second.Shared;
            }
            if (it->second.Empty()) {
                NonSnapshotPrebranches_.erase(it);
            }
        }
    }

    // NB: see TCypressManager::GetStrongestLockModeOfNestedTransactions.
    ELockMode FindNonSnapshotPrelockModeForTransactionSubtree(
        TNodeId nodeId,
        TTransaction* transaction) const
    {
        YT_VERIFY(transaction);

        auto doCheck = [&] (TTransaction* transactionToCheck) -> ELockMode {
            auto it = NonSnapshotPrebranches_.find(TVersionedNodeId{nodeId, transactionToCheck->GetId()});
            if (it != NonSnapshotPrebranches_.end()) {
                YT_VERIFY(!it->second.Empty());
                return it->second.GetStrongestMode();
            }

            return ELockMode::None;
        };

        auto strongestLockMode = doCheck(transaction);

        for (const auto& nestedTransaction : transaction->NestedTransactions()) {
            if (strongestLockMode == ELockMode::Exclusive) {
                break;
            }

            strongestLockMode = std::max(strongestLockMode, doCheck(nestedTransaction));
        }

        return strongestLockMode;
    }

    TError CheckNewSnapshotPrelock(TTransaction* transaction, TNodeId nodeId) const
    {
        if (!transaction) {
            return TError{};
        }

        auto lockMode = FindNonSnapshotPrelockModeForTransactionSubtree(nodeId, transaction);
        if (lockMode != ELockMode::None) {
            return TError(
                NSequoiaClient::EErrorCode::SequoiaRetriableError,
                "Cannot take %Qlv lock for node %v since %Qlv lock is being taken by transaction %v or its descendant",
                ELockMode::Snapshot,
                TVersionedNodeId{nodeId, GetObjectId(transaction)},
                lockMode,
                transaction->GetId());
        }

        return TError{};
    }

    void CheckExistingSnapshotPrelocks(
        TError* error,
        TTransaction* lockingTransaction,
        TNodeId nodeId,
        const TLockRequest& lockRequest) const
    {
        if (!error->IsOK()) {
            return;
        }

        // NB: snapshot prelock cannot be taken outside of transaction.

        for (
            auto* transaction = lockingTransaction;
            transaction;
            transaction = transaction->GetParent())
        {
            if (SnapshotPrelocks_.contains(TVersionedNodeId{nodeId, transaction->GetId()})) {
                *error = TError(
                    NSequoiaClient::EErrorCode::SequoiaRetriableError,
                    "Cannot take %Qlv lock for node %v since %Qlv lock is being taken by ancestor transaction %v",
                    lockRequest.Mode,
                    TVersionedNodeId{nodeId, GetObjectId(transaction)},
                    ELockMode::Snapshot,
                    transaction->GetId());
                break;
            }
        }
    }

    void CheckExistingExclusivePrelocks(
        TError* error,
        TVersionedNodeId nodeId,
        const TLockRequest& lockRequest) const
    {
        DoCheckExistingPrelocks(error, nodeId, lockRequest, nodeId.ObjectId, ExclusivePrelocks_, ELockMode::Exclusive);
    }

    void CheckExistingSharedPrelocks(
        TError* error,
        TVersionedNodeId nodeId,
        const TLockRequest& lockRequest) const
    {
        if (lockRequest.Mode != ELockMode::Exclusive) {
            // Only exclusive locks conflict with every shared lock.
            return;
        }

        DoCheckExistingPrelocks(error, nodeId, lockRequest, nodeId.ObjectId, SharedPrelocks_, ELockMode::Shared);
    }

    void CheckExistingSharedKeyPrelocks(
        TError* error,
        TVersionedNodeId nodeId,
        const TLockRequest& lockRequest) const
    {
        if (lockRequest.Mode != ELockMode::Shared) {
            // NB: exclusive locks are checked in CheckExistingSharedPrelocks().
            return;
        }

        switch (lockRequest.Key.Kind) {
            case ELockKeyKind::None:
                return;
            case ELockKeyKind::Child:
                DoCheckExistingPrelocks(
                    error,
                    nodeId,
                    lockRequest,
                    std::pair{nodeId.ObjectId, lockRequest.Key.Name},
                    SharedChildKeyPrelocks_,
                    ELockMode::Shared);
                return;
            case ELockKeyKind::Attribute:
                DoCheckExistingPrelocks(
                    error,
                    nodeId,
                    lockRequest,
                    std::pair(nodeId.ObjectId, lockRequest.Key.Name),
                    SharedAttributeKeyPrelocks_,
                    ELockMode::Shared);
                return;
        }
    }

    template <class TKey, class TTransactionSet>
    void DoCheckExistingPrelocks(
        TError* error,
        TVersionedNodeId nodeId,
        const TLockRequest& lockRequest,
        const TKey& key,
        const THashMap<TKey, TTransactionSet>& existingLocks,
        ELockMode existingLockMode) const
    {
        if (!error->IsOK()) {
            return;
        }

        auto transactionsIt = existingLocks.find(key);
        if (transactionsIt == existingLocks.end()) {
            return;
        }

        const auto& transactions = transactionsIt->second;
        YT_VERIFY(!transactions.empty());

        // NB: not only concurrent transactions have to be checked since
        // prelocks don't have any information about lock acquisition order.
        auto it = transactions.begin();

        auto getTransactionId = [] (const auto& idOrPair) -> TTransactionId {
            if constexpr (requires {{idOrPair.first};}) {
                return idOrPair.first;
            } else {
                return idOrPair;
            }
        };

        if (getTransactionId(*it) == nodeId.TransactionId) {
            it = std::next(it);
        }

        if (it == transactions.end()) {
            return;
        }

        auto existingTransactionId = getTransactionId(*it);
        switch (lockRequest.Key.Kind) {
            case ELockKeyKind::None:
                *error = TError(
                    NSequoiaClient::EErrorCode::SequoiaRetriableError,
                    "Cannot take %Qlv lock for node %v since %Qlv lock is being taken by transaction %v",
                    lockRequest.Mode,
                    nodeId,
                    existingLockMode,
                    existingTransactionId);
                return;
            case ELockKeyKind::Child:
                *error = TError(
                    NSequoiaClient::EErrorCode::SequoiaRetriableError,
                    "Cannot take lock for child %Qv of node %v since this child is being locked by transaction %v",
                    lockRequest.Key.Name,
                    nodeId,
                    existingTransactionId);
                return;
            case ELockKeyKind::Attribute:
                *error = TError(
                    NSequoiaClient::EErrorCode::SequoiaRetriableError,
                    "Cannot take lock for attribute %Qv of node %v since this attribute is being locked by transaction %v",
                    lockRequest.Key.Name,
                    nodeId,
                    existingTransactionId);
                return;
        }
    }
};

IPrelockTrackerPtr CreatePrelockTracker(NCellMaster::TBootstrap* bootstrap)
{
    return New<TPrelockTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
