#include "lock_manager.h"
#include "serialize.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/client/transaction_client/public.h>

#include <util/generic/cast.h>

#include <map>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TLockManager::TImpl
    : public TRefCounted
{
public:
    void Lock(TTimestamp timestamp, TTransactionId transactionId, bool confirmed)
    {
        LockCounter_++;
        YT_VERIFY(Transactions_.emplace(transactionId, timestamp).second);
        if (!confirmed) {
            UnconfirmedTransactionIds_.push_back(transactionId);
        }

        {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(SharedQueue_.emplace(timestamp, NewPromise<void>()).second);
        }
    }

    void Unlock(TTimestamp commitTimestamp, TTransactionId transactionId)
    {
        ++Epoch_;
        LastCommitTimestamp_ = std::max(LastCommitTimestamp_, commitTimestamp);

        if (auto it = Transactions_.find(transactionId)) {
            auto timestamp = it->second;
            Transactions_.erase(it);
            LockCounter_--;

            {
                auto guard = Guard(SpinLock_);

                auto it = SharedQueue_.find(timestamp);
                YT_VERIFY(it != SharedQueue_.end());
                it->second.Set();
                SharedQueue_.erase(it);
            }
        }
    }

    bool HasTransaction(TTransactionId transactionId) const
    {
        return Transactions_.contains(transactionId);
    }

    std::vector<TTransactionId> ExtractUnconfirmedTransactionIds()
    {
        auto result = std::move(UnconfirmedTransactionIds_);
        UnconfirmedTransactionIds_.clear();
        return result;
    }

    bool HasUnconfirmedTransactions() const
    {
        return !UnconfirmedTransactionIds_.empty();
    }

    TLockManagerEpoch GetEpoch() const
    {
        return Epoch_.load();
    }

    void Wait(TTimestamp timestamp, TLockManagerEpoch epoch)
    {
        if (epoch < GetEpoch()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::TabletSnapshotExpired,
                "Query should be retried to use new tablet snapshot");
        }

        if (LockCounter_ > 0) {
            DoWait(timestamp);
        }
    }

    TError ValidateTransactionConflict(TTimestamp startTimestamp) const
    {
        if (LockCounter_ > 0) {
            return TError("Tablet is locked by bulk insert");
        }

        if (LastCommitTimestamp_ > startTimestamp) {
            return TError("Lock conflict due to concurrent bulk insert")
                << TErrorAttribute("transaction_start_timestamp", startTimestamp)
                << TErrorAttribute("bulk_insert_commit_timestamp", LastCommitTimestamp_);
        }

        return {};
    }

    void BuildOrchidYson(NYTree::TFluentMap fluent) const
    {
        THashSet<TTransactionId> unconfirmedTransactionsSet(
            UnconfirmedTransactionIds_.begin(),
            UnconfirmedTransactionIds_.end());

        fluent
            .DoFor(
                Transactions_,
                [&] (TFluentMap fluent, const std::pair<TTransactionId, TTimestamp>& pair) {
                    fluent
                        .Item(ToString(pair.first)).BeginMap()
                            .Item("timestamp").Value(pair.second)
                            .Item("confirmed").Value(!unconfirmedTransactionsSet.contains(pair.first))
                        .EndMap();
                });
    }

    void Persist(const TStreamPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, LockCounter_);
        Persist(context, Transactions_);
        Persist(context, UnconfirmedTransactionIds_);
        Persist(context, LastCommitTimestamp_);

        if (context.IsLoad()) {
            SharedQueue_.clear();
            for (auto [transactionId, timestamp] : Transactions_) {
                SharedQueue_.emplace(timestamp, NewPromise<void>());
            }
        }
    }

private:
    std::atomic<int> LockCounter_ = 0;
    std::atomic<TLockManagerEpoch> Epoch_ = 0;
    THashMap<TTransactionId, TTimestamp> Transactions_;
    std::vector<TTransactionId> UnconfirmedTransactionIds_;
    TTimestamp LastCommitTimestamp_ = MinTimestamp;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::map<TTimestamp, TPromise<void>> SharedQueue_;


    void DoWait(TTimestamp timestamp)
    {
        TFuture<void> future;
        bool waited = false;
        do {
            future.Reset();

            {
                auto guard = Guard(SpinLock_);

                if (auto it = SharedQueue_.begin(); !SharedQueue_.empty() && it->first < timestamp) {
                    future = it->second;
                }
            }

            if (future) {
                WaitFor(future)
                    .ThrowOnError();
                waited = true;
            }
        } while (future);

        if (waited) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::TabletSnapshotExpired,
                "Query should be retried to use new tablet snapshot");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TLockManager::TLockManager()
    : Impl_(New<TImpl>())
{ }

TLockManager::~TLockManager() = default;

void TLockManager::Lock(TTimestamp timestamp, TTransactionId transactionId, bool confirmed)
{
    Impl_->Lock(timestamp, transactionId, confirmed);
}

void TLockManager::Unlock(TTimestamp commitTimestamp, TTransactionId transactionId)
{
    Impl_->Unlock(commitTimestamp, transactionId);
}

std::vector<TTransactionId> TLockManager::ExtractUnconfirmedTransactionIds()
{
    return Impl_->ExtractUnconfirmedTransactionIds();
}

bool TLockManager::HasUnconfirmedTransactions() const
{
    return Impl_->HasUnconfirmedTransactions();
}

TLockManagerEpoch TLockManager::GetEpoch() const
{
    return Impl_->GetEpoch();
}

bool TLockManager::HasTransaction(TTransactionId transactionId) const
{
    return Impl_->HasTransaction(transactionId);
}

void TLockManager::Wait(TTimestamp timestamp, TLockManagerEpoch epoch)
{
    Impl_->Wait(timestamp, epoch);
}

TError TLockManager::ValidateTransactionConflict(TTimestamp startTimestamp) const
{
    return Impl_->ValidateTransactionConflict(startTimestamp);
}

void TLockManager::BuildOrchidYson(NYTree::TFluentMap fluent) const
{
    Impl_->BuildOrchidYson(fluent);
}

void TLockManager::Persist(const TStreamPersistenceContext& context)
{
    Impl_->Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

