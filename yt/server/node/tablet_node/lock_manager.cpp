#include "lock_manager.h"
#include "serialize.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/ytree/fluent.h>

#include <yt/client/transaction_client/public.h>

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
            UnconfirmedTransactions_.push_back(transactionId);
        }

        {
            TGuard guard(SpinLock_);
            YT_VERIFY(SharedQueue_.emplace(timestamp, NewPromise<void>()).second);
        }
    }

    std::vector<TTransactionId> RemoveUnconfirmedTransactions()
    {
        auto result = std::move(UnconfirmedTransactions_);
        UnconfirmedTransactions_.clear();
        return result;
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
                TGuard guard(SpinLock_);

                auto it = SharedQueue_.find(timestamp);
                YT_VERIFY(it != SharedQueue_.end());
                it->second.Set();
                SharedQueue_.erase(it);
            }
        }
    }

    TLockManagerEpoch GetEpoch() const
    {
        return Epoch_.load();
    }

    void Wait(TTimestamp timestamp, TLockManagerEpoch epoch)
    {
        if (timestamp == AsyncLastCommittedTimestamp ||
            timestamp == AllCommittedTimestamp)
        {
            return;
        }

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
            UnconfirmedTransactions_.begin(),
            UnconfirmedTransactions_.end());

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
        Persist(context, UnconfirmedTransactions_);

        if (context.IsLoad()) {
            SharedQueue_.clear();
            for (const auto& pair : Transactions_) {
                SharedQueue_.emplace(pair.second, NewPromise<void>());
            }
        }

        if (context.GetVersion() >= ToUnderlying(ETabletReign::BulkInsertOverwrite)) {
            Persist(context, LastCommitTimestamp_);
        }
    }

private:
    std::atomic<int> LockCounter_;
    std::atomic<TLockManagerEpoch> Epoch_;
    THashMap<TTransactionId, TTimestamp> Transactions_;
    std::vector<TTransactionId> UnconfirmedTransactions_;
    TTimestamp LastCommitTimestamp_ = MinTimestamp;

    TSpinLock SpinLock_;
    std::map<TTimestamp, TPromise<void>> SharedQueue_;


    void DoWait(TTimestamp timestamp)
    {
        TFuture<void> future;
        bool waited = false;
        do {
            future.Reset();

            {
                TGuard guard(SpinLock_);

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

TLockManager::~TLockManager()
{ }

void TLockManager::Lock(TTimestamp timestamp, TTransactionId transactionId, bool confirmed)
{
    Impl_->Lock(timestamp, transactionId, confirmed);
}

std::vector<TTransactionId> TLockManager::RemoveUnconfirmedTransactions()
{
    return Impl_->RemoveUnconfirmedTransactions();
}

void TLockManager::Unlock(TTimestamp commitTimestamp, TTransactionId transactionId)
{
    Impl_->Unlock(commitTimestamp, transactionId);
}

TLockManagerEpoch TLockManager::GetEpoch() const
{
    return Impl_->GetEpoch();
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

