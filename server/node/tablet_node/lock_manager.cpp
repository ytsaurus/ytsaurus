#include "lock_manager.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/serialize.h>

#include <yt/client/transaction_client/public.h>

#include <util/generic/map.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TLockManager::TImpl
    : public TRefCounted
{
public:
    void Lock(TTimestamp timestamp, TTransactionId transactionId)
    {
        LockCounter_++;
        YT_VERIFY(Transactions_.emplace(transactionId, timestamp).second);
        UnconfirmedTransactions_.push_back(transactionId);

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

    void Unlock(TTransactionId transactionId)
    {
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

    void Wait(TTimestamp timestamp)
    {
        if (timestamp == AsyncLastCommittedTimestamp ||
            timestamp == AllCommittedTimestamp)
        {
            return;
        }

        if (LockCounter_ > 0) {
            DoWait(timestamp);
        }
    }

    // FIXME(savrus) Need to check before write.
    bool IsLocked()
    {
        return LockCounter_ > 0;
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
    }

private:
    std::atomic<int> LockCounter_;
    THashMap<TTransactionId, TTimestamp> Transactions_;
    std::vector<TTransactionId> UnconfirmedTransactions_;

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
                NTabletClient::EErrorCode::QueryRetryRequested,
                "Query should be retried to use new tablet snapshot");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TLockManager::TLockManager()
    : Impl_(New<TImpl>())
{ }

void TLockManager::Lock(TTimestamp timestamp, TTransactionId transactionId)
{
    Impl_->Lock(timestamp, transactionId);
}

std::vector<TTransactionId> TLockManager::RemoveUnconfirmedTransactions()
{
    return Impl_->RemoveUnconfirmedTransactions();
}

void TLockManager::Unlock(TTransactionId transactionId)
{
    Impl_->Unlock(transactionId);
}

void TLockManager::Wait(TTimestamp timestamp)
{
    Impl_->Wait(timestamp);
}

bool TLockManager::IsLocked()
{
    return Impl_->IsLocked();
}

void TLockManager::Persist(const TStreamPersistenceContext& context)
{
    Impl_->Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

