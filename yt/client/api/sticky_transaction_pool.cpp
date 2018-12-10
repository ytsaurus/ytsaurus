#include "sticky_transaction_pool.h"

#include "transaction.h"
#include <yt/core/concurrency/lease_manager.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TStickyTransactionPool
    : public IStickyTransactionPool
{
public:
    explicit TStickyTransactionPool(const NLogging::TLogger& logger):
        Logger(logger)
    {}

    virtual ITransactionPtr RegisterTransaction(ITransactionPtr transaction) override
    {
        const auto& transactionId = transaction->GetId();
        TStickyTransactionEntry entry{
            transaction,
            NConcurrency::TLeaseManager::CreateLease(
                transaction->GetTimeout(),
                BIND(&TStickyTransactionPool::OnStickyTransactionLeaseExpired, MakeWeak(this), transactionId))
        };

        {
            NConcurrency::TWriterGuard guard(StickyTransactionLock_);
            YCHECK(IdToStickyTransactionEntry_.emplace(transactionId, entry).second);
        }

        transaction->SubscribeCommitted(
            BIND(&TStickyTransactionPool::OnStickyTransactionFinished, MakeWeak(this), transactionId));
        transaction->SubscribeAborted(
            BIND(&TStickyTransactionPool::OnStickyTransactionFinished, MakeWeak(this), transactionId));

        LOG_DEBUG("Sticky transaction registered (TransactionId: %v)",
            transactionId);

        return transaction;
    }

    virtual ITransactionPtr GetTransactionAndRenewLease(
        const NTransactionClient::TTransactionId& transactionId) override
    {
        ITransactionPtr transaction;
        NConcurrency::TLease lease;
        {
            NConcurrency::TReaderGuard guard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                THROW_ERROR_EXCEPTION(
                    NTransactionClient::EErrorCode::NoSuchTransaction,
                    "Sticky transaction %v is not found",
                    transactionId);
            }
            const auto& entry = it->second;
            transaction = entry.Transaction;
            lease = entry.Lease;
        }
        NConcurrency::TLeaseManager::RenewLease(lease);
        LOG_DEBUG("Sticky transaction lease renewed (TransactionId: %v)",
            transactionId);
        return transaction;
    }

private:
    struct TStickyTransactionEntry
    {
        ITransactionPtr Transaction;
        NConcurrency::TLease Lease;
    };

    NConcurrency::TReaderWriterSpinLock StickyTransactionLock_;
    THashMap<NTransactionClient::TTransactionId, TStickyTransactionEntry> IdToStickyTransactionEntry_;

    const NLogging::TLogger& Logger;

    void OnStickyTransactionLeaseExpired(const NTransactionClient::TTransactionId& transactionId)
    {
        ITransactionPtr transaction;
        {
            NConcurrency::TWriterGuard guard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return;
            }
            transaction = it->second.Transaction;
            IdToStickyTransactionEntry_.erase(it);
        }

        LOG_DEBUG("Sticky transaction lease expired (TransactionId: %v)",
            transactionId);

        transaction->Abort();
    }
    void OnStickyTransactionFinished(const NTransactionClient::TTransactionId& transactionId)
    {
        NConcurrency::TLease lease;
        {
            NConcurrency::TWriterGuard guard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return;
            }
            lease = it->second.Lease;
            IdToStickyTransactionEntry_.erase(it);
        }

        LOG_DEBUG("Sticky transaction unregistered (TransactionId: %v)",
            transactionId);

        NConcurrency::TLeaseManager::CloseLease(lease);
    }
};

////////////////////////////////////////////////////////////////////////////////

IStickyTransactionPoolPtr CreateStickyTransactionPool(
    const NLogging::TLogger& logger)
{
    return New<TStickyTransactionPool>(logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
