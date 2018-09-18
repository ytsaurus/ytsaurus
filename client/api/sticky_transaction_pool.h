#pragma once

#include "client.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

template <class TTransactionPtr>
struct IStickyTransactionPool
    : public virtual TRefCounted
{
    virtual TTransactionPtr RegisterTransaction(TTransactionPtr transaction) = 0;
    virtual TTransactionPtr GetTransactionAndRenewLease(const NTransactionClient::TTransactionId& transactionId) = 0;
};

template <class TTransactionPtr>
using IStickyTransactionPoolPtr = TIntrusivePtr<IStickyTransactionPool<TTransactionPtr>>;

////////////////////////////////////////////////////////////////////////////////

template <class TTransactionPtr>
class TStickyTransactionPool
    : public IStickyTransactionPool<TTransactionPtr>
{
public:
    explicit TStickyTransactionPool(const NLogging::TLogger& logger):
        Logger(logger)
    {}

    virtual TTransactionPtr RegisterTransaction(TTransactionPtr transaction) override;
    virtual TTransactionPtr GetTransactionAndRenewLease(const NTransactionClient::TTransactionId& transactionId) override;

private:
    struct TStickyTransactionEntry
    {
        TTransactionPtr Transaction;
        NConcurrency::TLease Lease;
    };

    NConcurrency::TReaderWriterSpinLock StickyTransactionLock_;
    THashMap<NTransactionClient::TTransactionId, TStickyTransactionEntry> IdToStickyTransactionEntry_;

    const NLogging::TLogger& Logger;

    void OnStickyTransactionLeaseExpired(const NTransactionClient::TTransactionId& transactionId);
    void OnStickyTransactionFinished(const NTransactionClient::TTransactionId& transactionId);
};

template <class TTransactionPtr>
IStickyTransactionPoolPtr<TTransactionPtr> CreateStickyTransactionPool(
    const NLogging::TLogger& logger)
{
    return New<TStickyTransactionPool<TTransactionPtr>>(logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

#define STICKY_TRANSACTION_POOL_INL_H_
#include "sticky_transaction_pool-inl.h"
#undef STICKY_TRANSACTION_POOL_INL_H_
