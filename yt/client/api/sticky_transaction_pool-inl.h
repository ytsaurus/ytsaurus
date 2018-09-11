#pragma once
#ifndef STICKY_TRANSACTION_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_pool.h"
#endif
#undef STICKY_TRANSACTION_POOL_INL_H_

#include "transaction.h"

#include <yt/core/concurrency/lease_manager.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

template <class TTransactionPtr>
TTransactionPtr TStickyTransactionPool<TTransactionPtr>::RegisterTransaction(
    TTransactionPtr transaction)
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

    transaction->SubscribeCommitted(BIND(&TStickyTransactionPool::OnStickyTransactionFinished, MakeWeak(this), transactionId));
    transaction->SubscribeAborted(BIND(&TStickyTransactionPool::OnStickyTransactionFinished, MakeWeak(this), transactionId));

    LOG_DEBUG("Sticky transaction registered (TransactionId: %v)",
        transactionId);

    return transaction;
}

template <class TTransactionPtr>
TTransactionPtr TStickyTransactionPool<TTransactionPtr>::GetTransactionAndRenewLease(
    const NTransactionClient::TTransactionId& transactionId)
{
    TTransactionPtr transaction;
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

template <class TTransactionPtr>
void TStickyTransactionPool<TTransactionPtr>::OnStickyTransactionLeaseExpired(
    const NTransactionClient::TTransactionId& transactionId)
{
    TTransactionPtr transaction;
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

template <class TTransactionPtr>
void TStickyTransactionPool<TTransactionPtr>::OnStickyTransactionFinished(
    const NTransactionClient::TTransactionId& transactionId)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
