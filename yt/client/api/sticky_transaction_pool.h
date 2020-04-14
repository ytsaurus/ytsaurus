#pragma once

#include "client.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct IStickyTransactionPool
    : public virtual TRefCounted
{
    //! Registers a transaction in the pool.
    virtual ITransactionPtr RegisterTransaction(ITransactionPtr transaction) = 0;
    //! Finds a transaction by id and renews its lease. Returns |nullptr| if transaction is not found.
    virtual ITransactionPtr GetTransactionAndRenewLease(NTransactionClient::TTransactionId transactionId) = 0;

    //! Finds a transaction by id and renews its lease. Throws if transaction is not found.
    ITransactionPtr GetTransactionAndRenewLeaseOrThrow(NTransactionClient::TTransactionId transactionId);
};

DEFINE_REFCOUNTED_TYPE(IStickyTransactionPool)

////////////////////////////////////////////////////////////////////////////////

IStickyTransactionPoolPtr CreateStickyTransactionPool(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
