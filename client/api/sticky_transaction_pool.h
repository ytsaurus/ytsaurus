#pragma once

#include "client.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct IStickyTransactionPool
    : public virtual TRefCounted
{
    virtual ITransactionPtr RegisterTransaction(ITransactionPtr transaction) = 0;
    virtual ITransactionPtr GetTransactionAndRenewLease(NTransactionClient::TTransactionId transactionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IStickyTransactionPool)

////////////////////////////////////////////////////////////////////////////////

IStickyTransactionPoolPtr CreateStickyTransactionPool(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
