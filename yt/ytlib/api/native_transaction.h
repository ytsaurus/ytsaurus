#pragma once

#include "native_client.h"
#include "transaction.h"

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct INativeTransaction
    : public INativeClientBase
    , public ITransaction
{
    virtual TFuture<ITransactionPtr> StartSlaveTransaction(
        IClientPtr client,
        const TTransactionStartOptions& options = TTransactionStartOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeTransaction)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

