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
    virtual void AddAction(
        const NElection::TCellId& cellId,
        const NTransactionClient::TTransactionActionData& data) = 0;

    virtual TFuture<ITransactionPtr> StartForeignTransaction(
        const IClientPtr& client,
        const TTransactionStartOptions& options = TTransactionStartOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeTransaction)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

