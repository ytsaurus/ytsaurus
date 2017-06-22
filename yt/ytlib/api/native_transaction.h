#pragma once

#include "native_client.h"
#include "transaction.h"

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct TForeignTransactionStartOptions
    : public TTransactionStartOptions
{
    //! If |true| then the foreign transaction will use the start timestamp or its originator.
    //! If |false| then the foreign transaction will generate its own start timestamp.
    bool InheritStartTimestamp = true;
};

struct INativeTransaction
    : public INativeClientBase
    , public ITransaction
{
    virtual void AddAction(
        const NElection::TCellId& cellId,
        const NTransactionClient::TTransactionActionData& data) = 0;

    virtual TFuture<ITransactionPtr> StartForeignTransaction(
        const IClientPtr& client,
        const TForeignTransactionStartOptions& options = TForeignTransactionStartOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeTransaction)

INativeTransactionPtr CreateNativeTransaction(
    INativeClientPtr client,
    NTransactionClient::TTransactionPtr transaction,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

