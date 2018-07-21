#pragma once

#include "client.h"

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

struct TForeignTransactionStartOptions
    : public TTransactionStartOptions
{
    //! If |true| then the foreign transaction will use the start timestamp or its originator.
    //! If |false| then the foreign transaction will generate its own start timestamp.
    bool InheritStartTimestamp = true;
};

struct ITransaction
    : public IClientBase
    , public NApi::ITransaction
{
    virtual void AddAction(
        const NElection::TCellId& cellId,
        const NTransactionClient::TTransactionActionData& data) = 0;

    virtual TFuture<NApi::ITransactionPtr> StartForeignTransaction(
        const NApi::IClientPtr& client,
        const TForeignTransactionStartOptions& options = TForeignTransactionStartOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransaction)

ITransactionPtr CreateTransaction(
    IClientPtr client,
    NTransactionClient::TTransactionPtr transaction,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT

