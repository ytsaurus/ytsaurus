#pragma once

#include "public.h"

#include <yt/client/transaction_client/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionParticipantState,
    (Valid)
    (Invalid)
    (Unregistered)
);

struct ITransactionParticipant
    : public virtual TRefCounted
{
    virtual TCellId GetCellId() const = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() const = 0;

    virtual ETransactionParticipantState GetState() const = 0;

    virtual TFuture<void> PrepareTransaction(TTransactionId transactionId, TTimestamp prepareTimestamp, const TString& user) = 0;
    virtual TFuture<void> CommitTransaction(TTransactionId transactionId, TTimestamp commitTimestamp) = 0;
    virtual TFuture<void> AbortTransaction(TTransactionId transactionId) = 0;

    virtual TFuture<void> CheckAvailability() = 0;
    // COMPAT(savrus) Compatibility with pre 19.6 participants.
    virtual TFuture<void> CheckAvailabilityPre196() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionParticipant)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
