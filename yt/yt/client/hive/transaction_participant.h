#pragma once

#include "public.h"

#include <yt/client/transaction_client/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionParticipantState,
    // Participant is available for interaction; this does not imply any partcular health status.
    (Valid)
    // Cluster connection was terminated, participant is no longer usable.
    (Invalidated)
    // Participant is not known; e.g. cluster is not registered.
    (NotRegistered)
    // Participant is known to be unregistered and will never be resurrected.
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
};

DEFINE_REFCOUNTED_TYPE(ITransactionParticipant)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
