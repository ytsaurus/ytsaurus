#pragma once

#include "public.h"

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionFinisherHost
    : public virtual TRefCounted
{
    virtual TFuture<void> MaybeCommitTransactionWithoutLeaseRevocation(
        TTransaction* transaction,
        NRpc::TMutationId mutationId,
        const NRpc::TAuthenticationIdentity& identity,
        const std::vector<TTransactionId>& prerequisiteTransactionIds) = 0;
    virtual TFuture<void> MaybeAbortTransactionWithoutLeaseRevocation(
        TTransaction* transaction,
        NRpc::TMutationId mutationId,
        const NRpc::TAuthenticationIdentity& identity,
        bool force) = 0;
    virtual TFuture<void> MaybeAbortExpiredTransactionWithoutLeaseRevocation(
        TTransaction* transaction) = 0;
    virtual TFuture<void> RevokeLeasesForExpiredTransaction(TTransaction* transaction) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionFinisherHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
