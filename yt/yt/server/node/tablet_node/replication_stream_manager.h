#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Not thread-safe; callers must serialize access.
class TReplicationStreamManager
{
public:
    TError TryAcquireFence(
        NTabletClient::TReplicationStreamId replicationStreamId,
        TTransactionId transactionId);

    void VerifyFence(
        NTabletClient::TReplicationStreamId replicationStreamId,
        TTransactionId transactionId) const;

    TTransactionId FindFence(
        NTabletClient::TReplicationStreamId replicationStreamId) const;

    NTabletClient::TReplicationStreamId FindStream(TTransactionId transactionId) const;

    void ReleaseFence(TTransactionId transactionId);

    void Clear();

private:
    THashMap<NTabletClient::TReplicationStreamId, TTransactionId> ReplicationStreamToTransaction_;
    THashMap<TTransactionId, NTabletClient::TReplicationStreamId> TransactionToReplicationStream_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
