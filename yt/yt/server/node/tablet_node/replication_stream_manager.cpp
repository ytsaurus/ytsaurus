#include "replication_stream_manager.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NTabletNode {

using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TError TReplicationStreamManager::TryAcquireFence(
    TReplicationStreamId replicationStreamId,
    TTransactionId transactionId)
{
    if (!replicationStreamId) {
        return {};
    }

    YT_VERIFY(transactionId);

    const auto [fenceIt, fenceInserted] = ReplicationStreamToTransaction_.emplace(
        replicationStreamId,
        transactionId);

    if (!fenceInserted) {
        if (fenceIt->second == transactionId) {
            // Transaction may acquire the same fence repeatedly in case of write retries.
            return {};
        }

        return TError(
            NTabletClient::EErrorCode::ReplicationFenceConflict,
            "Cannot acquire replication fence for stream %v by transaction %v "
            "since it is already acquired by transaction %v",
            replicationStreamId,
            transactionId,
            fenceIt->second)
            .With("replication_stream_id", replicationStreamId)
            .With("existing_transaction_id", fenceIt->second)
            .With("transaction_id", transactionId);
    }

    const auto [streamIt, streamInserted] = TransactionToReplicationStream_.emplace(
        transactionId,
        replicationStreamId);

    if (streamInserted) {
        return {};
    }

    YT_VERIFY(streamIt->second != replicationStreamId);

    ReplicationStreamToTransaction_.erase(fenceIt);

    return TError(
        "Cannot have two different replication streams in transaction %v",
        transactionId)
        .With("transaction_id", transactionId)
        .With("existing_replication_stream_id", streamIt->second)
        .With("replication_stream_id", replicationStreamId);
}

void TReplicationStreamManager::VerifyFence(
    TReplicationStreamId replicationStreamId,
    TTransactionId transactionId) const
{
    if (!replicationStreamId) {
        return;
    }

    YT_VERIFY(GetOrCrash(ReplicationStreamToTransaction_, replicationStreamId) == transactionId);
}

TTransactionId TReplicationStreamManager::FindFence(
    TReplicationStreamId replicationStreamId) const
{
    return GetOrDefault(ReplicationStreamToTransaction_, replicationStreamId);
}

TReplicationStreamId TReplicationStreamManager::FindStream(
    TTransactionId transactionId) const
{
    return GetOrDefault(TransactionToReplicationStream_, transactionId);
}

void TReplicationStreamManager::ReleaseFence(TTransactionId transactionId)
{
    YT_VERIFY(transactionId);

    auto streamIt = TransactionToReplicationStream_.find(transactionId);
    if (streamIt == TransactionToReplicationStream_.end()) {
        return;
    }

    auto fenceIt = GetIteratorOrCrash(ReplicationStreamToTransaction_, streamIt->second);
    YT_VERIFY(fenceIt->second == transactionId);

    ReplicationStreamToTransaction_.erase(fenceIt);
    TransactionToReplicationStream_.erase(streamIt);
}

void TReplicationStreamManager::Clear()
{
    ReplicationStreamToTransaction_.clear();
    TransactionToReplicationStream_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
