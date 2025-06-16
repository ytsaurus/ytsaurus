#include "replication_card_updates_batcher_serialization.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TReplicationCardProgressUpdate* replicationCardProgressUpdate,
    const NChaosClient::NProto::TReplicationCardProgressUpdateItem& protoReplicationCardProgressUpdate)
{
    replicationCardProgressUpdate->ReplicationCardId = NYT::FromProto<TReplicationCardId>(
        protoReplicationCardProgressUpdate.replication_card_id());

    replicationCardProgressUpdate->FetchOptions = protoReplicationCardProgressUpdate.has_fetch_options()
        ? std::make_optional(NYT::FromProto<TReplicationCardFetchOptions>(protoReplicationCardProgressUpdate.fetch_options()))
        : std::nullopt;


    const auto& protoReplicaProgressUpdates = protoReplicationCardProgressUpdate.replica_progress_updates();
    auto& replicaProgressUpdates = replicationCardProgressUpdate->ReplicaProgressUpdates;
    replicaProgressUpdates.reserve(protoReplicaProgressUpdates.size());

    for (const auto& protoReplicaProgressUpdate : protoReplicaProgressUpdates) {
        auto& replicaProgressUpdate = replicaProgressUpdates.emplace_back();

        replicaProgressUpdate.ReplicaId = NYT::FromProto<TReplicaId>(protoReplicaProgressUpdate.replica_id());
        replicaProgressUpdate.ReplicationProgressUpdate =
            NYT::FromProto<TReplicationProgress>(protoReplicaProgressUpdate.replication_progress());
    }
}

void ToProto(
    NChaosClient::NProto::TReplicationCardProgressUpdateItem* protoReplicationCardProgressUpdate,
    const TReplicationCardProgressUpdate& replicationCardProgressUpdate)
{
    ToProto(
        protoReplicationCardProgressUpdate->mutable_replication_card_id(),
        replicationCardProgressUpdate.ReplicationCardId);

    protoReplicationCardProgressUpdate->mutable_replica_progress_updates()->Reserve(
        replicationCardProgressUpdate.ReplicaProgressUpdates.size());
    for (const auto& replicaProgressUpdate : replicationCardProgressUpdate.ReplicaProgressUpdates) {
        auto* protoReplicaProgressUpdate = protoReplicationCardProgressUpdate->add_replica_progress_updates();
        ToProto(
            protoReplicaProgressUpdate->mutable_replica_id(),
            replicaProgressUpdate.ReplicaId);
        ToProto(
            protoReplicaProgressUpdate->mutable_replication_progress(),
            replicaProgressUpdate.ReplicationProgressUpdate);
    }

    if (replicationCardProgressUpdate.FetchOptions) {
        ToProto(
            protoReplicationCardProgressUpdate->mutable_fetch_options(),
            *replicationCardProgressUpdate.FetchOptions);
    }

}

void FromProto(
    TReplicationCardProgressUpdatesBatch* batch,
    const NChaosClient::NProto::TReqUpdateMultipleTableProgresses& protoBatch)
{
    const auto& replicationCardProgressUpdates = protoBatch.replication_card_progress_updates();
    batch->ReplicationCardProgressUpdates.reserve(replicationCardProgressUpdates.size());

    for (const auto& protoReplicationCardProgressUpdate : replicationCardProgressUpdates) {
        batch->ReplicationCardProgressUpdates.emplace_back(::NYT::FromProto<TReplicationCardProgressUpdate>(
            protoReplicationCardProgressUpdate));
    }
}

TReplicationCardPtr FromProto(const NProto::TRspUpdateTableProgress& protoResponse)
{
    if (protoResponse.has_replication_card()) {
        auto replicationCard = New<TReplicationCard>();
        FromProto(replicationCard.Get(), protoResponse.replication_card());
        return replicationCard;
    } else {
        return TReplicationCardPtr();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
