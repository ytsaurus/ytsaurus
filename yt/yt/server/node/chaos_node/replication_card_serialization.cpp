#include "replication_card.h"
#include "replication_card_collocation.h"
#include "serialize.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT {

using namespace NChaosClient;
using namespace NChaosNode;
using namespace NElection;
using namespace NYson;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

template <>
void Save(NChaosNode::TSaveContext& context, const TReplicaInfo& replicaInfo)
{
    using NYT::Save;

    Save(context, replicaInfo.ClusterName);
    Save(context, replicaInfo.ReplicaPath);
    Save(context, replicaInfo.ContentType);
    Save(context, replicaInfo.Mode);
    Save(context, replicaInfo.State);
    Save(context, replicaInfo.History);
    Save(context, replicaInfo.ReplicationProgress);
    Save(context, replicaInfo.EnableReplicatedTableTracker);

}

template <>
void Load<TReplicaInfo, TLoadContext>(
    TLoadContext& context,
    TReplicaInfo& replicaInfo)
{
    using NYT::Load;

    Load(context, replicaInfo.ClusterName);
    Load(context, replicaInfo.ReplicaPath);
    Load(context, replicaInfo.ContentType);
    Load(context, replicaInfo.Mode);
    Load(context, replicaInfo.State);
    Load(context, replicaInfo.History);
    Load(context, replicaInfo.ReplicationProgress);
    Load(context, replicaInfo.EnableReplicatedTableTracker);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NChaosClient::NProto::TReplicationCard* protoReplicationCard,
    const NChaosNode::TReplicationCard& replicationCard,
    const TReplicationCardFetchOptions& fetchOptions)
{
    protoReplicationCard->set_era(replicationCard.GetEra());
    ToProto(protoReplicationCard->mutable_table_id(), replicationCard.GetTableId());
    protoReplicationCard->set_table_path(replicationCard.GetTablePath());
    protoReplicationCard->set_table_cluster_name(replicationCard.GetTableClusterName());
    protoReplicationCard->set_current_timestamp(replicationCard.GetCurrentTimestamp());

    if (auto* collocation = replicationCard.GetCollocation()) {
        ToProto(protoReplicationCard->mutable_replication_card_collocation_id(), collocation->GetId());
    } else if (replicationCard.GetAwaitingCollocationId()) {
        ToProto(protoReplicationCard->mutable_replication_card_collocation_id(), replicationCard.GetAwaitingCollocationId());
    }

    if (fetchOptions.IncludeCoordinators) {
        protoReplicationCard->mutable_coordinator_cell_ids()->Reserve(replicationCard.Coordinators().size());
        for (const auto& [cellId, info] : replicationCard.Coordinators()) {
            if (info.State == EShortcutState::Granted) {
                ToProto(protoReplicationCard->add_coordinator_cell_ids(), cellId);
            }
        }
    }

    protoReplicationCard->mutable_replicas()->Reserve(replicationCard.Replicas().size());
    for (const auto& [replicaId, replicaInfo] : replicationCard.Replicas()) {
        auto* protoEntry = protoReplicationCard->add_replicas();
        ToProto(protoEntry->mutable_id(), replicaId);
        ToProto(protoEntry->mutable_info(), replicaInfo, fetchOptions);
    }

    if (fetchOptions.IncludeReplicatedTableOptions) {
        protoReplicationCard->set_replicated_table_options(ToProto(ConvertToYsonString(replicationCard.GetReplicatedTableOptions())));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
