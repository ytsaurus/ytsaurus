#include "replication_card_serialization.h"

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
using namespace NTabletClient;

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

    for (const auto& secondaryIndex : replicationCard.SecondaryIndices()) {
        ToProto(protoReplicationCard->add_secondary_indices(), secondaryIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NChaosNode::NProto::TSecondaryIndexPendingTransition* serialized, const TSecondaryIndexPendingTransitionPtr& original)
{
    serialized->set_state(ToProto(original->State));
    ToProto(serialized->mutable_index_replication_card_id(), original->IndexReplicationCardId);
    YT_OPTIONAL_SET_PROTO(serialized, new_correspondence, original->NewCorrespondence);
}

void FromProto(TSecondaryIndexPendingTransitionPtr* original, const NChaosNode::NProto::TSecondaryIndexPendingTransition& serialized)
{
    auto secondaryIndexPendingTransition = New<TSecondaryIndexPendingTransition>();

    FromProto(&secondaryIndexPendingTransition->State, serialized.state());
    FromProto(&secondaryIndexPendingTransition->IndexReplicationCardId, serialized.index_replication_card_id());
    secondaryIndexPendingTransition->NewCorrespondence = YT_OPTIONAL_FROM_PROTO(serialized, new_correspondence, ETableToIndexCorrespondence);

    *original = std::move(secondaryIndexPendingTransition);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
