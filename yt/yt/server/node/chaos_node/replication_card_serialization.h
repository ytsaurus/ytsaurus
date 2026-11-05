#include "public.h"

#include <yt/yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt_proto/yt/client/chaos_client/proto/replication_card.pb.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
void Save(NChaosNode::TSaveContext& context, const NChaosClient::TReplicaInfo& replicaInfo);

template <>
void Load<NChaosClient::TReplicaInfo, NChaosNode::TLoadContext>(
    NChaosNode::TLoadContext& context,
    NChaosClient::TReplicaInfo& replicaInfo);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NChaosClient::NProto::TReplicationCard* protoReplicationCard,
    const NChaosNode::TReplicationCard& replicationCard,
    const NChaosClient::TReplicationCardFetchOptions& fetchOptions);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NChaosNode::NProto::TSecondaryIndexPendingTransition* serialized,
    const NChaosNode::TSecondaryIndexPendingTransitionPtr& original);
void FromProto(
    NChaosNode::TSecondaryIndexPendingTransitionPtr* original,
    const NChaosNode::NProto::TSecondaryIndexPendingTransition& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
