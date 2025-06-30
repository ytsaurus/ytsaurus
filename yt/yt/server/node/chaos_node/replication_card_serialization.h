#include "public.h"

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

} // namespace NYT
