#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
void Save(NChaosNode::TSaveContext& context, const NChaosClient::TReplicaInfo& replicaInfo);

template <>
void Load<NChaosClient::TReplicaInfo, NChaosNode::TLoadContext>(
    NChaosNode::TLoadContext& context,
    NChaosClient::TReplicaInfo& replicaInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
