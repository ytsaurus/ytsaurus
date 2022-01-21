#include "public.h"

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NChaosClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TReplicationCardId MakeReplicationCardId(TObjectId randomId)
{
    return MakeId(
        EObjectType::ReplicationCard,
        CellTagFromId(randomId),
        CounterFromId(randomId),
        HashFromId(randomId) & 0xffff0000);
}

TReplicaId MakeReplicaId(TReplicationCardId replicationCardId, TReplicaIdIndex index)
{
    return MakeId(
        EObjectType::ReplicationCardReplica,
        CellTagFromId(replicationCardId),
        CounterFromId(replicationCardId),
        HashFromId(replicationCardId) | index);
}

TReplicationCardId ReplicationCardIdFromReplicaId(TReplicaId replicaId)
{
    return MakeId(
        EObjectType::ReplicationCard,
        CellTagFromId(replicaId),
        CounterFromId(replicaId),
        HashFromId(replicaId) & 0xffff0000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
