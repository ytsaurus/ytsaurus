#include "public.h"
#include "replication_card_serialization.h"

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NChaosClient {

using namespace NObjectClient;
using namespace NTableClient;

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
        EObjectType::ChaosTableReplica,
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

TReplicationCardId ReplicationCardIdFromUpstreamReplicaIdOrNull(TReplicaId upstreamReplicaId)
{
    return TypeFromId(upstreamReplicaId) == EObjectType::ChaosTableReplica
        ? ReplicationCardIdFromReplicaId(upstreamReplicaId)
        : TReplicationCardId();
}

void ValidateOrderedTabletReplicationProgress(const TReplicationProgress& progress)
{
    const auto& segments = progress.Segments;
    const auto& upper = progress.UpperKey;
    if (segments.size() != 1 ||
        segments[0].LowerKey.GetCount() != 1 ||
        segments[0].LowerKey[0].Type != EValueType::Int64 || 
        upper.GetCount() != 1 ||
        upper[0].Type != EValueType::Int64)
    {
        THROW_ERROR_EXCEPTION("Invalid replication progress for ordered table")
            << TErrorAttribute("replication_progress", progress);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
