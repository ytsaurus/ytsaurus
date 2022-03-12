#pragma once

#include "public.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

TReplicationCardId MakeReplicationCardId(NObjectClient::TObjectId randomId);
TReplicaId MakeReplicaId(TReplicationCardId replicationCardId, TReplicaIdIndex index);
TReplicationCardId ReplicationCardIdFromReplicaId(TReplicaId replicaId);
TReplicationCardId ReplicationCardIdFromUpstreamReplicaIdOrNull(TReplicaId upstreamReplicaId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
