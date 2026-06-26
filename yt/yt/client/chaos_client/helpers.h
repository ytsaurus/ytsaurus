#pragma once

#include "public.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

TReplicationCardId MakeReplicationCardId(NObjectClient::TObjectId randomId);
TChaosLeaseId MakeChaosLeaseId(NObjectClient::TObjectId randomId);
TReplicaId MakeReplicaId(TReplicationCardId replicationCardId, TReplicaIdIndex index);
TReplicationCardId ReplicationCardIdFromReplicaId(TReplicaId replicaId);
TReplicationCardId ReplicationCardIdFromUpstreamReplicaIdOrNull(TReplicaId upstreamReplicaId);
TReplicationCardId MakeReplicationCardCollocationId(NObjectClient::TObjectId randomId);

NObjectClient::TCellTag GetSiblingChaosCellTag(NObjectClient::TCellTag cellTag);

bool IsValidReplicationProgress(const TReplicationProgress& progress);
bool IsOrderedTabletReplicationProgress(const TReplicationProgress& progress);
bool IsOrderedTableReplicationProgress(const TReplicationProgress& progress, int tabletCount);
void ValidateOrderedTabletReplicationProgress(const TReplicationProgress& progress);
void ValidateOrderedTableReplicationProgress(const TReplicationProgress& progress, int tabletCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
