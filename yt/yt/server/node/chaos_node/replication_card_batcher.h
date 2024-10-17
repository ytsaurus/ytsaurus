#pragma once

#include "public.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

std::vector<TReplicationCardId> BuildReadyToMigrateReplicationCardBatch(
    const NHydra::TEntityMap<TReplicationCard>& replicationCardMap,
    int targetBatchSize);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
