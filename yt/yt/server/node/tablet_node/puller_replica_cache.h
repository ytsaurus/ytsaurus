#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! This cache stores tablet ids of the replicas, that recently pulled data from the tablet.
struct IPullerReplicaCache
    : public virtual TRefCounted
{
    virtual void OnPull(TTabletId pullerTabletId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPullerReplicaCache)

////////////////////////////////////////////////////////////////////////////////

IPullerReplicaCachePtr GetDisabledPullerReplicaCache();

IPullerReplicaCachePtr CreatePullerReplicaCache(
    TTablet* tablet,
    NChaosClient::TReplicationCardId replicationCardId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
