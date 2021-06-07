#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/lsm/lsm_backend.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IPartitionBalancer
    : public virtual TRefCounted
{
    virtual void ProcessLsmActionBatch(
        const ITabletSlotPtr& slot,
        const NLsm::TLsmActionBatch& batch) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitionBalancer)

IPartitionBalancerPtr CreatePartitionBalancer(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
