#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IPartitionBalancer
    : public virtual TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitionBalancer)

IPartitionBalancerPtr CreatePartitionBalancer(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
