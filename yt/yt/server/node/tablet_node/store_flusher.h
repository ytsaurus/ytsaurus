#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStoreFlusher
    : public virtual TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoreFlusher)

IStoreFlusherPtr CreateStoreFlusher(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
