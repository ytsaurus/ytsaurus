#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStoreTrimmer
    : public virtual TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoreTrimmer)

IStoreTrimmerPtr CreateStoreTrimmer(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
