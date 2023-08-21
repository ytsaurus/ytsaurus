#pragma once

#include "public.h"

#include <yt/yt/server/lib/lsm/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct ILsmInterop
    : public virtual TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(ILsmInterop)

////////////////////////////////////////////////////////////////////////////////

ILsmInteropPtr CreateLsmInterop(
    IBootstrap* bootstrap,
    const IStoreCompactorPtr& storeCompactor,
    const IPartitionBalancerPtr& partitionBalancer,
    const IStoreRotatorPtr& storeRotator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
