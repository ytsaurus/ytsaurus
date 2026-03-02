#pragma once

#include "private.h"
#include "public.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct ICellTracker
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellTracker)

////////////////////////////////////////////////////////////////////////////////

ICellTrackerPtr CreateCellTracker(IBootstrap* bootstrap, TCellBalancerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
