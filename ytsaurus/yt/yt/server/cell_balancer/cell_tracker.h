#pragma once

#include "private.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class ICellTracker
    : public TRefCounted
{
public:
    virtual void Start() = 0;

    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellTracker)

////////////////////////////////////////////////////////////////////////////////

ICellTrackerPtr CreateCellTracker(IBootstrap* bootstrap, TCellBalancerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
