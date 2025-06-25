#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletBalancer
    : public TRefCounted
{
public:
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Reconfigure(TTabletBalancerMasterConfigPtr config) = 0;

    virtual void OnTabletHeartbeat(TTablet* tablet) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletBalancer)

////////////////////////////////////////////////////////////////////////////////

ITabletBalancerPtr CreateTabletBalancer(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
