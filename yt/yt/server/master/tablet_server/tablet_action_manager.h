#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletActionManager
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Reconfigure(TTabletActionManagerMasterConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletActionManager)

////////////////////////////////////////////////////////////////////////////////

ITabletActionManagerPtr CreateTabletActionManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
