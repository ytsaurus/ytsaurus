#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletService
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletService)

////////////////////////////////////////////////////////////////////////////////

ITabletServicePtr CreateTabletService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
