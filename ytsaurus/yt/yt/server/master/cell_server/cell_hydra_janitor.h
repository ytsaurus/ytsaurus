#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct ICellHydraJanitor
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellHydraJanitor)

////////////////////////////////////////////////////////////////////////////////

ICellHydraJanitorPtr CreateCellHydraJanitor(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
