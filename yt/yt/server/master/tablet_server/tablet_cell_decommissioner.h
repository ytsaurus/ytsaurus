#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletCellDecommissioner
    : public TRefCounted
{
public:
    virtual void Start() const = 0;
    virtual void Stop() const = 0;

    virtual void Reconfigure(TTabletCellDecommissionerConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletCellDecommissioner)

////////////////////////////////////////////////////////////////////////////////

ITabletCellDecommissionerPtr CreateTabletCellDecommissioner(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
