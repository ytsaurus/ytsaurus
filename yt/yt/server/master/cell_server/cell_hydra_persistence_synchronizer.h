#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct ICellHydraPersistenceSynchronizer
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellHydraPersistenceSynchronizer)

////////////////////////////////////////////////////////////////////////////////

ICellHydraPersistenceSynchronizerPtr CreateCellHydraPersistenceSynchronizer(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
