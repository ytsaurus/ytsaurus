#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

struct IAlienCellSynchronizer
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;
    virtual void Reconfigure(TAlienCellSynchronizerConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAlienCellSynchronizer)

IAlienCellSynchronizerPtr CreateAlienCellSynchronizer(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
