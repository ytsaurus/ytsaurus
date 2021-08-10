#pragma once

#include "public.h"
#include "chaos_cell.h"
#include "chaos_cell_bundle.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

struct IChaosManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;
    virtual const TAlienClusterRegistryPtr& GetAlienClusterRegistry() const = 0;
    virtual TChaosCell* FindChaosCell(TChaosCellId cellId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosManager)

IChaosManagerPtr CreateChaosManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
