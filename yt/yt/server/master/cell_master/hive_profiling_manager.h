#pragma once

#include "public.h"

namespace NYT::NCellMaster {

struct IHiveProfilingManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;
};

DEFINE_REFCOUNTED_TYPE(IHiveProfilingManager);

IHiveProfilingManagerPtr CreateHiveProfilingManager(TBootstrap* bootstrap);

} // namespace NYT::NCellMaster
