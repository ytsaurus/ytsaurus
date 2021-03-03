#pragma once

#include "config.h"
#include "public.h"

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarManager
    : public TRefCounted
{
    virtual void Initialize() = 0;
    virtual ICellarPtr GetCellar(ECellarType type) = 0;
    virtual ICellarPtr FindCellar(ECellarType type) = 0;

    virtual void Reconfigure(TDynamicCellarManagerConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarManager)

ICellarManagerPtr CreateCellarManager(
    TCellarManagerConfigPtr config,
    ICellarBootstrapProxyPtr bootstrapProxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
