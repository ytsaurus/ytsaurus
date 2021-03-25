#pragma once

#include "config.h"
#include "public.h"

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICellarManager
    : public TRefCounted
{
    virtual void Initialize() = 0;
    virtual ICellarPtr GetCellar(NCellarClient::ECellarType type) = 0;
    virtual ICellarPtr FindCellar(NCellarClient::ECellarType type) = 0;

    virtual void Reconfigure(TCellarManagerDynamicConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellarManager)

ICellarManagerPtr CreateCellarManager(
    TCellarManagerConfigPtr config,
    ICellarBootstrapProxyPtr bootstrapProxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
