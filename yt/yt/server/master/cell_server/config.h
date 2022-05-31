#pragma once

#include "public.h"

#include <yt/yt/server/master/tablet_server/config.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTabletCellSmoothing;

    TCellBalancerConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellarNodeTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxConcurrentHeartbeats;

    TDynamicCellarNodeTrackerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellarNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellManagerConfig
    : public NHydra::THydraJanitorConfig
{
public:
    TDynamicCellarNodeTrackerConfigPtr CellarNodeTracker;

    REGISTER_YSON_STRUCT(TDynamicCellManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
