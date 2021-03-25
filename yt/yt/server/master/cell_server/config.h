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

    TCellBalancerConfig()
    {
        RegisterParameter("enable_tablet_cell_smoothing", EnableTabletCellSmoothing)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellarNodeTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxConcurrentHeartbeats;

    TDynamicCellarNodeTrackerConfig()
    {
        RegisterParameter("max_concurrent_heartbeats", MaxConcurrentHeartbeats)
            .Default(10)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellarNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellManagerConfig
    : public NHydra::THydraJanitorConfig
{
public:
    TDynamicCellarNodeTrackerConfigPtr CellarNodeTracker;

    TDynamicCellManagerConfig()
    {
        RegisterParameter("cellar_node_tracker", CellarNodeTracker)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
