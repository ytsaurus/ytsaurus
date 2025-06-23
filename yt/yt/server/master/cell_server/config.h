#pragma once

#include "public.h"

#include <yt/yt/server/master/tablet_server/config.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct TCellBalancerBootstrapConfig
    : public NYTree::TYsonStruct
{
    bool EnableTabletCellSmoothing;

    REGISTER_YSON_STRUCT(TCellBalancerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCellarNodeTrackerConfig
    : public NYTree::TYsonStruct
{
    int MaxConcurrentHeartbeats;

    REGISTER_YSON_STRUCT(TDynamicCellarNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellarNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellManagerConfig
    : public NYTree::TYsonStruct
{
    // COMPAT(danilalexeev)
    bool CreateVirtualCellMapsByDefault;

    REGISTER_YSON_STRUCT(TCellManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCellManagerConfig
    : public NYTree::TYsonStruct
{
    TDynamicCellarNodeTrackerConfigPtr CellarNodeTracker;

    REGISTER_YSON_STRUCT(TDynamicCellManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
