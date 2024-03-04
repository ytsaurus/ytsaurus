#pragma once

#include "public.h"

#include <yt/yt/server/master/tablet_server/config.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableTabletCellSmoothing;

    REGISTER_YSON_STRUCT(TCellBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellarNodeTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxConcurrentHeartbeats;

    REGISTER_YSON_STRUCT(TDynamicCellarNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellarNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellManagerConfig
    : public NYTree::TYsonStruct
{
public:
    // COMPAT(danilalexeev)
    bool CreateVirtualCellMapsByDefault;

    REGISTER_YSON_STRUCT(TCellManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDynamicCellarNodeTrackerConfigPtr CellarNodeTracker;

    REGISTER_YSON_STRUCT(TDynamicCellManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
