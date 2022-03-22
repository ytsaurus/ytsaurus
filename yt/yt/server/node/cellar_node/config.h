#pragma once

#include "public.h"

#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent cellar node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for cellar node heartbeats.
    TDuration HeartbeatPeriodSplay;

    REGISTER_YSON_STRUCT(TMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent cellar node heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Splay for cellar node heartbeats.
    std::optional<TDuration> HeartbeatPeriodSplay;

    //! Timeout of the cellar node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    NCellarAgent::TCellarManagerDynamicConfigPtr CellarManager;

    TMasterConnectorDynamicConfigPtr MasterConnector;

    REGISTER_YSON_STRUCT(TCellarNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeConfig
    : public NYTree::TYsonStruct
{
public:
    NCellarAgent::TCellarManagerConfigPtr CellarManager;

    TMasterConnectorConfigPtr MasterConnector;

    REGISTER_YSON_STRUCT(TCellarNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarNodeConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
