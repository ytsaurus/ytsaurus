#pragma once

#include "public.h"

#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent cellar node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for cellar node heartbeats.
    TDuration HeartbeatPeriodSplay;

    //! Timeout of the cellar node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    TMasterConnectorConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
            .Default(TDuration::Seconds(1));
        RegisterParameter("heartbeat_timeout", HeartbeatTimeout)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent cellar node heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Splay for cellar node heartbeats.
    std::optional<TDuration> HeartbeatPeriodSplay;

    TMasterConnectorDynamicConfig()
    {
        RegisterParameter("heartbeat_period", HeartbeatPeriod)
            .Default();
        RegisterParameter("heartbeat_period_splay", HeartbeatPeriodSplay)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    NCellarAgent::TCellarManagerDynamicConfigPtr CellarManager;

    TMasterConnectorDynamicConfigPtr MasterConnector;

    TCellarNodeDynamicConfig()
    {
        RegisterParameter("cellar_manager", CellarManager)
            .DefaultNew();
        RegisterParameter("master_connector", MasterConnector)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    NCellarAgent::TCellarManagerConfigPtr CellarManager;

    TMasterConnectorConfigPtr MasterConnector;

    TCellarNodeConfig()
    {
        RegisterParameter("cellar_manager", CellarManager)
            .DefaultNew();

        RegisterParameter("master_connector", MasterConnector)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TCellarNodeConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
