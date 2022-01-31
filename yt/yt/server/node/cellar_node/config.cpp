#include "config.h"

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default();
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TCellarNodeDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellar_manager", &TThis::CellarManager)
        .DefaultNew();
    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TCellarNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cellar_manager", &TThis::CellarManager)
        .DefaultNew();

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
