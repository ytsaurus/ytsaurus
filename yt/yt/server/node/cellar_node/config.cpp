#include "config.h"

#include <yt/yt/server/node/cluster_node/config.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("heartbeat_period", &TThis::HeartbeatPeriod)
        .Default();
    registrar.Parameter("heartbeat_period_splay", &TThis::HeartbeatPeriodSplay)
        .Default();
    registrar.Parameter("heartbeat_timeout", &TThis::HeartbeatTimeout)
        .Default(TDuration::Seconds(60));
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

void TCpuLimits::Register(TRegistrar registrar)
{
    registrar.Parameter("write_thread_pool_size", &TThis::WriteThreadPoolSize)
        .GreaterThan(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TBundleDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_limits", &TThis::CpuLimits)
        .DefaultNew();

    registrar.Parameter("memory_limits", &TThis::MemoryLimits)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
