#include "config.h"

namespace NYT::NZookeeperProxy {

////////////////////////////////////////////////////////////////////////////////

void TZookeeperServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(2181);

    registrar.Parameter("max_simultaneous_connections", &TThis::MaxSimultaneousConnections)
        .Default(50'000);

    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(8192);

    registrar.Parameter("bind_retry_count", &TThis::BindRetryCount)
        .Default(5);
    registrar.Parameter("bind_retry_backoff", &TThis::BindRetryBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("read_idle_timeout", &TThis::ReadIdleTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("write_idle_timeout", &TThis::ReadIdleTimeout)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

void TZookeeperProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("server", &TThis::Server)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
