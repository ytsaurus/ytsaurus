#include "config.h"

#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

void TKafkaProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(80);
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("bind_retry_count", &TThis::BindRetryCount)
        .Default(5);

    registrar.Parameter("bind_retry_backoff", &TThis::BindRetryBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("max_simultaneous_connections", &TThis::MaxSimultaneousConnections)
        .Default(50'000);

    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(8192);

    registrar.Parameter("read_idle_timeout", &TThis::ReadIdleTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("write_idle_timeout", &TThis::WriteIdleTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();

    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();

    registrar.Parameter("auth", &TThis::Auth)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TKafkaProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("poller_thread_count", &TThis::PollerThreadCount)
        .Default(2);
    registrar.Parameter("acceptor_thread_count", &TThis::AcceptorThreadCount)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaroxy
