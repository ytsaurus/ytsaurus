#include "config.h"

#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

void TRouterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("dialer", &TThis::Dialer)
        .DefaultNew();

    registrar.Parameter("max_listener_backlog_size", &TThis::MaxListenerBacklogSize)
        .Default(8192);
}

////////////////////////////////////////////////////////////////////////////////

void TTcpProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("role", &TThis::Role)
        .Default("default");

    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();

    registrar.Parameter("router", &TThis::Router)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TRouterDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("routing_table_update_period", &TThis::RoutingTableUpdatePeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TTcpProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("poller_thread_count", &TThis::PollerThreadCount)
        .Default(2);
    registrar.Parameter("acceptor_thread_count", &TThis::AcceptorThreadCount)
        .Default(2);

    registrar.Parameter("router", &TThis::Router)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
