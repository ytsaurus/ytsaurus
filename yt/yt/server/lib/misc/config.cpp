#include "config.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TServerConfig::TServerConfig()
{
    RegisterParameter("bus_server", BusServer)
        .DefaultNew();
    RegisterParameter("rpc_server", RpcServer)
        .DefaultNew();
    RegisterParameter("core_dumper", CoreDumper)
        .Default();

    RegisterParameter("rpc_port", RpcPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);
    RegisterParameter("monitoring_port", MonitoringPort)
        .Default(0)
        .GreaterThanOrEqual(0)
        .LessThan(65536);

    RegisterPostprocessor([&] {
        if (RpcPort > 0) {
            if (BusServer->Port || BusServer->UnixDomainSocketPath) {
                THROW_ERROR_EXCEPTION("Explicit socket configuration for bus server is forbidden");
            }
            BusServer->Port = RpcPort;
        }
    });
}

NHttp::TServerConfigPtr TServerConfig::CreateMonitoringHttpServerConfig()
{
    auto config = New<NHttp::TServerConfig>();
    config->Port = MonitoringPort;
    config->BindRetryCount = BusServer->BindRetryCount;
    config->BindRetryBackoff = BusServer->BindRetryBackoff;
    config->ServerName = "monitoring";
    return config;
}

////////////////////////////////////////////////////////////////////////////////

TDiskLocationConfig::TDiskLocationConfig()
{
    RegisterParameter("path", Path)
        .NonEmpty();
    RegisterParameter("min_disk_space", MinDiskSpace)
        .GreaterThanOrEqual(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TDiskHealthCheckerConfig::TDiskHealthCheckerConfig()
{
    RegisterParameter("check_period", CheckPeriod)
        .Default(TDuration::Minutes(1));
    RegisterParameter("test_size", TestSize)
        .InRange(0, 1_GB)
        .Default(1_MB);
    RegisterParameter("timeout", Timeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

TFormatConfigBase::TFormatConfigBase()
{
    RegisterParameter("enable", Enable)
        .Default(true);
    RegisterParameter("default_attributes", DefaultAttributes)
        .Default(NYTree::GetEphemeralNodeFactory()->CreateMap());
}

////////////////////////////////////////////////////////////////////////////////

TFormatConfig::TFormatConfig()
{
    RegisterParameter("user_overrides", UserOverrides)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

