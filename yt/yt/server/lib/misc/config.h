#pragma once

#include "public.h"

#include <yt/server/lib/core_dump/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/program/config.h>

#include <yt/core/net/address.h>

#include <yt/core/rpc/config.h>

#include <yt/core/bus/tcp/config.h>

#include <yt/core/tracing/config.h>

#include <yt/core/logging/config.h>

#include <yt/core/http/config.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public TSingletonsConfig
    , public TDiagnosticDumpConfig
{
public:
    NBus::TTcpBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;
    NCoreDump::TCoreDumperConfigPtr CoreDumper;

    int RpcPort;

    // COMPAT(babenko): get rid of this after switching to new HTTP implementation
    int MonitoringPort;
    NHttp::TServerConfigPtr MonitoringServer;

    TServerConfig()
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
        RegisterParameter("monitoring_server", MonitoringServer)
            .DefaultNew();

        RegisterPostprocessor([&] {
            if (RpcPort > 0) {
                if (BusServer->Port || BusServer->UnixDomainSocketPath) {
                    THROW_ERROR_EXCEPTION("Explicit socket configuration for bus server is forbidden");
                }
                BusServer->Port = RpcPort;
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDiskLocationConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Root directory for the location.
    TString Path;

    //! Minimum size the disk partition must have to make this location usable.
    std::optional<i64> MinDiskSpace;

    TDiskLocationConfig()
    {
        RegisterParameter("path", Path)
            .NonEmpty();
        RegisterParameter("min_disk_space", MinDiskSpace)
            .GreaterThanOrEqual(0)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TDiskLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiskHealthCheckerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent checks.
    TDuration CheckPeriod;

    //! Size of the test file.
    i64 TestSize;

    //! Maximum time allowed for execution of a single check.
    TDuration Timeout;

    TDiskHealthCheckerConfig()
    {
        RegisterParameter("check_period", CheckPeriod)
            .Default(TDuration::Minutes(1));
        RegisterParameter("test_size", TestSize)
            .InRange(0, 1_GB)
            .Default(1_MB);
        RegisterParameter("timeout", Timeout)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TDiskHealthCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TFormatConfigBase
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;
    NYTree::IMapNodePtr DefaultAttributes;

    TFormatConfigBase()
    {
        RegisterParameter("enable", Enable)
            .Default(true);
        RegisterParameter("default_attributes", DefaultAttributes)
            .Default(NYTree::GetEphemeralNodeFactory()->CreateMap());
    }
};

DEFINE_REFCOUNTED_TYPE(TFormatConfigBase)

class TFormatConfig
    : public TFormatConfigBase
{
public:
    THashMap<TString, TFormatConfigBasePtr> UserOverrides;

    TFormatConfig()
    {
        RegisterParameter("user_overrides", UserOverrides)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TFormatConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
