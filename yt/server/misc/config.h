#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/misc/config.h>

#include <yt/core/net/address.h>

#include <yt/core/rpc/config.h>

#include <yt/core/bus/config.h>

#include <yt/core/tracing/config.h>

#include <yt/core/logging/config.h>

#include <yt/core/http/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    // Singletons.
    THashMap<TString, int> FiberStackPoolSizes;
    NNet::TAddressResolverConfigPtr AddressResolver;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NLogging::TLogConfigPtr Logging;
    NTracing::TTraceManagerConfigPtr Tracing;

    NBus::TTcpBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;
    TCoreDumperConfigPtr CoreDumper;

    //! RPC interface port number.
    int RpcPort;

    // COMPAT(babenko): get rid of this after switching to new HTTP implementation
    int MonitoringPort;
    bool UseNewHttpServer;
    NHttp::TServerConfigPtr MonitoringServer;

    TServerConfig()
    {
        RegisterParameter("fiber_stack_pool_sizes", FiberStackPoolSizes)
            .Default({});
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("rpc_dispatcher", RpcDispatcher)
            .DefaultNew();
        RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();
        RegisterParameter("logging", Logging)
            .Default(NLogging::TLogConfig::CreateDefault());
        RegisterParameter("tracing", Tracing)
            .DefaultNew();

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
        RegisterParameter("use_new_http_server", UseNewHttpServer)
            .Default(true);
    }

    virtual void OnLoaded() override
    {
        TYsonSerializable::OnLoaded();
        if (RpcPort > 0) {
            if (BusServer->Port || BusServer->UnixDomainName) {
                THROW_ERROR_EXCEPTION("Explicit socket configuration for bus server is forbidden");
            }
            BusServer->Port = RpcPort;
        }
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
    TNullable<i64> MinDiskSpace;

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

} // namespace NYT
