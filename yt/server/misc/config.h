#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/misc/config.h>

#include <yt/core/misc/address.h>

#include <yt/core/rpc/config.h>

#include <yt/core/bus/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAddressResolverConfigPtr AddressResolver;
    NBus::TTcpBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    TCoreDumperConfigPtr CoreDumper;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    TServerConfig()
    {
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("bus_server", BusServer)
            .DefaultNew();
        RegisterParameter("rpc_server", RpcServer)
            .DefaultNew();
        RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
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
    }

    virtual void OnLoaded() final
    {
        if (BusServer->Port || BusServer->UnixDomainName) {
            THROW_ERROR_EXCEPTION("Explicit socket configuration for bus server is forbidden");
        }
        if (RpcPort > 0) {
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
    Stroka Path;

    //! Minimum size the disk partition must have to make this location usable.
    TNullable<i64> MinDiskSpace;

    TDiskLocationConfig()
    {
        RegisterParameter("path", Path)
            .NonEmpty();
        RegisterParameter("min_disk_space", MinDiskSpace)
            .GreaterThanOrEqual(0)
            .Default(TNullable<i64>());
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
            .InRange(0, (i64) 1024 * 1024 * 1024)
            .Default((i64) 1024 * 1024);
        RegisterParameter("timeout", Timeout)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TDiskHealthCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
