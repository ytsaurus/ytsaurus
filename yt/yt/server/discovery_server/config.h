#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/discovery_server/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryServerBootstrapConfig
    : public NServer::TServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    int WorkerThreadPoolSize;

    NBus::TBusConfigPtr BusClient;

    NDiscoveryServer::TDiscoveryServerConfigPtr DiscoveryServer;

    REGISTER_YSON_STRUCT(TDiscoveryServerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryServerProgramConfig
    : public TDiscoveryServerBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TDiscoveryServerProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
